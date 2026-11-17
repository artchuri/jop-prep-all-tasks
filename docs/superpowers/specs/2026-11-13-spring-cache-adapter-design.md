# Spring Cache поверх Lua (LuaCache implements Cache) + Jedis

## Зачем (intent)
Переписать cache-aside чтения аккаунтов с самописного вызова скриптов в сервисе на
стандартную абстракцию Spring Cache: `@Cacheable`/`@CacheEvict` на методах,
исполнение — через наш `LuaCache implements Cache`, клиент — Jedis вместо дефолтного Lettuce.
Цель: идиоматичность под ревью (видно аннотациями, что кешируется/инвалидируется)
без потери семантики версии/lease/stale.

## Что сохраняем, что меняем

| Компонент | Сейчас | Станет |
|---|---|---|
| Клиент Redis | Lettuce (дефолт стартера) | Jedis (зависимость + `spring.data.redis.client-type: jedis`) |
| Чтение | `AccountServiceImpl.get()` вручную вызывает `accountCacheService.read()` | `@Cacheable(cacheNames="accounts", key="#accountId", sync=true)` |
| Наполнение | `loadAndFill()` → `fill()` | `Cache.put()` внутри `get(key, valueLoader)` |
| Инвалидация | listener → `invalidate()` | `@CacheEvict`-семантика через `cache.evict()` в AFTER_COMMIT-слушателе |
| Lua-скрипты | 4 (read/fill/invalidate/release) | те же 4, вызываются из `LuaCache` |
| Version-gating | invalidate.lua | без изменений |
| Lease/stale | read.lua | без изменений |
| Metrics | AccountCacheScripts | переезжают в LuaCache |
| Fallback PG при падении Redis | try/catch в сервисе | `CachingConfigurer#errorHandler` → ошибки get = идти в БД |

## Подходы
1. **Полный дефолт Spring Cache (отклонён):** теряем version-gating и lease; stale до 60 c на гонке evict/fill, stampede на промахе.
2. **Свой аспект + своя аннотация (отклонён):** изобретаем мини-Spring Cache.
3. **✅ LuaCache implements Cache (выбран):** стандартные аннотации + 2 класса адаптера, семантика скриптов сохранена.

## Ключевые решения (мои предложения — проверить)
- Ключ кэша: `acc:v1:<accountId>`, строится в `LuaCache` из числового ключа.
- `sync=true` на `@Cacheable`: Spring вызывает `get(key, Callable)` — внутри lease-логика
  (LEASE → loader → fill; WAIT → короткий цикл read; STALE → отдать stale как HIT).
  JVM-синхронизация Spring поверх распределённой lease — не конфликтует, лишь уменьшает конкуренцию.
- STALE возвращаем как обычное значение (признак — метрика, не API; в DTO его и сейчас нет).
- Инвалидация: слушатель делает `cache.evict(accountId)`; version-gate + stale-окно — внутри evict().
- Existing IT (`AccountCacheIntegrationIT`, `CacheRollbackSafetyIT`) не меняем по поведению — проверить моки.
- `AccountCacheScripts` после переезда либо удаляется, либо остаётся фасадом для тестов — решить по факту.
- Jedis: `redis.clients:jedis` (версию даст Boot BOM) + `spring.data.redis.client-type: jedis`.
  `DefaultRedisScript` от клиента не зависит.
- Путь GET: **решено — оставляем `/v1/account`** (ед. число), README поправить под фактический путь.

## Ограничения
- Spring 7.0.9 / Boot 4.1.1 — работаем с синхронным Cache API (`retrieve()` не трогаем).
- Поведение (staleness bounds, имена метрик, HTTP-контракты) не меняется — только механика вызовов.
- Java 21.

## План имплементации
1. pom.xml + application.yaml: Jedis.
2. `LuaCache`, `LuaCacheManager`, `@EnableCaching` + `CachingConfigurer` (errorHandler: ошибки Redis → null → PG).
3. `@Cacheable` на read-path (self-invocation: метод должен зваться извне бина — при необходимости выделить read-facade).
4. `@CacheEvict`-семантика в AFTER_COMMIT-слушателе.
5. Метрики `cache_requests_total{result}` и таймеры — в LuaCache.
6. Прогнать unit + IT; README: убрать механику из Consistency model, bounds оставить.
7. Почистить мёртвые константы GlobalValues по факту использования.
## Статус: РЕАЛИЗОВАНО (2026-11-13)

Итоговая структура:
- `utils/cache/LuaCache implements Cache` — адаптер: read/fill/invalidate Lua-скрипты внутри стандартного Cache API; значение кэша — `AccountResponseDto` (собирается из Redis-хэша через `AccountSnapshot`), поэтому loader и HIT возвращают один тип.
- `utils/cache/LuaCacheManager implements CacheManager` — единственный CacheManager-бин; Boot отступает через `@ConditionalOnMissingBean(CacheManager.class)`.
- `config/CacheConfig` — `@EnableCaching` + `CachingConfigurer`.
- `config/SuppressingCacheErrorHandler` — ошибки Redis в аннотационном слое логируются и глотаются (read → PG fallback).
- ~~`service/AccountCacheReadService`~~ — фасад удалён: единственный caller `get()` — контроллер (внешний вызов, прокси активен), поэтому `@Cacheable` перенесён прямо на `AccountServiceImpl.get()` с комментарием о self-invocation. Если появится внутренний caller — вынести снова.
- Инвалидация (Tier 1 AFTER_COMMIT-слушатель и Tier 2 Kafka-консьюмер): `Cache.evict(new LuaCache.Invalidate(accountId, version))` — version-gated через invalidate.lua.
- Клиент: Jedis (`pom.xml` + `spring.data.redis.client-type: jedis`).

Зафиксированные решения по ходу имплементации:
- `sync=true`: Spring JVM-синхронизация поверх распределённой lease — оставлено.
- STALE возвращается как обычное значение (признак — только метрика).
- `put()` — no-op: fill возможен только под валидным lease-токеном.
- На пути загрузки под lease при ошибке loader'а делается `release(token)` (раньше lease истекал сам).

Попутно починен pre-existing сломанный рефакторинг (не связан с кэшем):
- `AccountRowMapper`: jOOQ отдаёт `OffsetDateTime` для timestamptz — приведение к `Instant`.
- `TransferRowMapper`: для timestamp — `java.sql.Timestamp` → `Instant`.
- Тесты outbox: `OutboxPublisher.TOPIC` → `GlobalValues.KAFKA_TOPIC`.

Верификация: unit 15/15, cache IT 7/7 (Testcontainers PG+Redis+Kafka), компиляция JDK 21.
## Дополнение (2026-11-14): инвалидация через стандартный @CacheEvict

Промежуточный вариант с собственной аннотацией @VersionedCacheEvict и @Aspect был реализован, затем удалён:
два разных механизма (Spring Cache для чтения, собственный аспект для инвалидации) усложняют чтение кода.

Итоговое решение — одна идиома Spring Cache:
- Tier 1: `@CacheEvict(cacheNames = "accounts", key = "new ...LuaCache.Invalidate(#event.accountId(), #event.newVersion())")` на AFTER_COMMIT-слушателе;
- `LuaCache.evict(Invalidate)` выполняет version-gated инвалидацию (invalidate.lua: применяет только если версия новее);
- ошибки Redis глотаются `SuppressingCacheErrorHandler` (CachingConfigurer);
- Tier 2 (Kafka-консьюмер) остаётся программным: 1–3 динамических evict на сообщение с ветвлением по типу события не выразить аннотацией.

Покрытие: `freezeInvalidatesThroughTheAnnotatedListenerPath` в AccountCacheIntegrationIT проходит полный путь
freeze() -> коммит -> AFTER_COMMIT -> @CacheEvict -> Redis invalidate -> следующий GET читает из PG.

Попутно: AccountRowMapper принимает и OffsetDateTime (SELECT), и Timestamp (RETURNING).
## Дополнение (2026-11-14): наблюдаемость и пул соединений

После перехода на @CacheEvict метрики Tier 1 исчезли вместе с удалённым аспектом — восстановлены в других точках:
- `cache_evict_total{result=success|error}` — в LuaCache.evict (единая точка любой инвалидации);
- `cache_errors_total{operation=get|put|evict|clear}` — в SuppressingCacheErrorHandler;
- `cache_requests_total{result=hit|stale|miss_lease|wait}`, `cache_redis_latency_seconds` — как раньше, в AccountCacheScripts.
Дашборд cache-dashboard.json обновлён: панель «Tier 1» теперь читает cache_evict_total, панель ошибок — cache_errors_total.

Jedis: включён пул соединений (commons-pool2 + spring.data.redis.jedis.pool: max-active 64, max-idle 16, min-idle 4, max-wait 100ms). Без commons-pool2 Boot не создаёт пул, и каждый вызов открывал бы новое соединение.
## Дополнение (2026-11-14): eviction внутри сервиса через afterCommit

R&D по «эвиктить прямо в сервисе без рисков». Рассмотрено три подхода:
1. `@CacheEvict` прямо на `freeze()`/`transfer()` — отклонено: evict происходит внутри транзакции, при откате кэш уже очищен; плюс ключ требует версии, которой нет в аргументах.
2. `TransactionAwareCacheManager` (spring-context-support) + `@CacheEvict` — безопасно по времени, но ключ с версией всё равно невыразим.
3. **Выбран:** `CacheEvictAfterCommit` — `TransactionSynchronization.afterCommit()` в точке записи.

Реализация:
- `utils/cache/CacheEvictAfterCommit.evict(accountId, version)` — если транзакция активна, регистрирует afterCommit-колбэк; иначе эвиктит сразу. Ошибки Redis глотаются (запись не ломается).
- `AccountServiceImpl.freeze()` → `cacheEvict.evict(accountId, updated.getVersion())`.
- `TransferServiceImpl.publishTransferCompleted()` → два вызова (from/to с их версиями).
- Удалены `CacheInvalidationAfterCommitListener` и событие `AccountCacheInvalidationRequested` — они были лишь транспортом для той же логики.

Гарантии сохранены и покрыты тестами (`CacheEvictAfterCommitTest`): без транзакции — немедленно; в транзакции — только после коммита; откат — никогда; ошибка Redis — не пробрасывается.
## Дополнение (2026-11-14): TTL и параметры кэша переехали в compose

Параметры кэша больше не захардкожены в коде и не живут только в yaml:
- `config/AccountCacheProperties` — `@ConfigurationProperties(prefix = "account-cache")`: `ttlMs`, `ttlJitterMs` (двусторонний разброс: фактический TTL = `ttlMs ± ttlJitterMs`), `leaseMs`, `marginMs`, `staleMs` (с `@DefaultValue` для локальных прогонов и тестов);
- `AccountCacheScripts` читает их вместо констант `GlobalValues.CACHE_*`, которые удалены;
- `deploy/compose.yaml` задаёт `ACCOUNT_CACHE_*` явно **для каждого инстанса** (app-a и app-b), потому что допустимая граница устаревания — решение деплоя, а не константа в коде.

Смена TTL = правка compose + рестарт, без пересборки. Оба инстанса обязаны иметь одинаковые значения, иначе у них разные границы устаревания при одном Redis.
## Дополнение (2026-11-14): circuit breaker доставки outbox

Kafka-недоступность перестала сжигать попытки доставки:
- `resilience4j-spring-boot4:2.4.0` (совместим с Boot 4.1.1/Spring 7);
- `OutboxPublisher.poll()`: при `OPEN` breaker'е claim не выполняется — строки остаются NEW с нетронутым attempt_count;
- каждый send вручную оборачивается в `breaker.onError/onSuccess` (гранулярность — отправка, не батч; аннотация `@CircuitBreaker` на `poll()` дала бы неверную семантику и self-invocation);
- пороги — в `application.yaml` (`resilience4j.circuitbreaker.instances.outbox-kafka.*`): open после 5 фейлов подряд, 30 с до half-open, 1 пробная отправка;
- метрики: `outbox_breaker_transitions_total{from,to}`, `outbox_poll_skipped_breaker_open`.

Слои восстановления: breaker (короткий сбой) → retry backoff (средний) → reclaimer (REJECTED → NEW через 5 мин) → TTL (последняя линия, 51–69 с).
## Дополнение (2026-11-14): латентность Redis с исходом (blocker: cache metrics)

Latency-таймеры `cache_redis_latency_seconds` получили тег `outcome=success|error`:
- ранее при падении Redis таймер в finally тикал в общий ряд — p99 смешивал успехи и ошибки, и деградация была невидима;
- теперь панель дашборда разбивает перцентили по `outcome`, и ошибка Redis видна отдельно от нормы.
## Дополнение (2026-11-14): полный code review ACC-106 — найденные и закрытые находки

**N1 (критично): consumer мог уйти в вечный ретрай на битом payload.** NPE/NumberFormatException при извлечении полей не ловился (только JacksonException) → одна битая строка блокировала партицию навсегда. Теперь извлечение целей отделено от Redis-инвалидации: битый payload → `malformed_payload` метрика + ack (ретрай бессмысленен), ошибка Redis → проброс в error handler (ретрай с backoff).

**N2: freeze идемпотентная ветка** — добавлен комментарий, объясняющий отсутствие evict (версия не меняется → кэш уже согласован; та же логика, что в transfer replay).

**N3: отсутствовал явный тест AC8** — добавлен `staleCacheValueDoesNotAffectFinancialOperations`: прогретый кэш скрывает реальный баланс (50 вместо 100) и FROZEN-статус, но перевод отклоняется из PG (InsufficientFunds и AccountStatusConflict). Доказывает, что финансовые операции никогда не доверяют кэшу.

**N4: WAIT-константы перенесены** из `GlobalValues` в `AccountCacheProperties` (waitAttempts/waitPauseMs) — yaml + compose, как остальные кэш-параметры.

**Попутно: исправлен порядок сидинга в `seedAccount`** — пользователь вставляется до аккаунта (FK-совместимо), удаление осиротевших пользователей убрано (делало сидинг хрупким при пустой таблице аккаунтов).
