# Тестирование

## Два уровня

| Уровень | Запуск | Инфраструктура |
|---|---|---|
| Unit (`*Test.java`) | `./mvnw test` | Mockito/подставы, без контейнеров — 61 тест |
| Integration (`*IT.java`) | `./mvnw failsafe:integration-test failsafe:verify` | Testcontainers (PostgreSQL, Redis, Kafka), реальный Spring-контекст |

Соло-прогон одного IT:

```bash
./mvnw -q compile test-compile failsafe:integration-test failsafe:verify \
  -Dit.test='AccountCacheIntegrationIT' -DfailIfNoTests=false
```

Инфраструктура поднимается статически в `AbstractIntegrationTest` / `AbstractCacheIntegrationTest` (shared containers на класс). В инициализаторе контекста исключены авто-конфигурации, требующие отсутствующей инфры (JobRunr/Mongo, S3, Eureka).

## Что доказывает каждый IT

| Тест | Доказательство |
|---|---|
| `cache/AccountCacheIntegrationIT` | Cache-aside по шагам: miss → fill, повторный GET — hit без PG; version-gate (событие со старой версией не перезаписывает свежее); WAIT-lease при гонке читателей; **AC8** `staleCacheValueDoesNotAffectFinancialOperations` — прогретый кэш скрывает реальный баланс/FROZEN, но финансовые операции отклоняются из PG (кэш не источник решений) |
| `cache/CacheRollbackSafetyIT` | Rollback транзакции не инвалидирует кэш и не публикует событие (после commit, не до) |
| `cache/RedisInfraSmokeIT` | Redis достижим, EVAL Lua работает (инфраструктурный smoke) |
| `outbox/core/OutboxPublisherIT` | End-to-end: событие в PG → Kafka → потребитель видит сообщение |
| `outbox/core/OutboxPublisherRetryCharacterizationIT` | Характеризация ретраев: 5 попыток → REJECTED; backoff-окна. При изменении политики — править осознанно |
| `outbox/core/OutboxReclaimerIT` | REJECTED старше quiet-периода → NEW, попытки сброшены; свежие REJECTED не трогаются; прочие статусы не трогаются |
| `outbox/core/OutboxCleanupIT` | Ретенция: старая пустая партиция дропается, реестр согласован; партиция с NEW/REJECTED выживает всегда |
| `migration/OutboxMigrationIT` | Миграция default-партиции: строки переезжают, default возвращается |
| `test/transfer/TransferServiceImplConcurrencyIT` | Конкурентные переводы: суммарный баланс сохраняется (row locks) |
| `JobPrepApplicationIT` | Приложение стартует целиком в интеграционном окружении |
| `ratelimit/RateLimitSaturationIT` | I1–I3 на ужатых лимитах (get-account 3/min): ниже лимита все 2XX; сверх — 429 + `Retry-After` + счётчик `rejected`; насыщенный бакет не трогает соседние эндпоинты (I3) |
| `ratelimit/RateLimitOutageProxyIT` | I7 через toxiproxy: Redis отрезан → 503 `RATE_LIMITER_UNAVAILABLE` + `Retry-After` + счётчик outage; восстановление связи → снова 2XX; счётчики outage зарегистрированы на старте (zero вместо no data) |
| `ratelimit/RateLimitTwoInstancesIT` | I5: лимит глобальный — два полных инстанса (два Spring-контекста, порты 18081/18082) выедают одно окно в Redis; после исчерпания на A, B отвечает 429 |
| `ratelimit/LuaReserveRefundLogicIT` | Ядро алгоритма на живом Redis: reserve/refund по дизайн-доку Q3/Q6/Q12, время подаётся аргументом (без sleep) |

Остальная матрица НФТ — **I4** (изменение лимита через рестарт, не hot-reload) и **I6** (allow-стратегия при outage) — по согласованию вынесена в ACC-107g; в этом мерже их поведение зафиксировано конфигом и кодом, автотестов нет.

## Юнит-тесты, которые держат контракты

- `CacheEvictAfterCommitTest` — evict строго после commit, при rollback — нет; ошибка Redis не роняет операцию;
- `OutboxPublisherTest` — claim/mark/failure-переходы publisher'а;
- `OutboxServiceTest`, `AccountServiceImplTest`, `TransferServiceImplTest`, `AccountControllerTest` — доменные правила и API-контракты;
- `ratelimit/PeakProfileEvaluatorMidWindowTest` — I8: множитель пика действует внутри окна, max при пересечении профилей, ровный откат на границе fire+duration (полуинтервал);
- `ratelimit/RateLimitFilterPathTest` — матчинг пути: segment-exact префикс (`/v1` не глотает `/v10`), учёт context-path.

## Известные особенности запуска

- IT-прогон требует Docker; сборка образов дольше дефолтных таймаутов — запускайте build фоном.
- Статические Testcontainers-контейнеры шарятся между классами: после упавшего прогона в БД остаётся dirty state; `seedAccount` сеет пользователя до аккаунта (FK-порядок).
- Characterization-тесты фиксируют текущее поведение ретраев: правишь backoff/лимиты — обновляй их же заголовок как контракт.
