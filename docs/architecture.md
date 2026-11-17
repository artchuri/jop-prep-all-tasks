# Архитектура

Приложение: распределённый кэш аккаунтов (Redis) поверх PostgreSQL, с доставкой инвалидаций через transactional outbox + Kafka. Java 21, Spring Boot 4.1.x, jOOQ, Liquibase.

## Схема потоков

```
                      ┌──────────────┐
  GET /v1/account/{id}│   app-a/b    │ cache-aside: Redis (Tier 0)
─────────────────────▶│  (2 инстанса)│ miss/lease → PG → fill.lua
                      └──┬───────┬───┘
              freeze/    │       │ Tier 1: AFTER_COMMIT evict
              transfer   │       │ (best-effort, 100 ms timeout)
                         │       ▼
                         │    Redis (единый для обоих инстансов)
                         │
                         ▼
              PostgreSQL (source of truth, row locks)
                         │
              та же TX:  │ insert into outbox (Tier 2 (источник))
                         ▼
              OutboxPublisher ──▶ Kafka account-events
                         │            │
                         │            ▼
                         │   AccountCacheInvalidationConsumer
                         │            │ invalidate.lua (version-gated)
                         ▼            ▼
              Circuit breaker  Redis invalidation
              (outbox-kafka)
```

## Пакеты (src/main/java/nip/edu/jobprep)

| Пакет | Ответственность |
|---|---|
| `config` | Конфигурация кэша (`AccountCacheProperties`, `CacheConfig`, `KafkaConfig` — micrometer-биндинг продюсера Kafka) |
| `controller` | HTTP-поверхность: `AccountController`, `TransferController` (см. [api.md](api.md)) |
| `dto` | 5 record-DTO: запросы/ответы аккаунта и перевода |
| `entity` | jOOQ-сущности: `Account`, `TransferHistory`, user, outbox, история статусов |
| `exceptions` | Домашние исключения + `GlobalExceptionHandler` + `SuppressingCacheErrorHandler` (ошибки Redis не роняют операции) |
| `model` | `GlobalValues` — константы (пути Lua-скриптов, лимиты попыток, топик, имена партиций) |
| `service` | `AccountServiceImpl` (GET через кэш, freeze), `TransferServiceImpl` (переводы, row locks) |
| `repositories` | jOOQ-репозитории: аккаунты, история переводов, outbox-события |
| `outbox` | Transactional outbox (см. ниже) |
| `utils.cache` | Redis-слой: `LuaCache`/`LuaCacheManager`, `AccountCacheScripts` (Lua: fill/read/invalidate/release), `CacheEvictAfterCommit` |
| `utils.mapper` | Entity↔DTO мапперы |

## Transactional outbox (`outbox.*`)

- `event/`: `DomainEvent` + payload-записи (`AccountFrozenPayload`, `TransferCompletedPayload`).
- Событие пишется **в той же транзакции**, что и изменение данных — атомарность гарантирована.
- `core/`:
  - `OutboxService` — сохранение событий;
  - `OutboxPublisher` — планировщик (500 мс): claim батча (FOR UPDATE SKIP LOCKED, lease) → проверка порядка по userId → send в Kafka → markAsCompleted. Circuit breaker (Resilience4j, инстанс `outbox-kafka`): при OPEN claim пропускается, `attempt_count` не сгорает;
  - `OutboxRetryPolicy` — экспоненциальный backoff (2^attempt с, потолок 300 с), после 5 попыток — REJECTED;
  - `OutboxReclaimer` — раз в минуту возвращает REJECTED-строки старше 5 минут в NEW (re-drive после длинного простоя Kafka);
  - `OutboxPartitionRepository/Service` — партиционирование по месяцам, миграция default-партиции, advisory lock на DDL;
  - `OutboxCleanup` — ночная ретенция: дроп партиций старше cutoff без неразрешённых строк;
  - `OutboxMetrics` — счётчики/таймеры publisher'а.
- `delivery/`: `AccountCacheInvalidationConsumer` — ручной ack; битый payload ack'ается сразу (ретрай бессмысленен), ошибка Redis — ретрай с backoff без коммита offset.

## Модель согласованности

Финансовые операции (transfer, freeze) **никогда не читают кэш**: только PG под row lock. Кэш обслуживает только GET /v1/account/{id}. Границы устаревания:

| Слой | Граница |
|---|---|
| Tier 1 (inline evict после commit) | до 3 с lease в норме |
| Tier 2 (outbox → Kafka) | секунды при потере Tier 1 |
| TTL (последняя линия) | 51–69 с (60 с ± 9 с джиттер) |

Запись в кэш защищена version-gate: устаревшее значение не может перезаписать свежее (fill.lua сравнивает версии), повторная инвалидация идемпотентна.

Подробнее по решениям: [specs/2026-11-13-spring-cache-adapter-design.md](superpowers/specs/2026-11-13-spring-cache-adapter-design.md).