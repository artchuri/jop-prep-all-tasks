# ACC-10x: Distributed account cache (Redis)

Cache-aside reading of accounts on Redis; PostgreSQL stays the source of truth.

## Документация

| Документ | Содержание |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Архитектура: пакеты, потоки данных, transactional outbox, модель согласованности |
| [docs/api.md](docs/api.md) | HTTP API: эндпоинты, контракты, коды ошибок, известные ограничения |
| [docs/configuration.md](docs/configuration.md) | Вся конфигурация: compose-сервисы, `account-cache.*`, breaker, Kafka, health |
| [docs/observability.md](docs/observability.md) | Метрики, дашборды Grafana, health, нагрузочные скрипты |
| [docs/testing.md](docs/testing.md) | Тесты: два уровня, что доказывает каждый IT, особенности запуска |
| [docs/kafka-config.md](docs/kafka-config.md) | Консолидация Kafka-настроек, circuit breaker доставки |
| [docs/superpowers/specs/2026-11-13-spring-cache-adapter-design.md](docs/superpowers/specs/2026-11-13-spring-cache-adapter-design.md) | Дизайн-решения кэш-адаптера (журнал изменений) |

## Быстрый старт

```bash
cd deploy && docker compose up -d   # pg, redis, kafka, оба инстанса, наблюдаемость
# app-a: http://localhost:8081, app-b: http://localhost:8082, Grafana: http://localhost:3000
```

## Consistency model
- Financial operations (transfer, freeze) never read the cache: they read PG under row locks (SELECT FOR UPDATE) and keep their existing atomicity/idempotency guarantees.
- GET /v1/account/{id} may serve stale data, bounded by:
  - up to lease TTL (0.8 s) after commit in normal operation (Tier 1 invalidation);
  - seconds when Tier 1 is lost but Tier 2 (outbox -> Kafka -> consumer) delivers;
  - up to the record TTL (60 s +-15% jitter) when the invalidation is fully lost.
- The cache entry stores id, balance (amount), status, version; GET also exposes version, Cache-Control: no-store.

## Invalidation
- Tier 1: AFTER_COMMIT listener, best-effort, Redis command timeout 100 ms; errors counted, never propagated to the committed operation.
- Tier 2: transactional outbox (TRANSFER_COMPLETED with fromAccountVersion/toAccountVersion, ACCOUNT_FROZEN with version) -> Kafka topic account-events -> consumer group account-cache-invalidation; offset committed only after successful invalidate; Redis failures block and retry with 1 s backoff, never DLT.
- TTL 60 s +-15% (51..69 s) is the last-resort recovery layer.
- Tombstone semantics: each entry carries `tver` (tombstone version) alongside the displayed `ver`. Invalidation advances `tver`; STALE responses therefore serve a COHERENT snapshot (old bal/st with their own ver), never a mixed one. Invalidating a MISSING key creates a short-lived tombstone (`lease+margin`), so a lease taken before the commit cannot fill pre-invalidation data. A leader mid-load whose snapshot predates the tombstone serves its own PG result but never hands it to waiters - they re-resolve (fresh load or coherent STALE).

## Known limitations (agreed decision)
- After the outbox publisher exhausts MAX_ATTEMPTS=5 for a row (sustained Kafka outage beyond ~10 min of delivery timeouts), the row is REJECTED and that invalidation is never delivered; recovery is by TTL (<= 69 s staleness) only. This is accepted for ACC-10x and revisited if product requires faster recovery.
- Head-of-line: hasOlderActiveForUser gates delivery per user_id; a stalled older row delays newer invalidations of the same user (bounded by lease/бackoff, still covered by TTL).
- Concurrent cache-miss stampede is bounded by the lease (one filler per key) and a WAIT poll that spans the remaining lease window in 100 ms slices: cross-instance waiters never reach PostgreSQL while a live lease exists; after expiry they collapse into one PG query per instance (single-flight), whose result is cached under a fresh lease.
## Implementation notes
- Redis client: Jedis (`spring.data.redis.client-type=jedis`).
- The read path is annotation-driven: `@Cacheable(cacheNames="accounts", key="#accountId", sync=true)` on `AccountCacheReadService`, executed by a custom `Cache` (`LuaCache`) over the lease/stale/version Lua scripts. Redis read errors are logged and suppressed, falling back to PostgreSQL.
- Invalidation (Tier 1 AFTER_COMMIT listener and Tier 2 Kafka consumer) calls `Cache.evict(accountId, version)` — version-gated by the invalidate script, so stale events never regress fresh data.
## Running two instances (blocker: shared Redis for instances)

```bash
docker compose -f deploy/compose.yaml up -d postgres kafka redis app-a app-b
bash deploy/cache-cross-instance.sh      # verifies cross-instance invalidation
```

- `app-a` → `localhost:8081`, `app-b` → `localhost:8082`; both point at the same
  `redis:6379`, `postgres:5432` and `kafka:29092`.
- The script proves the shared-cache contract: A fills the entry, B is served from it;
  a freeze committed on A must stop B from serving the stale snapshot; the entry TTL
  stays within the documented 51–69 s bound.
- TTL is the last-resort layer: even if both invalidation tiers are lost, an entry
  cannot be served stale for longer than 69 s.

## Cache tuning lives in the deployment descriptor

`account-cache.*` values are set explicitly per instance in `deploy/compose.yaml`
(`ACCOUNT_CACHE_*` environment variables), because the acceptable staleness bound is a
deployment decision, not a code constant:

| Env var | Value | Meaning |
|---|---|---|
| `ACCOUNT_CACHE_TTL_MS` | 60000 | base entry lifetime (last-resort staleness bound, before jitter) |
| `ACCOUNT_CACHE_TTL_JITTER_MS` | 9000 | two-sided spread (+-9 s): actual TTL is 51..69 s, avoids lockstep expiry |
| `ACCOUNT_CACHE_LEASE_MS` | 800 | lease held while one reader loads from PostgreSQL |
| `ACCOUNT_CACHE_MARGIN_MS` | 400 | extra time kept beyond the lease for the filler to write |
| `ACCOUNT_CACHE_STALE_MS` | 5000 | window in which an invalidated entry may still be served |

Both instances must agree on these values: a mismatch would give them different
staleness bounds against the same Redis. Changing the TTL is a compose edit plus a
restart - no rebuild.