# ACC-10x: Distributed account cache (Redis)

Cache-aside reading of accounts on Redis; PostgreSQL stays the source of truth.

## Consistency model
- Financial operations (transfer, freeze) never read the cache: they read PG under row locks (SELECT FOR UPDATE) and keep their existing atomicity/idempotency guarantees.
- GET /v1/accounts/{id} may serve stale data, bounded by:
  - up to lease TTL (3 s) after commit in normal operation (Tier 1 invalidation);
  - seconds when Tier 1 is lost but Tier 2 (outbox -> Kafka -> consumer) delivers;
  - up to the record TTL (60 s +-15% jitter) when the invalidation is fully lost.
- The cache entry stores id, balance (amount), status, version; GET also exposes version, Cache-Control: no-store.

## Invalidation
- Tier 1: AFTER_COMMIT listener, best-effort, Redis command timeout 100 ms; errors counted, never propagated to the committed operation.
- Tier 2: transactional outbox (TRANSFER_COMPLETED with fromAccountVersion/toAccountVersion, ACCOUNT_FROZEN with version) -> Kafka topic account-events -> consumer group account-cache-invalidation; offset committed only after successful invalidate; Redis failures block and retry with 1 s backoff, never DLT.
- TTL 60 s +-15% is the last-resort recovery layer.

## Known limitations (agreed decision)
- After the outbox publisher exhausts MAX_ATTEMPTS=5 for a row (sustained Kafka outage beyond ~10 min of delivery timeouts), the row is REJECTED and that invalidation is never delivered; recovery is by TTL (<= 69 s staleness) only. This is accepted for ACC-10x and revisited if product requires faster recovery.
- Head-of-line: hasOlderActiveForUser gates delivery per user_id; a stalled older row delays newer invalidations of the same user (bounded by lease/бackoff, still covered by TTL).
- Concurrent cache-miss stampede is bounded by the lease (one filler per key) and the WAIT loop; a slow filler leads to a controlled PG fallback without cache write.
