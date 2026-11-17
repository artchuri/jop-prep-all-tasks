package nip.edu.jobprep.utils.cache;

import nip.edu.jobprep.dto.AccountResponseDto;
import java.math.BigDecimal;
import nip.edu.jobprep.config.AccountCacheProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.lang.Nullable;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class LuaCache implements Cache {

    public static final String NAME = "accounts";

    private static final Logger log = LoggerFactory.getLogger(LuaCache.class);

    private final AccountCacheScripts scripts;
    private final MeterRegistry meters;
    private final AccountCacheProperties properties;

    private final ConcurrentHashMap<Long, CompletableFuture<AccountSnapshot>> inFlight = new ConcurrentHashMap<>();

    private static final long WAIT_STEP_MS = 100;

    public LuaCache(AccountCacheScripts scripts, MeterRegistry meters, AccountCacheProperties properties) {
        this.scripts = scripts;
        this.meters = meters;
        this.properties = properties;
        meters.counter("cache_evict_total", "result", "success");
        meters.counter("cache_evict_total", "result", "error");
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Object getNativeCache() {
        return scripts;
    }

    @Nullable
    @Override
    public ValueWrapper get(Object key) {
        ReadResult r = read(key);
        return r.value() == null ? null : new SimpleValueWrapper(toDto(r.value()));
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public <T> T get(Object key, @Nullable Class<T> type) {
        ReadResult r = read(key);
        if (r.value() == null) {
            return null;
        }
        T dto = (T) toDto(r.value());
        if (type != null && !type.isInstance(dto)) {
            throw new IllegalStateException("Cached value is not of required type [" + type.getName() + "]: " + dto);
        }
        return dto;
    }

    /**
     * Direct PostgreSQL load used when Redis is unavailable on the read path.
     * The cache is an optimisation, not a requirement: the GET must not fail
     * because of a Redis outage, so the value loader (the DB) is invoked and
     * the result is served without a cache write ("controlled PG fallback").
     * A loader failure is wrapped like any other loader exception.
     */
    @SuppressWarnings("unchecked")
    private <T> T directLoad(Object key, Callable<T> valueLoader) {
        try {
            return valueLoader.call();
        } catch (Exception e) {
            throw new ValueRetrievalException(key, valueLoader, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        long accountId = asLong(key);
        ReadResult r = redisQuietly("get", () -> read(accountId), null);
        if (r == null) {
            // Redis read failed outright (outage): serve from PostgreSQL.
            return (T) directLoad(key, valueLoader);
        }
        if (r.value() != null) {
            return (T) toDto(r.value());
        }
        if (r.leaseToken() != null) {
            return (T) loadUnderLease(key, accountId, r.leaseToken(), valueLoader);
        }
        // WAIT: a lease is held - by a loader on this instance (its future is in
        // the map) or by the peer instance (no local future, bounded poll instead).
        AccountSnapshot viaFlight = awaitInFlight(accountId);
        if (viaFlight != null) {
            return (T) toDto(viaFlight);
        }
        WaitOutcome waited = waitThenRead(accountId, r.waitRemainingMs());
        if (waited.snapshot() != null) {
            return (T) toDto(waited.snapshot());
        }
        if (waited.token() != null) {
            // The peer's lease expired while polling and Redis granted us the
            // fill: load under our own token so the result is cached.
            return (T) loadUnderLease(key, accountId, waited.token(), valueLoader);
        }
        // Wait window is over and the cache is still empty. Collapse concurrent
        // waiters into one PostgreSQL query per instance instead of a stampede.
        return (T) loadSingleFlight(key, accountId, valueLoader);
    }

    private <T> T loadUnderLease(Object key, long accountId, String token, Callable<T> valueLoader) {
        CompletableFuture<AccountSnapshot> flight = inFlight.computeIfAbsent(accountId, k -> new CompletableFuture<>());
        try {
            T loaded = valueLoader.call();
            if (loaded instanceof AccountResponseDto dto) {
                if (supersededByTombstone(accountId, dto.version())) {
                    // The account changed while this PG load ran: the Redis fill of
                    // this snapshot would be tombstone-rejected, so waiters must not
                    // receive it either - they re-resolve (STALE / fresh load).
                    // The leader still returns its own PG result: the GET must not
                    // fail after a successful read.
                    flight.complete(null);
                } else {
                    // Waiters unblock with the DB result immediately, even if the cache
                    // write below fails: a Redis outage must never turn a successful
                    // read into a 500 - the cache is an optimisation, not a requirement.
                    flight.complete(new AccountSnapshot(accountId, dto.balance().toPlainString(), dto.status(), dto.version()));
                    fillSafely(accountId, token, dto);
                }
            } else {
                flight.complete(null);
            }
            return loaded;
        } catch (Exception e) {
            flight.completeExceptionally(e);
            releaseQuietly(accountId, token);
            throw new ValueRetrievalException(key, valueLoader, e);
        } finally {
            inFlight.remove(accountId, flight);
        }
    }

    /**
     * Tombstone version of the cached entry, or -1 when there is none (missing
     * key or field). Redis unreachable returns Long.MAX_VALUE: the check cannot
     * be performed, so the snapshot is treated as superseded and waiters
     * re-resolve against PostgreSQL instead of possibly pre-invalidation data.
     */
    private long tombstoneVersion(long accountId) {
        String raw = redisQuietly("get", () -> {
            Object f = scripts.field(accountId, "tver");
            return f == null ? "" : String.valueOf(f);
        }, "REDIS_DOWN");
        if ("REDIS_DOWN".equals(raw)) {
            return Long.MAX_VALUE;
        }
        if (raw.isEmpty()) {
            return -1;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private boolean supersededByTombstone(long accountId, long loadedVersion) {
        return tombstoneVersion(accountId) > loadedVersion;
    }

    /**
     * Writes the loaded snapshot to Redis. Failures are contained: the caller
     * already holds fresh PostgreSQL data, so the GET still succeeds; the lease
     * is released so the next reader can take a fresh one instead of WAIT-ing
     * for a token that will never be filled.
     */
    private void fillSafely(long accountId, String token, AccountResponseDto dto) {
        try {
            scripts.fill(accountId, token, dto.version(), dto.balance().toPlainString(), dto.status());
        } catch (RuntimeException e) {
            meters.counter("cache_errors_total", "operation", "fill").increment();
            log.warn("cache fill failed for account {}: {} - serving DB result, releasing lease", accountId, e.getMessage());
            releaseQuietly(accountId, token);
        }
    }

    private void releaseQuietly(long accountId, String token) {
        try {
            scripts.release(accountId, token);
        } catch (RuntimeException e) {
            log.warn("lease release failed for account {}: {}", accountId, e.getMessage());
        }
    }


    /**
     * Waits for the loader on this instance to finish. Bounded by the lease
     * window plus grace: past that point the loader is gone and waiting longer
     * would only delay the fallback.
     */
    @Nullable
    private AccountSnapshot awaitInFlight(long accountId) {
        CompletableFuture<AccountSnapshot> flight = inFlight.get(accountId);
        if (flight == null) {
            return null;
        }
        try {
            return flight.get(leaseWaitBudgetMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException | TimeoutException e) {
            return null;
        }
    }

    private long leaseWaitBudgetMs() {
        return properties.leaseMs() + properties.marginMs() + 1000;
    }

    /**
     * Bounded poll for a lease held by the peer instance (no local future to
     * await). The read script reports the remaining lease time, so the poll
     * window spans exactly that - a waiter never bails while the peer can still
     * fill, which is what keeps cross-instance misses from stampeding
     * PostgreSQL. Checked in {@link #WAIT_STEP_MS} slices so an early fill is
     * picked up promptly instead of after the full window.
     */
    private WaitOutcome waitThenRead(long accountId, long waitBudgetMs) {
        long cap = properties.leaseMs() + properties.marginMs();
        long budget = waitBudgetMs > 0 ? Math.min(waitBudgetMs, cap) : cap;
        long waited = 0;
        while (waited < budget) {
            long step = Math.min(WAIT_STEP_MS, budget - waited);
            try {
                Thread.sleep(step);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new WaitOutcome(null, null);
            }
            waited += step;
            AccountCacheScripts.ReadResult r = redisQuietly("get", () -> scripts.read(accountId), null);
            if (r == null) {
                // Redis died mid-wait: stop polling, degrade to a direct load.
                return new WaitOutcome(null, null);
            }
            switch (r.status()) {
                case HIT, STALE -> {
                    return new WaitOutcome(new AccountSnapshot(accountId, r.balance(), r.statusName(), r.version()), null);
                }
                case LEASE -> {
                    // The peer's lease expired mid-poll: we are the filler now.
                    return new WaitOutcome(null, r.token());
                }
                case WAIT -> {
                    // Re-sync to the live lease: it may have been re-granted by
                    // another reader (a new filler) or expire sooner than budgeted.
                    if (r.waitRemainingMs() > 0) {
                        budget = Math.min(r.waitRemainingMs(), cap);
                        waited = 0;
                    }
                }
            }
        }
        return new WaitOutcome(null, null);
    }

    /**
     * Last-resort load after the lease window: a single-flight wrapper so
     * concurrent waiters share one PostgreSQL query instead of stampeding.
     * Before loading it re-tries the lease: with a fresh token the result is
     * cached, so the next reader hits Redis instead of PostgreSQL.
     * A waiter whose leader failed falls through to a direct load - rare (PG
     * error), bounded by the number of concurrent waiters.
     */
    @SuppressWarnings("unchecked")
    private <T> T loadSingleFlight(Object key, long accountId, Callable<T> valueLoader) {
        AccountCacheScripts.ReadResult fresh = redisQuietly("get", () -> scripts.read(accountId), null);
        if (fresh == null) {
            return (T) directLoad(key, valueLoader);
        }
        if (fresh.status() == AccountCacheScripts.Status.HIT || fresh.status() == AccountCacheScripts.Status.STALE) {
            return (T) toDto(new AccountSnapshot(accountId, fresh.balance(), fresh.statusName(), fresh.version()));
        }
        if (fresh.token() != null) {
            return (T) loadUnderLease(key, accountId, fresh.token(), valueLoader);
        }
        CompletableFuture<AccountSnapshot> mine = new CompletableFuture<>();
        CompletableFuture<AccountSnapshot> existing = inFlight.putIfAbsent(accountId, mine);
        if (existing != null) {
            AccountSnapshot s = awaitFuture(existing);
            if (s != null) {
                return (T) toDto(s);
            }
            try {
                return valueLoader.call();
            } catch (Exception e) {
                throw new ValueRetrievalException(key, valueLoader, e);
            }
        }
        try {
            T loaded = valueLoader.call();
            if (loaded instanceof AccountResponseDto dto) {
                mine.complete(new AccountSnapshot(accountId, dto.balance().toPlainString(), dto.status(), dto.version()));
                fillViaFreshLease(accountId, dto);
            } else {
                mine.complete(null);
            }
            return loaded;
        } catch (Exception e) {
            mine.completeExceptionally(e);
            throw new ValueRetrievalException(key, valueLoader, e);
        } finally {
            inFlight.remove(accountId, mine);
        }
    }

    /**
     * The single-flight leader holds no lease token (the old one expired), so
     * after loading it re-reads: if Redis grants a fresh lease the result is
     * cached for the next reader instead of being lost.
     */
    private void fillViaFreshLease(long accountId, AccountResponseDto dto) {
        try {
            AccountCacheScripts.ReadResult fresh = scripts.read(accountId);
            if (fresh.token() != null) {
                fillSafely(accountId, fresh.token(), dto);
            }
        } catch (RuntimeException e) {
            log.warn("post-load fill skipped for account {}: {}", accountId, e.getMessage());
        }
    }

    @Nullable
    private AccountSnapshot awaitFuture(CompletableFuture<AccountSnapshot> future) {
        try {
            return future.get(leaseWaitBudgetMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException | TimeoutException e) {
            return null;
        }
    }

    /**
     * Redis call that degrades instead of throwing: a RuntimeException (Redis
     * outage) is counted, logged, and reported as {@code onUnavailable}.
     * Needed on the sync {@code @Cacheable} path: Spring rethrows anything the
     * cache throws to the caller without consulting the CacheErrorHandler, so
     * without this a Redis outage would break GETs that the DB could serve.
     */
    @Nullable
    private <R> R redisQuietly(String operation, java.util.function.Supplier<R> call, @Nullable R onUnavailable) {
        try {
            return call.get();
        } catch (RuntimeException e) {
            meters.counter("cache_errors_total", "operation", operation).increment();
            log.warn("cache redis call failed ({}): {} - degrading to PostgreSQL", operation, e.getMessage());
            return onUnavailable;
        }
    }

    private ReadResult read(Object key) {
        long accountId = asLong(key);
        AccountCacheScripts.ReadResult r = scripts.read(accountId);
        return switch (r.status()) {
            case HIT, STALE -> new ReadResult(new AccountSnapshot(accountId, r.balance(), r.statusName(), r.version()), null, 0);
            case LEASE -> new ReadResult(null, r.token(), 0);
            case WAIT -> new ReadResult(null, null, r.waitRemainingMs());
        };
    }

    private static AccountResponseDto toDto(AccountSnapshot s) {
        return new AccountResponseDto(s.accountId(), new BigDecimal(s.balance()), s.status(), s.version());
    }

    private static long asLong(Object key) {
        if (key instanceof Number n) return n.longValue();
        return Long.parseLong(String.valueOf(key));
    }

    @Override
    public void put(Object key, @Nullable Object value) {
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, @Nullable Object value) {
        return null;
    }

    @Override
    public void evict(Object key) {
        try {
            if (key instanceof Invalidate i) {
                scripts.invalidate(i.accountId(), i.version());
            } else {
                scripts.invalidate(asLong(key), Long.MAX_VALUE);
            }
            meters.counter("cache_evict_total", "result", "success").increment();
        } catch (RuntimeException e) {
            meters.counter("cache_evict_total", "result", "error").increment();
            throw e;
        }
    }

    @Override
    public boolean evictIfPresent(Object key) {
        evict(key);
        return true;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("clear() is not supported for version-gated Lua cache");
    }

    public record AccountSnapshot(long accountId, String balance, String status, long version) {
    }

    public record Invalidate(long accountId, long version) {
    }

    private record ReadResult(AccountSnapshot value, String leaseToken, long waitRemainingMs) {
    }

    /** Outcome of the cross-instance wait poll: a filled snapshot, a token granted to us, or neither. */
    private record WaitOutcome(AccountSnapshot snapshot, String token) {
    }
}

