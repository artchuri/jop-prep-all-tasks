package nip.edu.jobprep.cache;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.RedisCallback;
import nip.edu.jobprep.dto.AccountResponseDto;
import nip.edu.jobprep.utils.cache.AccountCacheScripts;
import nip.edu.jobprep.utils.cache.LuaCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ContextConfiguration;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-instance cache-miss coordination without a second application: the
 * "remote leader" is simulated by taking the lease directly from Redis and
 * never filling it, so every {@link LuaCache} reader is a cross-instance waiter.
 *
 * <p>Blocks the stampede regression: while that lease is alive no waiter may
 * reach PostgreSQL (the old code bailed out of its ~60 ms poll and queried PG
 * every wave for the whole lease window); after expiry the waiters collapse
 * into one single-flight PostgreSQL query whose result is cached.
 */
@Tag("integration")
@ContextConfiguration(initializers = CrossInstanceLeaseWaitIT.ShortLease.class)
class CrossInstanceLeaseWaitIT extends AbstractCacheIntegrationTest {

    /** Short lease (lease + margin = 400 ms) keeps the wait window fast but still far above the old ~60 ms bail-out. */
    public static class ShortLease extends Initializer {
        @Override
        public void initialize(ConfigurableApplicationContext context) {
            super.initialize(context);
            TestPropertyValues.of(
                    "account-cache.lease-ms=300",
                    "account-cache.margin-ms=100"
            ).applyTo(context.getEnvironment());
        }
    }

    @Autowired
    CacheManager cacheManager;

    @Autowired
    MeterRegistry meters;

    @Autowired
    AccountCacheScripts scripts;

    @Autowired
    StringRedisTemplate redis;

    Cache cache;

    @BeforeEach
    void initCache() {
        cache = cacheManager.getCache(LuaCache.NAME);
        redis.execute((RedisCallback<Void>) conn -> {
            conn.serverCommands().flushAll();
            return null;
        });
    }

    @Test
    void waitersHoldForTheWholeLeaseThenCollapseIntoOnePgQuery() throws Exception {
        long accountId = 4242;

        // The "peer instance" takes the lease and is slow: it never fills.
        AccountCacheScripts.ReadResult lease = scripts.read(accountId);
        assertThat(lease.status()).isEqualTo(AccountCacheScripts.Status.LEASE);

        AtomicInteger dbCalls = new AtomicInteger();
        Callable<AccountResponseDto> loader = () -> {
            dbCalls.incrementAndGet();
            return new AccountResponseDto(accountId, new BigDecimal("100.00"), "ACTIVE", 1);
        };

        double waitBefore = meters.counter("cache_requests_total", "result", "wait").count();
        int waiters = 8;
        ExecutorService pool = Executors.newFixedThreadPool(waiters);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Long>> results = new ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            results.add(pool.submit(() -> {
                start.await();
                long t0 = System.nanoTime();
                AccountResponseDto dto = cache.get(accountId, loader);
                assertThat(dto).isNotNull();
                return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            }));
        }
        start.countDown();

        long minElapsed = Long.MAX_VALUE;
        for (Future<Long> f : results) {
            minElapsed = Math.min(minElapsed, f.get(10, TimeUnit.SECONDS));
        }
        pool.shutdown();

        // Waiters held for (at least) the lease window instead of bailing out
        // early - that early bail was the stampede on PostgreSQL.
        assertThat(minElapsed).isGreaterThanOrEqualTo(250);
        // All 8 readers observed a live foreign lease (WAIT): with the lease
        // pre-granted, every reader must reach the wait loop, never the loader.
        // Each waiter polls in 100 ms slices, so count outcomes, not calls:
        // >= 8 proves every reader saw at least one WAIT while the lease lived.
        assertThat(meters.counter("cache_requests_total", "result", "wait").count() - waitBefore)
                .isGreaterThanOrEqualTo(8.0);

        // Collapse: the expired lease admits one filler; at most one straggler
        // that raced past the in-flight registration also reaches PostgreSQL.
        assertThat(dbCalls.get()).isLessThanOrEqualTo(2);

        // The single-flight leader's fill landed under a fresh lease, so the
        // next reader is served from Redis without touching PostgreSQL again.
        int before = dbCalls.get();
        assertThat(cache.get(accountId, loader)).isNotNull();
        assertThat(dbCalls.get()).isEqualTo(before);
    }
}
