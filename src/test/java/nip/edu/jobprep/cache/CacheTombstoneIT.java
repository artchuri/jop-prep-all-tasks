package nip.edu.jobprep.cache;

import org.springframework.data.redis.core.RedisCallback;
import nip.edu.jobprep.config.AccountCacheProperties;
import nip.edu.jobprep.dto.AccountResponseDto;
import nip.edu.jobprep.utils.cache.AccountCacheScripts;
import nip.edu.jobprep.utils.cache.LuaCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tombstone semantics of the version-gated cache (ACC-106 review follow-up):
 *
 * - BUG-A: a STALE entry exposes the version coherent with the bal/st it
 *   carries; the tombstone version lives in a separate field (tver).
 * - BUG-B: invalidating a MISSING key creates a tombstone, so a lease taken
 *   before the commit cannot fill pre-invalidation data afterwards.
 * - BUG-C: waiters never receive a leader snapshot that predates an applied
 *   invalidation - the Redis fill path would reject it, so must the in-JVM
 *   future path.
 */
@Tag("integration")
class CacheTombstoneIT extends AbstractCacheIntegrationTest {

    @Autowired
    CacheManager cacheManager;

    @Autowired
    AccountCacheScripts scripts;

    @Autowired
    AccountCacheProperties properties;

    @Autowired
    StringRedisTemplate redis;

    Cache cache;

    @BeforeEach
    void init() {
        cache = cacheManager.getCache(LuaCache.NAME);
        redis.execute((RedisCallback<Void>) conn -> {
            conn.serverCommands().flushAll();
            return null;
        });
    }

    @Test
    void staleEntryStaysCoherentAndTombstoneGatesFills() {
        warm(9, "100.00", "ACTIVE", 0);
        // a fill records the tombstone version it observed; before any
        // invalidation it simply equals the filled version
        assertThat(field(9, "tver")).isEqualTo("0");

        assertThat(invalidateRaw(9, 1)).isEqualTo("VOIDED");

        // BUG-A: displayed version stays coherent with the balance it labels;
        // the tombstone version advanced separately.
        assertThat(field(9, "ver")).isEqualTo("0");
        assertThat(field(9, "tver")).isEqualTo("1");
        // STALE is served to a reader holding a lease over the stale entry:
        // the first read grants the lease, the second reads under it.
        scripts.read(9);
        AccountCacheScripts.ReadResult stale = scripts.read(9);
        assertThat(stale.status()).isEqualTo(AccountCacheScripts.Status.STALE);
        assertThat(stale.version()).isEqualTo(0L);
        assertThat(stale.balance()).isEqualTo("100.00");
    }

    @Test
    void missingKeyInvalidationCreatesTombstoneThatGatesStaleFill() {
        // No key yet: invalidation must leave a tombstone, not a NOOP.
        assertThat(invalidateRaw(42, 5)).isEqualTo("TOMBSTONED");
        assertThat(field(42, "tver")).isEqualTo("5");

        // A lease granted over the tombstone cannot fill pre-commit data (v4),
        // but a fresh load (v5) passes the gate.
        AccountCacheScripts.ReadResult lease = scripts.read(42);
        assertThat(lease.status()).isEqualTo(AccountCacheScripts.Status.LEASE);

        assertThat(fillStatus(42, lease.token(), 4, "1.00", "ACTIVE")).isEqualTo("REJECTED");
        assertThat(redis.opsForHash().get(AccountCacheScripts.key(42), "st"))
                .as("rejected fill must not write data").isNull();
        assertThat(field(42, "tver")).as("tombstone survives the rejected fill").isEqualTo("5");

        assertThat(fillStatus(42, lease.token(), 5, "1.00", "ACTIVE")).isEqualTo("OK");
        assertThat(field(42, "ver")).isEqualTo("5");
        assertThat(field(42, "tver")).isEqualTo("5");
    }

    @Test
    void waitersNeverReceiveSnapshotOlderThanTombstone() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch leaderInFlight = new CountDownLatch(1);
        CountDownLatch releaseLeader = new CountDownLatch(1);

        // Leader: takes the lease, loads slowly from "PG" (v0 snapshot).
        Callable<AccountResponseDto> leaderLoader = () -> {
            leaderInFlight.countDown();
            releaseLeader.await(5, TimeUnit.SECONDS);
            return new AccountResponseDto(1L, new BigDecimal("100.00"), "ACTIVE", 0L);
        };
        // Waiter's own "PG" view reflects the committed change (v1, FROZEN).
        Callable<AccountResponseDto> waiterLoader = () ->
                new AccountResponseDto(1L, new BigDecimal("0.00"), "FROZEN", 1L);

        Future<AccountResponseDto> leader = pool.submit(() -> cache.get(1L, leaderLoader));
        assertThat(leaderInFlight.await(5, TimeUnit.SECONDS)).isTrue();

        Future<AccountResponseDto> waiter = pool.submit(() -> cache.get(1L, waiterLoader));
        // Deterministic overlap: the waiter is parked on the leader's future.
        awaitInFlight(1L);

        // Freeze commits while the leader is mid-load; invalidation applies.
        assertThat(invalidateRaw(1, 1)).isEqualTo("VOIDED");
        releaseLeader.countDown();

        AccountResponseDto leaderDto = leader.get(10, TimeUnit.SECONDS);
        // The leader still returns its own PG result (a GET must not fail after
        // a successful read) - coherent v0, never mislabelled as v1.
        assertThat(leaderDto.version()).isEqualTo(0L);

        // BUG-C: the waiter re-resolved instead of receiving the v0 snapshot.
        AccountResponseDto waiterDto = waiter.get(10, TimeUnit.SECONDS);
        assertThat(waiterDto).isNotNull();
        assertThat(waiterDto.version()).isEqualTo(1L);
        assertThat(waiterDto.status()).isEqualTo("FROZEN");

        pool.shutdown();
    }

    /** Waits until a lease is live on the key (state=L), i.e. a loader is mid-flight. */
    private void awaitInFlight(long accountId) {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(field(accountId, "state")).isEqualTo("L"));
        // small settle time so the waiter is parked on the future before invalidation
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Runs the fill script directly with an explicit version; surfaces REJECTED statuses. */
    private String fillStatus(long id, String token, long version, String bal, String st) {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis/account_cache_fill.lua"));
        script.setResultType(List.class);
        List<String> out = redis.execute(script, List.of(AccountCacheScripts.key(id)),
                token, String.valueOf(version), bal, st, "60000");
        return out.get(0);
    }

    /** Runs the invalidate script directly; surfaces NOOP / VOIDED / TOMBSTONED. */
    private String invalidateRaw(long id, long version) {
        return scripts.invalidateRaw(id, version);
    }

    private void warm(long id, String bal, String st, long ver) {
        cache.get(id, () -> new AccountResponseDto(id, new BigDecimal(bal), st, ver));
    }

    private Object field(long id, String name) {
        return redis.opsForHash().get(AccountCacheScripts.key(id), name);
    }
}
