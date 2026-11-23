package nip.edu.jobprep.ratelimit;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Tag;
import org.springframework.data.redis.core.RedisCallback;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class LuaReserveRefundLogicIT {
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    private static JedisConnectionFactory factory;

    private static StringRedisTemplate redis;

    private static RateLimitScripts scripts;

    private static RateLimitMetrics metrics;

    private static final long WINDOW = 60000L;
    private static final double LIMIT = 3.0;

    @BeforeAll
    static void start() {
        REDIS.start();
        factory = new JedisConnectionFactory(new RedisStandaloneConfiguration(
                REDIS.getHost(), REDIS.getMappedPort(6379)));
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
        SimpleMeterRegistry registry =
                new SimpleMeterRegistry();
        metrics = new RateLimitMetrics(registry, new RateLimitProperties(
                false, "reject", "/v1", WINDOW, 600, 30,
                List.of(new RateLimitProperties.ClientSpec("c", "k")),
                List.of(), 16, 8, 2, 25, List.of()));
        scripts = new RateLimitScripts(factory, metrics);
        redis.execute((RedisCallback<Void>) c -> { c.serverCommands().flushAll(); return null; });
    }

    @AfterAll
    static void stop() {
        factory.destroy();
        REDIS.stop();
    }

    private RateLimitScripts.ReserveResult reserve(long nowMs) {
        long bucket = Math.floorDiv(nowMs, WINDOW);
        List<String> keys = List.of("{" + RateLimiter.clientTag("k") + "}:rate:t:b" + bucket,
                "{" + RateLimiter.clientTag("k") + "}:rate:t:b" + (bucket - 1));
        return scripts.reserve(keys, nowMs, WINDOW, LIMIT);
    }

    @Test
    void underLimitAllows() {
        long now = 1_000_000L;
        assertThat(reserve(now).allowed()).isTrue();
        assertThat(reserve(now + 1).allowed()).isTrue();
        assertThat(reserve(now + 2).allowed()).isTrue();
    }

    @Test
    void overLimitRejectsWithRetryAfter() {
        long now = 2_000_000L;
        reserve(now); reserve(now + 1); reserve(now + 2);
        RateLimitScripts.ReserveResult rejected = reserve(now + 3);
        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.retryAfterMs()).isBetween(1L, WINDOW);
    }

    @Test
    void rejectedRequestDoesNotConsumeTheLimit() {
        long now = 3_000_000L;
        reserve(now); reserve(now + 1); reserve(now + 2);
        assertThat(reserve(now + 3).allowed()).isFalse();
        scripts.refund("{" + RateLimiter.clientTag("k") + "}:rate:t:b" + Math.floorDiv(now, WINDOW));
        assertThat(reserve(now + 4).allowed()).isTrue();
    }

    @Test
    void refundReturnsTheSlot() {
        long now = 4_000_000L;
        RateLimitScripts.ReserveResult first = reserve(now);
        assertThat(first.allowed()).isTrue();
        scripts.refund("{" + RateLimiter.clientTag("k") + "}:rate:t:b" + Math.floorDiv(now, WINDOW));
        assertThat(reserve(now + 1).allowed()).isTrue();
        assertThat(reserve(now + 2).allowed()).isTrue();
        assertThat(reserve(now + 3).allowed()).isTrue();
    }

    @Test
    void refundOnEmptyBucketIsNoop() {
        assertThat(scripts.refund("{" + RateLimiter.clientTag("k") + "}:rate:ghost:b1")).isEqualTo(0L);
    }

    @Test
    void slidingWindowBoundaryBehaviour() {
        long t0 = 10_000_000L;
        long base = (t0 / WINDOW) * WINDOW;
        for (int i = 0; i < 3; i++) assertThat(reserve(base + i).allowed()).isTrue();
        assertThat(reserve(base + 4).allowed()).isFalse();
        long at59 = base + 59_000;
        assertThat(reserve(at59).allowed()).isFalse();
        long at61 = base + 61_000;
        RateLimitScripts.ReserveResult r = reserve(at61);
        assertThat(r.allowed()).isFalse();
        long later = base + 105_000;
        assertThat(reserve(later).allowed()).isTrue();
    }

    @Test
    void usageGrowsWithRequests() {
        long now = 20_000_000L;
        double u0 = reserve(now).used();
        double u1 = reserve(now + 1).used();
        assertThat(u1).isGreaterThan(u0);
    }
}
