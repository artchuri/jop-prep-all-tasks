package nip.edu.jobprep.ratelimit;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.List;

public final class RateLimitScripts implements org.springframework.beans.factory.DisposableBean {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(RateLimitScripts.class);

    private static final DefaultRedisScript<List> RESERVE_SCRIPT = script("redis/rate_limit_reserve.lua");

    private static final DefaultRedisScript<Long> REFUND_LONG_SCRIPT = longScript("redis/rate_limit_refund.lua");

    private static DefaultRedisScript<List> script(String location) {
        DefaultRedisScript<List> s = new DefaultRedisScript<>();
        s.setLocation(new ClassPathResource(location));
        s.setResultType(List.class);
        return s;
    }

    private static DefaultRedisScript<Long> longScript(String location) {
        DefaultRedisScript<Long> s = new DefaultRedisScript<>();
        s.setLocation(new ClassPathResource(location));
        s.setResultType(Long.class);
        return s;
    }

    private final StringRedisTemplate redis;

    private final RateLimitMetrics metrics;

    public RateLimitScripts(RedisConnectionFactory rateLimitConnectionFactory, RateLimitMetrics metrics) {
        this.redis = new StringRedisTemplate(rateLimitConnectionFactory);
        this.metrics = metrics;
    }

    @SuppressWarnings("unchecked")
    public ReserveResult reserve(List<String> keys, long nowMs, long windowMs, double limit) {
        long start = System.nanoTime();
        boolean failed = false;
        try {
            List<String> out = (List<String>) redis.execute(RESERVE_SCRIPT, keys,
                    String.valueOf(nowMs), String.valueOf(windowMs), String.valueOf(limit));
            return new ReserveResult(
                    "1".equals(out.get(0)),
                    Long.parseLong(out.get(1)),
                    Long.parseLong(out.get(2)) / 100.0);
        } catch (RuntimeException e) {
            failed = true;
            metrics.redisErrorReserve();
            throw e;
        } finally {
            metrics.evalReserve(failed, System.nanoTime() - start);
        }
    }

    public Long refund(String key) {
        long start = System.nanoTime();
        boolean failed = false;
        try {
            return redis.execute(REFUND_LONG_SCRIPT, List.of(key));
        } catch (RuntimeException e) {
            failed = true;
            metrics.redisErrorRefund();
            log.warn("rate limit refund failed (slot left to TTL out): {}: {}",
                    e.getClass().getSimpleName(), e.getMessage());
            return null;
        } finally {
            metrics.evalRefund(failed, System.nanoTime() - start);
        }
    }

    public record ReserveResult(boolean allowed, long retryAfterMs, double used) {
    }

    @Override
    public void destroy() {
        if (redis.getConnectionFactory() instanceof org.springframework.data.redis.connection.jedis.JedisConnectionFactory jf) {
            jf.destroy();
        }
    }
}
