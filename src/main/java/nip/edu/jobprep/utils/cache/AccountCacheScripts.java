package nip.edu.jobprep.utils.cache;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import nip.edu.jobprep.config.AccountCacheProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;



@Component
public class AccountCacheScripts {

    @SuppressWarnings("rawtypes")
    private static final DefaultRedisScript<List> READ_SCRIPT = script("redis/account_cache_read.lua");
    @SuppressWarnings("rawtypes")
    private static final DefaultRedisScript<List> FILL_SCRIPT = script("redis/account_cache_fill.lua");
    @SuppressWarnings("rawtypes")
    private static final DefaultRedisScript<List> INVALIDATE_SCRIPT = script("redis/account_cache_invalidate.lua");
    @SuppressWarnings("rawtypes")
    private static final DefaultRedisScript<List> RELEASE_SCRIPT = script("redis/account_cache_release.lua");

    @SuppressWarnings("rawtypes")
    private static DefaultRedisScript<List> script(String location) {
        DefaultRedisScript<List> s = new DefaultRedisScript<>();
        s.setLocation(new ClassPathResource(location));
        s.setResultType(List.class);
        return s;
    }

    private final Map<String, Counter> requestCounters;
    private final Timer readSuccess;
    private final Timer readError;
    private final Timer invalidateSuccess;
    private final Timer invalidateError;

    private final StringRedisTemplate redis;
    private final MeterRegistry meters;
    private final AccountCacheProperties properties;

    public AccountCacheScripts(StringRedisTemplate redis, MeterRegistry meters, AccountCacheProperties properties) {
        this.redis = redis;
        this.meters = meters;
        this.properties = properties;
        this.requestCounters = Map.of(
                "hit", requestCounter("hit"),
                "stale", requestCounter("stale"),
                "miss_lease", requestCounter("miss_lease"),
                "wait", requestCounter("wait"));
        this.readSuccess = redisTimer("read", "success");
        this.readError = redisTimer("read", "error");
        this.invalidateSuccess = redisTimer("invalidate", "success");
        this.invalidateError = redisTimer("invalidate", "error");
    }

    private Counter requestCounter(String result) {
        return Counter.builder("cache_requests_total")
                .description("Account cache reads by outcome")
                .tag("result", result)
                .register(meters);
    }

    private Timer redisTimer(String op, String outcome) {
        return Timer.builder("cache_redis_latency_seconds")
                .description("Redis call latency for cache operations")
                .tag("op", op)
                .tag("outcome", outcome)
                .publishPercentileHistogram()
                .register(meters);
    }

    public static String key(long accountId) {
        return "acc:v1:" + accountId;
    }

    public ReadResult read(long accountId) {
        String token = UUID.randomUUID().toString();
        long start = System.nanoTime();
        List<String> r;
        boolean readFailed = false;
        try {
            r = raw(redis.execute(READ_SCRIPT, List.of(key(accountId)),
                    String.valueOf(properties.leaseMs()), token, String.valueOf(properties.marginMs())));
        } catch (RuntimeException e) {
            readFailed = true;
            throw e;
        } finally {
            (readFailed ? readError : readSuccess)
                    .record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
        }
        ReadResult result = ReadResult.of(r, token);
        requestCounters.get(switch (result.status()) {
            case HIT -> "hit";
            case STALE -> "stale";
            case LEASE -> "miss_lease";
            case WAIT -> "wait";
        }).increment();
        return result;
    }

    public void fill(long accountId, String token, long version, String balance, String status) {
        fillRaw(accountId, token, version, balance, status);
    }

    /** Raw fill result for tests and diagnostics: REJECTED statuses are returned, not swallowed. */
    public ReadResult fillRaw(long accountId, String token, long version, String balance, String status) {
        List<String> out = raw(redis.execute(FILL_SCRIPT, List.of(key(accountId)),
                token, String.valueOf(version), balance, status, String.valueOf(ttlWithJitter())));
        return ReadResult.of(out, token);
    }

    private long ttlWithJitter() {
        long ttl = properties.ttlMs() + ThreadLocalRandom.current().nextLong(-properties.ttlJitterMs(), properties.ttlJitterMs() + 1);
        return ttl < 1 ? 1 : ttl;
    }

    public void invalidate(long accountId, long newVersion) {
        invalidateRaw(accountId, newVersion);
    }

    /** Raw invalidate result for tests and diagnostics: NOOP / VOIDED / TOMBSTONED. */
    public String invalidateRaw(long accountId, long newVersion) {
        long start = System.nanoTime();
        boolean invalidateFailed = false;
        try {
            List<String> out = raw(redis.execute(INVALIDATE_SCRIPT, List.of(key(accountId)),
                    String.valueOf(newVersion), String.valueOf(properties.staleMs()),
                    String.valueOf(properties.leaseMs()), String.valueOf(properties.marginMs())));
            return out.get(0);
        } catch (RuntimeException e) {
            invalidateFailed = true;
            throw e;
        } finally {
            (invalidateFailed ? invalidateError : invalidateSuccess)
                    .record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
        }
    }

    public void release(long accountId, String token) {
        raw(redis.execute(RELEASE_SCRIPT, List.of(key(accountId)), token));
    }

    @SuppressWarnings("unchecked")
    private List<String> raw(List<?> result) {
        return (List<String>) result;
    }

    public enum Status { HIT, STALE, LEASE, WAIT }

    public record ReadResult(Status status, String token, String balance, String statusName, long version, long waitRemainingMs) {
        static ReadResult of(List<String> r, String token) {
            Status s = Status.valueOf(r.get(0));
            if (s == Status.HIT || s == Status.STALE) {
                return new ReadResult(s, token, r.get(1), r.get(2), Long.parseLong(r.get(3)), 0);
            }
            if (s == Status.WAIT) {
                long remaining = 0;
                try {
                    remaining = Math.max(Long.parseLong(r.get(1)), 0);
                } catch (RuntimeException ignored) {
                }
                return new ReadResult(s, token, null, null, 0, remaining);
            }
            return new ReadResult(s, token, null, null, 0, 0);
        }
    }

    public Object field(long accountId, String field) {
        return redis.opsForHash().get(key(accountId), field);
    }
}