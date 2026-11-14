package nip.edu.jobprep.utils.cache;

import io.micrometer.core.instrument.MeterRegistry;
import nip.edu.jobprep.config.AccountCacheProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
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

    private final StringRedisTemplate redis;
    private final MeterRegistry meters;
    private final AccountCacheProperties properties;

    public AccountCacheScripts(StringRedisTemplate redis, MeterRegistry meters, AccountCacheProperties properties) {
        this.redis = redis;
        this.meters = meters;
        this.properties = properties;
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
            meters.timer("cache_redis_latency_seconds", "op", "read", "outcome", readFailed ? "error" : "success")
                    .record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
        }
        ReadResult result = ReadResult.of(r, token);
        meters.counter("cache_requests_total", "result", switch (result.status()) {
            case HIT -> "hit";
            case STALE -> "stale";
            case LEASE -> "miss_lease";
            case WAIT -> "wait";
        }).increment();
        return result;
    }

    public void fill(long accountId, String token, long version, String balance, String status) {
        long ttl = properties.ttlMs() + ThreadLocalRandom.current().nextLong(-properties.ttlJitterMs(), properties.ttlJitterMs() + 1);
        if (ttl < 1) ttl = 1;
        raw(redis.execute(FILL_SCRIPT, List.of(key(accountId)),
                token, String.valueOf(version), balance, status, String.valueOf(ttl)));
    }

    public void invalidate(long accountId, long newVersion) {
        long start = System.nanoTime();
        boolean invalidateFailed = false;
        try {
            raw(redis.execute(INVALIDATE_SCRIPT, List.of(key(accountId)),
                    String.valueOf(newVersion), String.valueOf(properties.staleMs())));
        } catch (RuntimeException e) {
            invalidateFailed = true;
            throw e;
        } finally {
            meters.timer("cache_redis_latency_seconds", "op", "invalidate", "outcome", invalidateFailed ? "error" : "success")
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

    public record ReadResult(Status status, String token, String balance, String statusName, long version) {
        static ReadResult of(List<String> r, String token) {
            Status s = Status.valueOf(r.get(0));
            if (s == Status.HIT || s == Status.STALE) {
                return new ReadResult(s, token, r.get(1), r.get(2), Long.parseLong(r.get(3)));
            }
            return new ReadResult(s, token, null, null, 0);
        }
    }

    public Object field(long accountId, String field) {
        return redis.opsForHash().get(key(accountId), field);
    }
}