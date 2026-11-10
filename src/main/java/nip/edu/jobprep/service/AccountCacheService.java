package nip.edu.jobprep.service;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;


@Service
public class AccountCacheService {

    static final long LEASE_MS = 3000;
    static final long MARGIN_MS = 1000;
    static final long STALE_MS = 5000;
    static final long TTL_MS = 60000;

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

    public AccountCacheService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public static String key(long accountId) {
        return "acc:v1:" + accountId;
    }

    public ReadResult read(long accountId) {
        String token = UUID.randomUUID().toString();
        List<String> r = raw(redis.execute(READ_SCRIPT, List.of(key(accountId)),
                String.valueOf(LEASE_MS), token, String.valueOf(MARGIN_MS)));
        return ReadResult.of(r, token);
    }

    public void fill(long accountId, String token, long version, String balance, String status) {
        long ttl = 51000 + ThreadLocalRandom.current().nextLong(18001); // 51..69 s
        raw(redis.execute(FILL_SCRIPT, List.of(key(accountId)),
                token, String.valueOf(version), balance, status, String.valueOf(ttl)));
    }

    public void invalidate(long accountId, long newVersion) {
        raw(redis.execute(INVALIDATE_SCRIPT, List.of(key(accountId)),
                String.valueOf(newVersion), String.valueOf(STALE_MS)));
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