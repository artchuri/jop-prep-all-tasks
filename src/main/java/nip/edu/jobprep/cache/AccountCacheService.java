package nip.edu.jobprep.cache;

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

    private static final String READ_LUA = """
            local k = KEYS[1]
            local leaseMs = tonumber(ARGV[1])
            local token = ARGV[2]
            local margin = tonumber(ARGV[3])
            local t = redis.call('TIME')
            local now = t[1] * 1000 + math.floor(t[2] / 1000)
            if redis.call('EXISTS', k) == 0 then
                redis.call('HSET', k, 'state', 'L', 'tok', token, 'lexp', now + leaseMs)
                redis.call('PEXPIRE', k, leaseMs + margin)
                return {'LEASE', token}
            end
            local h = redis.call('HGETALL', k)
            local m = {}
            for i = 1, #h, 2 do m[h[i]] = h[i + 1] end
            if m['state'] == 'F' then
                return {'HIT', m['bal'] or '', m['st'] or '', m['ver'] or ''}
            end
            if m['tok'] and m['tok'] ~= '' and tonumber(m['lexp'] or 0) > now then
                if m['state'] == 'S' and tonumber(m['sdl'] or 0) > now then
                    return {'STALE', m['bal'] or '', m['st'] or '', m['ver'] or ''}
                end
                return {'WAIT', ''}
            end
            redis.call('HSET', k, 'tok', token, 'lexp', now + leaseMs)
            local pttl = redis.call('PTTL', k)
            local ttl = math.max(pttl, leaseMs + margin)
            if ttl > 0 then redis.call('PEXPIRE', k, ttl) end
            return {'LEASE', token}
            """;

    private static final String FILL_LUA = """
            local k = KEYS[1]
            local token = ARGV[1]
            local loadedVer = tonumber(ARGV[2])
            local bal = ARGV[3]
            local st = ARGV[4]
            local ttlMs = tonumber(ARGV[5])
            if redis.call('EXISTS', k) == 0 then return {'REJECTED', 'missing_key'} end
            local h = redis.call('HGETALL', k)
            local m = {}
            for i = 1, #h, 2 do m[h[i]] = h[i + 1] end
            if m['tok'] ~= token then return {'REJECTED', 'token_void'} end
            local t = redis.call('TIME')
            local now = t[1] * 1000 + math.floor(t[2] / 1000)
            if tonumber(m['lexp'] or 0) <= now then return {'REJECTED', 'token_void'} end
            if m['ver'] and loadedVer < tonumber(m['ver']) then return {'REJECTED', 'older_version'} end
            redis.call('HSET', k, 'bal', bal, 'st', st, 'ver', tostring(loadedVer), 'state', 'F')
            redis.call('HDEL', k, 'tok', 'lexp', 'sdl')
            redis.call('PEXPIRE', k, ttlMs)
            return {'OK', ''}
            """;

    private static final String INVALIDATE_LUA = """
            local k = KEYS[1]
            local newVer = tonumber(ARGV[1])
            local staleMs = tonumber(ARGV[2])
            if redis.call('EXISTS', k) == 0 then return {'NOOP', ''} end
            local h = redis.call('HGETALL', k)
            local m = {}
            for i = 1, #h, 2 do m[h[i]] = h[i + 1] end
            local cur = tonumber(m['ver'] or -1)
            if m['state'] == 'F' and cur >= newVer then return {'NOOP', ''} end
            if newVer > cur then redis.call('HSET', k, 'ver', tostring(newVer)) end
            local t = redis.call('TIME')
            local now = t[1] * 1000 + math.floor(t[2] / 1000)
            if m['state'] == 'F' then
                redis.call('HSET', k, 'state', 'S', 'sdl', now + staleMs)
                redis.call('PEXPIRE', k, staleMs + MARGIN_MS)
            end
            redis.call('HDEL', k, 'tok', 'lexp')
            return {'VOIDED', ''}
            """.replace("MARGIN_MS", String.valueOf(MARGIN_MS));

    private static final String RELEASE_LUA = """
            local k = KEYS[1]
            local token = ARGV[1]
            local h = redis.call('HGETALL', k)
            local m = {}
            for i = 1, #h, 2 do m[h[i]] = h[i + 1] end
            if m['tok'] == token then
                if m['state'] == 'L' then
                    redis.call('DEL', k)
                else
                    redis.call('HDEL', k, 'tok', 'lexp')
                end
            end
            return {'OK', ''}
            """;


    private static final DefaultRedisScript<List> READ_SCRIPT = script(READ_LUA);

    private static final DefaultRedisScript<List> FILL_SCRIPT = script(FILL_LUA);

    private static final DefaultRedisScript<List> INVALIDATE_SCRIPT = script(INVALIDATE_LUA);
    private static final DefaultRedisScript<List> RELEASE_SCRIPT = script(RELEASE_LUA);

    private static DefaultRedisScript<List> script(String lua) {
        DefaultRedisScript<List> s = new DefaultRedisScript<>();
        s.setScriptText(lua);
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