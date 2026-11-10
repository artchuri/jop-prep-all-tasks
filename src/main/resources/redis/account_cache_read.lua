-- Cache-aside READ with lease + stale (ACC-106 D-01, invariants I-3, I-5, I-6, I-7).
-- KEYS[1] = account hash key
-- ARGV[1] = lease ms, ARGV[2] = new lease token, ARGV[3] = ttl margin ms
-- Returns: {HIT, bal, st, ver} | {STALE, bal, st, ver} | {LEASE, token} | {WAIT, ''}
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
