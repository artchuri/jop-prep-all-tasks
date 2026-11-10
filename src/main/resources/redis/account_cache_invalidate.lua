-- INVALIDATE: never lowers version (I-2), annuls any lease, FRESH -> STALE.
-- ARGV[1] = new version (always known in this codebase), ARGV[2] = stale window ms.
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
    redis.call('PEXPIRE', k, staleMs + 1000)
end
redis.call('HDEL', k, 'tok', 'lexp')
return {'VOIDED', ''}
