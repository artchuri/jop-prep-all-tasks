local k = KEYS[1]
local newVer = tonumber(ARGV[1])
local staleMs = tonumber(ARGV[2])
local leaseMs = tonumber(ARGV[3])
local marginMs = tonumber(ARGV[4])
-- Tombstone version: gates fills and NOOP decisions. Lives independently of the
-- displayed version, which must stay coherent with the bal/st it labels.
if redis.call('EXISTS', k) == 0 then
    -- Missing key: create a tombstone so a lease taken BEFORE this commit cannot
    -- fill pre-invalidation data after it (fill gates loadedVer < tver).
    redis.call('HSET', k, 'state', 'L', 'tver', tostring(newVer))
    redis.call('PEXPIRE', k, leaseMs + marginMs)
    return {'TOMBSTONED', ''}
end
local h = redis.call('HGETALL', k)
local m = {}
for i = 1, #h, 2 do m[h[i]] = h[i + 1] end
local tver = tonumber(m['tver'] or m['ver'] or -1)
if tver >= newVer then return {'NOOP', ''} end
redis.call('HSET', k, 'tver', tostring(newVer))
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
if m['state'] == 'F' then
    redis.call('HSET', k, 'state', 'S', 'sdl', now + staleMs)
    redis.call('PEXPIRE', k, staleMs + 1000)
end
redis.call('HDEL', k, 'tok', 'lexp')
return {'VOIDED', ''}
