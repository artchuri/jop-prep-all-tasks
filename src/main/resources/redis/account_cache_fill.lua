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
-- Tombstone gate: pre-invalidation data must never land, even when the
-- displayed version field is absent (fresh key created before the commit).
local tver = tonumber(m['tver'] or m['ver'] or -1)
if loadedVer < tver then return {'REJECTED', 'older_version'} end
redis.call('HSET', k, 'bal', bal, 'st', st, 'ver', tostring(loadedVer), 'state', 'F')
if loadedVer >= tver then redis.call('HSET', k, 'tver', tostring(loadedVer)) end
redis.call('HDEL', k, 'tok', 'lexp', 'sdl')
redis.call('PEXPIRE', k, ttlMs)
return {'OK', ''}
