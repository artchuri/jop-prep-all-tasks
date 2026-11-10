-- FILL: only the lease owner may write; rejects older versions (I-1).
-- Only place that (re)sets the record TTL (I-5). ARGV: token, version, bal, st, ttlMs.
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
