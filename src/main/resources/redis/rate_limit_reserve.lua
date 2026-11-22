local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

local curr = tonumber(redis.call('GET', KEYS[1]) or '0') + 1
if curr == 1 then
    redis.call('SET', KEYS[1], curr, 'PX', window * 2 + 1000)
else
    redis.call('INCR', KEYS[1])
end

local prev = tonumber(redis.call('GET', KEYS[2]) or '0')
local elapsed = now % window
local weight = 1 - elapsed / window
local used = prev * weight + curr

if used <= limit then
    return {'1', '0', tostring(math.floor(used * 100))}
end

redis.call('DECR', KEYS[1])

local remainder = window - elapsed
local retry_after = remainder
if prev > 0 then
    local needed = math.ceil((used - limit) / (prev / window))
    if needed < retry_after then retry_after = needed end
end
if retry_after < 1 then retry_after = 1 end
return {'0', tostring(math.floor(retry_after)), tostring(math.floor((used - 1) * 100))}

