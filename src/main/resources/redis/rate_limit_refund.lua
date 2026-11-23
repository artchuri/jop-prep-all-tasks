local n = tonumber(redis.call('GET', KEYS[1]) or '0')
if n <= 0 then
    return 0
end
return redis.call('DECR', KEYS[1])

