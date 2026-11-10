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
