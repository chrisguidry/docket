local lease_key = KEYS[1]
local holder = ARGV[1]
local duration_ms = tonumber(ARGV[2])

if redis.call('GET', lease_key) == holder then
    return redis.call('PEXPIRE', lease_key, duration_ms)
end
return 0
