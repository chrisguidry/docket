local ratelimit_key = KEYS[1]
local member = ARGV[1]
local now_ms = tonumber(ARGV[2])
local window_ms = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])

-- Atomic sliding-window rate-limit check.  Returns
-- ``[action, retry_after_ms]`` where action is PROCEED (1) or
-- BLOCKED (2).

-- Prune entries older than the window
local cutoff = now_ms - window_ms
redis.call('ZREMRANGEBYSCORE', ratelimit_key, '-inf', cutoff)

-- Count remaining entries
local count = redis.call('ZCARD', ratelimit_key)

if count < limit then
    -- Under limit: record this execution and set safety TTL
    redis.call('ZADD', ratelimit_key, now_ms, member)
    redis.call('PEXPIRE', ratelimit_key, ttl_ms)
    return {1, 0}
end

-- Over limit: compute when the oldest entry will expire
local oldest = redis.call('ZRANGE', ratelimit_key, 0, 0, 'WITHSCORES')
local oldest_score = tonumber(oldest[2])
local retry_after = oldest_score + window_ms - now_ms
if retry_after < 1 then
    retry_after = 1
end
return {2, retry_after}
