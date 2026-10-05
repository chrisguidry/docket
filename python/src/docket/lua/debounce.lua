local winner_key = KEYS[1]
local seen_key = KEYS[2]
local execution_key = ARGV[1]
local settle_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

-- Atomic debounce decision.  Returns ``[action, remaining_ms]`` where
-- action is one of PROCEED/RESCHEDULE/DROP (see constants above).

local winner = redis.call('GET', winner_key)

if not winner then
    -- No winner: I become winner, record last_seen = now
    redis.call('SET', winner_key, execution_key, 'PX', ttl_ms)
    redis.call('SET', seen_key, tostring(now_ms), 'PX', ttl_ms)
    return {2, settle_ms}
end

if winner == execution_key then
    -- I'm the winner, returning from reschedule
    local last_seen_str = redis.call('GET', seen_key)
    local last_seen = tonumber(last_seen_str) or 0
    local elapsed = now_ms - last_seen

    if elapsed >= settle_ms then
        -- Settled: clean up and proceed
        redis.call('DEL', winner_key, seen_key)
        return {1, 0}
    else
        -- Not settled yet: refresh TTLs and reschedule for remaining time
        local remaining = settle_ms - elapsed
        redis.call('PEXPIRE', winner_key, ttl_ms)
        redis.call('PEXPIRE', seen_key, ttl_ms)
        return {2, remaining}
    end
end

-- Someone else is the winner: update last_seen and refresh TTLs
redis.call('SET', seen_key, tostring(now_ms), 'PX', ttl_ms)
redis.call('PEXPIRE', winner_key, ttl_ms)
return {3, 0}
