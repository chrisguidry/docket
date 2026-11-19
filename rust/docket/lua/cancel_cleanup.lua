local waiters_stream = KEYS[1]
local progress_key = KEYS[2]
local runs_key = KEYS[3]
local waiter_index = KEYS[4]
local parked_total = KEYS[5]
local waiter_entry_id = ARGV[1]

-- Records how many tasks are parked on this stream, and moves the docket's
-- total by the difference from the count recorded before.  A count that is
-- wrong for a stream is right again after any script runs on that stream.
local function record_parked()
    local now = redis.call('XLEN', waiters_stream)
    local before = tonumber(redis.call('ZSCORE', waiter_index, waiters_stream) or 0)
    if now == 0 then
        redis.call('DEL', waiters_stream)
        redis.call('ZREM', waiter_index, waiters_stream)
    else
        redis.call('ZADD', waiter_index, tostring(now), waiters_stream)
    end
    if now ~= before then
        local total = tonumber(redis.call('GET', parked_total) or 0) + now - before
        if total > 0 then
            redis.call('SET', parked_total, tostring(total))
        else
            redis.call('DEL', parked_total)
        end
    end
end

-- Atomically tear down a cancelled task's waiter footprint.  Invoked
-- by ConcurrencyLimit's pubsub-driven cancel subscriber after
-- Docket.cancel flips the task to 'cancelled'.

redis.call('XDEL', waiters_stream, waiter_entry_id)
record_parked()

-- claim() creates a per-task progress hash before the concurrency gate
-- runs, so a task that's cancelled while parked leaks one of these
-- without an explicit DEL.
redis.call('DEL', progress_key)

redis.call('HDEL', runs_key, 'waiter_stream', 'waiter_entry_id')
