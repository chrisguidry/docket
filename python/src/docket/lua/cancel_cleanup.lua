local waiters_stream = KEYS[1]
local progress_key = KEYS[2]
local runs_key = KEYS[3]
local waiter_entry_id = ARGV[1]

-- Atomically tear down a cancelled task's waiter footprint.  Invoked
-- by ConcurrencyLimit's pubsub-driven cancel subscriber after
-- Docket.cancel flips the task to 'cancelled'.

redis.call('XDEL', waiters_stream, waiter_entry_id)
if redis.call('XLEN', waiters_stream) == 0 then
    redis.call('DEL', waiters_stream)
end

-- claim() creates a per-task progress hash before the concurrency gate
-- runs, so a task that's cancelled while parked leaks one of these
-- without an explicit DEL.
redis.call('DEL', progress_key)

redis.call('HDEL', runs_key, 'waiter_stream', 'waiter_entry_id')
