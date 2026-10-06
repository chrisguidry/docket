local slots_key = KEYS[1]
local waiters_stream = KEYS[2]
local stream_key = KEYS[3]
local runs_key = KEYS[4]
local max_concurrent = tonumber(ARGV[1])
local task_key = ARGV[2]
local current_time = tonumber(ARGV[3])
local is_redelivery = ARGV[4] == '1'
local stale_threshold = tonumber(ARGV[5])
local key_ttl = tonumber(ARGV[6])
local message_id = ARGV[7]
local worker_group_name = ARGV[8]
local state_channel = ARGV[9]
local state_payload = ARGV[10]
local message_start = 11

-- Acquire a concurrency slot, or park the task on the waiter stream
-- atomically.  Returns 1 if acquired (task should run), 0 if parked
-- (the inflight stream message has been XACK+XDEL'd and re-XADD'd
-- into the waiter stream; caller raises ConcurrencyBlocked(handled=True)
-- so the worker takes no further action).

-- If this task already has a slot (previous delivery attempt), only a
-- redelivery with a stale original holder can take it over.  Otherwise we
-- must not run a second time alongside a still-live peer.
local slot_time = redis.call('ZSCORE', slots_key, task_key)
if slot_time then
    slot_time = tonumber(slot_time)
    if is_redelivery and slot_time <= stale_threshold then
        redis.call('ZADD', slots_key, current_time, task_key)
        redis.call('EXPIRE', slots_key, key_ttl)
        return 1
    end
else
    if redis.call('ZCARD', slots_key) < max_concurrent then
        redis.call('ZADD', slots_key, current_time, task_key)
        redis.call('EXPIRE', slots_key, key_ttl)
        return 1
    end

    -- All slots full.  Scavenge any that have gone stale (holder is dead).
    local stale_slots = redis.call('ZRANGEBYSCORE', slots_key, 0, stale_threshold, 'LIMIT', 0, 1)
    if #stale_slots > 0 then
        redis.call('ZREM', slots_key, stale_slots[1])
        redis.call('ZADD', slots_key, current_time, task_key)
        redis.call('EXPIRE', slots_key, key_ttl)
        return 1
    end
end

-- Park: ACK the main-stream message, re-XADD the payload into the waiter
-- stream.  Doing this here, atomically with the acquire check, keeps a
-- concurrent slot release from missing us in the gap between "blocked" and
-- "parked".
redis.call('XACK', stream_key, worker_group_name, message_id)
redis.call('XDEL', stream_key, message_id)

-- Bump generation so stale redeliveries of this task are superseded on wake,
-- and splice the new value into the message as we forward it.
local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
local message = {}
local function_name, args_data, kwargs_data
for i = message_start, #ARGV, 2 do
    local field_name = ARGV[i]
    local field_value = ARGV[i + 1]
    if field_name == 'generation' then
        field_value = tostring(new_gen)
    elseif field_name == 'function' then
        function_name = field_value
    elseif field_name == 'args' then
        args_data = field_value
    elseif field_name == 'kwargs' then
        kwargs_data = field_value
    end
    message[#message + 1] = field_name
    message[#message + 1] = field_value
end

local waiter_entry_id = redis.call('XADD', waiters_stream, '*', unpack(message))

-- Record the waiter's location so the dependency's cancel subscriber can
-- find and XDEL the waiter entry when the task transitions to 'cancelled'.
redis.call('HSET', runs_key,
    'state', 'scheduled',
    'waiter_stream', waiters_stream,
    'waiter_entry_id', waiter_entry_id,
    'function', function_name,
    'args', args_data,
    'kwargs', kwargs_data
)
-- The claim before admission wrote a worker and a start time, and a parked
-- task has neither until a worker claims it again.
redis.call('HDEL', runs_key, 'stream_id', 'worker', 'started_at')

redis.call('PUBLISH', state_channel, state_payload)

return 0
