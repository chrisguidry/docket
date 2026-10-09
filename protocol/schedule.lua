local stream_key = KEYS[1]
local known_key = KEYS[2]
local parked_key = KEYS[3]
local queue_key = KEYS[4]
local stream_id_key = KEYS[5]
local runs_key = KEYS[6]
local state_channel = KEYS[7]
local task_key = ARGV[1]
local when_timestamp = tonumber(ARGV[2])
local is_immediate = ARGV[3] == '1'
local replace = ARGV[4] == '1'
local reschedule_message_id = ARGV[5]
local expected_generation = tonumber(ARGV[6])
local worker_group_name = ARGV[7]
local state_payload = ARGV[8]
local message_start = 9

-- TODO: Remove known_key / parked_key / queue_key / stream_id_key
-- handling in v0.14.0 (legacy key locations).

-- A caller that is rescheduling on behalf of the attempt it just ran
-- (Perpetual's on_complete, or a Retry) passes the generation it holds, so
-- the supersession check rides along with the schedule instead of costing
-- its own HGET.  A newer stored generation means someone else has taken
-- the key and this schedule must not touch it.  A missing runs hash does
-- not refuse the schedule: an eviction or an older clear() can remove the
-- hash of a run that is still going, and that run's retry or next
-- Perpetual run went ahead before this check existed.  0 means "no check",
-- both for callers that don't have a generation and for messages that
-- predate generation tracking.
if expected_generation > 0 then
    local stored = redis.call('HGET', runs_key, 'generation')
    if stored and tonumber(stored) > expected_generation then
        return 'SUPERSEDED'
    end
end

-- Extract message fields
local message = {}
local function_name = nil
local args_data = nil
local kwargs_data = nil
local generation_index = nil

for i = message_start, #ARGV, 2 do
    local field_name = ARGV[i]
    local field_value = ARGV[i + 1]
    message[#message + 1] = field_name
    message[#message + 1] = field_value

    -- Extract task data fields for runs hash
    if field_name == 'function' then
        function_name = field_value
    elseif field_name == 'args' then
        args_data = field_value
    elseif field_name == 'kwargs' then
        kwargs_data = field_value
    elseif field_name == 'generation' then
        generation_index = #message
    end
end

-- Handle rescheduling from stream: atomically ACK the original message and
-- re-route the task.  Prevents both task loss (ACK before reschedule) and
-- duplicate execution (reschedule before ACK with slow reschedule causing
-- redelivery).  Honors is_immediate so a retry with delay=0 lands in the
-- stream right away instead of waiting for the scheduler poll.  Sets
-- 'known' so a concurrent docket.add() for the same key dedups against
-- this rescheduled task.
if reschedule_message_id ~= '' then
    -- Acknowledge and delete the message from the stream
    redis.call('XACK', stream_key, worker_group_name, reschedule_message_id)
    redis.call('XDEL', stream_key, reschedule_message_id)

    -- Increment generation counter
    local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
    if generation_index then
        message[generation_index] = tostring(new_gen)
    end

    if is_immediate then
        -- Add directly to stream for immediate execution
        local new_message_id = redis.call('XADD', stream_key, '*', unpack(message))
        redis.call('HSET', runs_key,
            'state', 'queued',
            'when', when_timestamp,
            'known', when_timestamp,
            'stream_id', new_message_id,
            'function', function_name,
            'args', args_data,
            'kwargs', kwargs_data
        )
    else
        -- Park task data for future execution
        redis.call('HSET', parked_key, unpack(message))
        redis.call('ZADD', queue_key, when_timestamp, task_key)
        redis.call('HSET', runs_key,
            'state', 'scheduled',
            'when', when_timestamp,
            'known', when_timestamp,
            'function', function_name,
            'args', args_data,
            'kwargs', kwargs_data
        )
        redis.call('HDEL', runs_key, 'stream_id')
    end

    -- Clear fields written by the previous attempt's ``_claim`` so the
    -- runs hash describes the rescheduled (queued/scheduled) attempt,
    -- not the worker and start-time of the attempt that just failed.
    redis.call('HDEL', runs_key, 'worker', 'started_at')

    redis.call('PUBLISH', state_channel, state_payload)

    return 'OK'
end

-- Handle replacement: cancel existing task if needed
if replace then
    -- Get stream ID from runs hash (check new location first)
    local existing_message_id = redis.call('HGET', runs_key, 'stream_id')

    -- TODO: Remove in next breaking release (v0.14.0) - check legacy location
    if not existing_message_id then
        existing_message_id = redis.call('GET', stream_id_key)
    end

    if existing_message_id then
        redis.call('XDEL', stream_key, existing_message_id)
    end

    redis.call('ZREM', queue_key, task_key)
    redis.call('DEL', parked_key)

    -- TODO: Remove in next breaking release (v0.14.0) - clean up legacy keys
    redis.call('DEL', known_key, stream_id_key)

    -- Note: runs_key is updated below, not deleted
else
    -- Check if task already exists (check new location first, then legacy)
    local known_exists = redis.call('HEXISTS', runs_key, 'known') == 1
    if not known_exists then
        -- Check if task is currently running (known field deleted at claim time)
        local state = redis.call('HGET', runs_key, 'state')
        if state == 'running' then
            return 'EXISTS'
        end
        -- TODO: Remove in next breaking release (v0.14.0) - check legacy location
        known_exists = redis.call('EXISTS', known_key) == 1
    end
    if known_exists then
        return 'EXISTS'
    end
end

-- A key used again after its run ended: terminal.lua gave that run's
-- record an expiry of execution_ttl, which the new run would inherit, so
-- the record could expire before a worker claims the new run (it is then
-- refused as superseded) or while it runs (it then can't be seen, and the
-- key no longer refuses a duplicate).  The new run starts a record of its
-- own, keeping only the generation counter.
local previous_state = redis.call('HGET', runs_key, 'state')
if previous_state == 'completed' or previous_state == 'failed' or previous_state == 'cancelled' then
    local previous_generation = redis.call('HGET', runs_key, 'generation')
    redis.call('DEL', runs_key)
    if previous_generation then
        redis.call('HSET', runs_key, 'generation', previous_generation)
    end
end

-- Increment generation counter
local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
if generation_index then
    message[generation_index] = tostring(new_gen)
end

-- A new run starts without the previous run's ending, so a reused key
-- never shows the old worker, error, completion time, or result.
redis.call('HDEL', runs_key, 'worker', 'started_at', 'completed_at', 'error', 'result_key')

if is_immediate then
    -- Add to stream for immediate execution
    local message_id = redis.call('XADD', stream_key, '*', unpack(message))

    -- Store state and metadata in runs hash
    redis.call('HSET', runs_key,
        'state', 'queued',
        'when', when_timestamp,
        'known', when_timestamp,
        'stream_id', message_id,
        'function', function_name,
        'args', args_data,
        'kwargs', kwargs_data
    )
else
    -- Park task data for future execution
    redis.call('HSET', parked_key, unpack(message))

    -- Add to sorted set queue
    redis.call('ZADD', queue_key, when_timestamp, task_key)

    -- Store state and metadata in runs hash
    redis.call('HSET', runs_key,
        'state', 'scheduled',
        'when', when_timestamp,
        'known', when_timestamp,
        'function', function_name,
        'args', args_data,
        'kwargs', kwargs_data
    )
end

redis.call('PUBLISH', state_channel, state_payload)

return 'OK'
