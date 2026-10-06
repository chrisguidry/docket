local stream_key = KEYS[1]
local known_key = KEYS[2]
local parked_key = KEYS[3]
local queue_key = KEYS[4]
local stream_id_key = KEYS[5]
local runs_key = KEYS[6]
local progress_key = KEYS[7]
local state_channel = KEYS[8]
local task_key = ARGV[1]
local completed_at = ARGV[2]
local state_payload = ARGV[3]

-- TODO: Remove known_key / parked_key / stream_id_key handling in
-- v0.14.0 (legacy key locations).

-- Get stream ID (check new location first, then legacy)
local message_id = redis.call('HGET', runs_key, 'stream_id')

-- TODO: Remove in next breaking release (v0.14.0) - check legacy location
if not message_id then
    message_id = redis.call('GET', stream_id_key)
end

-- Delete from stream if message ID exists
if message_id then
    redis.call('XDEL', stream_key, message_id)
end

-- Clean up legacy keys and parked data
redis.call('DEL', known_key, parked_key, stream_id_key)
redis.call('ZREM', queue_key, task_key)

-- Drop the per-task progress hash that ``Execution.claim``
-- creates -- without a TTL of its own, it would otherwise
-- leak when a task is cancelled after being claimed but
-- before it completes (e.g. parked on a side channel).
redis.call('DEL', progress_key)

-- Clear scheduling markers so add() can reschedule this key
redis.call('HDEL', runs_key, 'known', 'stream_id')

-- Only set CANCELLED if not already in a terminal state
local current_state = redis.call('HGET', runs_key, 'state')
if current_state ~= 'completed' and current_state ~= 'failed' and current_state ~= 'cancelled' then
    redis.call('HSET', runs_key, 'state', 'cancelled', 'completed_at', completed_at)
end

-- No worker holds a task that has not started, so this script publishes
-- its cancelled state.  A running task's worker publishes the terminal
-- state itself, and Perpetual cancels its own key before the worker
-- marks the run completed.
if current_state == 'scheduled' or current_state == 'queued' then
    redis.call('PUBLISH', state_channel, state_payload)
end

return 'OK'
