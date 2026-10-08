local runs_key = KEYS[1]
local state_channel = KEYS[2]
local progress_key = KEYS[3]
local stream_key = KEYS[4]
local generation = tonumber(ARGV[1])
local state = ARGV[2]
local completed_at = ARGV[3]
local ttl_seconds = tonumber(ARGV[4])
local state_payload = ARGV[5]
local worker_group_name = ARGV[6]
local message_id = ARGV[7]
local extra_fields_start = 8

-- Check supersession (generation 0 = pre-tracking, always write).  Two
-- supersession shapes, both handled the same way:
--   * runs hash missing entirely -- a newer generation already completed
--     and its execution_ttl expired (or it was 0).
--   * runs hash present but its generation is newer -- a successor is in
--     flight or has just finished within its execution_ttl window.
-- In both cases we still publish the terminal-state event so subscribers
-- waiting on completion don't deadlock, and we still clean up this
-- execution's progress hash and stream entry.  We do NOT recreate or
-- mutate the runs hash on a supersession -- the successor owns it.
--
-- An empty state_payload publishes nothing.  A retry that a replace
-- superseded passes it, because the key's task has not ended, and a
-- caller waiting on the key must wait for the replacement.
if generation > 0 then
    local current = redis.call('HGET', runs_key, 'generation')
    if not current or tonumber(current) > generation then
        if state_payload ~= '' then
            redis.call('PUBLISH', state_channel, state_payload)
        end
        -- Only DEL the progress hash if it belongs to us (matching
        -- generation tag) or is untagged (pre-fix / pre-tracking data,
        -- preserve the prior unconditional-DEL behaviour).  A newer
        -- generation's tag means the successor is actively reporting
        -- against the hash and we must not clobber its state.
        local progress_gen = redis.call('HGET', progress_key, 'generation')
        if not progress_gen or tonumber(progress_gen) <= generation then
            redis.call('DEL', progress_key)
        end
        if message_id ~= '' then
            redis.call('XACK', stream_key, worker_group_name, message_id)
            redis.call('XDEL', stream_key, message_id)
        end
        return 'SUPERSEDED'
    end
end

-- Build HSET args: state + completed_at + any extras
local hset_args = {'state', state, 'completed_at', completed_at}
for i = extra_fields_start, #ARGV, 2 do
    hset_args[#hset_args + 1] = ARGV[i]
    hset_args[#hset_args + 1] = ARGV[i + 1]
end
redis.call('HSET', runs_key, unpack(hset_args))

if ttl_seconds > 0 then
    redis.call('EXPIRE', runs_key, ttl_seconds)
else
    redis.call('DEL', runs_key)
end

if state_payload ~= '' then
    redis.call('PUBLISH', state_channel, state_payload)
end
redis.call('DEL', progress_key)
if message_id ~= '' then
    redis.call('XACK', stream_key, worker_group_name, message_id)
    redis.call('XDEL', stream_key, message_id)
end

return 'OK'
