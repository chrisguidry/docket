local runs_key = KEYS[1]
local progress_key = KEYS[2]
local known_key = KEYS[3]
local stream_id_key = KEYS[4]
local state_channel = KEYS[5]
local stream_key = KEYS[6]
local worker = ARGV[1]
local started_at = ARGV[2]
local generation = tonumber(ARGV[3])
local state_payload = ARGV[4]
local key_json = ARGV[5]
local worker_group_name = ARGV[6]
local message_id = ARGV[7]

-- TODO: Remove known_key / stream_id_key handling in v0.14.0
-- (legacy key locations).

-- Every reply is {status, runs hash, progress hash}: the caller reads
-- the two hashes back from the claim instead of paying for its own
-- HGETALL pair a moment earlier.  On both paths the hashes are what
-- Redis holds once the script is done, so a refused claim reports the
-- key as its winner left it.

-- Check supersession: generation > 0 means tracking is active.  When the
-- claim is for a stale message we still ACK and XDEL it so the stream
-- entry doesn't linger -- nothing else will clean it up.
if generation > 0 then
    local current = redis.call('HGET', runs_key, 'generation')
    if not current or tonumber(current) > generation then
        -- Either the runs hash was cleaned up (execution_ttl=0 after a
        -- newer generation completed) or a newer generation holds it.
        if message_id ~= '' then
            redis.call('XACK', stream_key, worker_group_name, message_id)
            redis.call('XDEL', stream_key, message_id)
        end
        return {
            'SUPERSEDED',
            redis.call('HGETALL', runs_key),
            redis.call('HGETALL', progress_key)
        }
    end
end

-- A cancel does not change the generation.  It also cannot XDEL an entry
-- the scheduler moved from the queue, because the runs hash has no
-- stream_id for it.  So refuse a cancelled key here, and ACK and XDEL
-- its message.
if redis.call('HGET', runs_key, 'state') == 'cancelled' then
    if message_id ~= '' then
        redis.call('XACK', stream_key, worker_group_name, message_id)
        redis.call('XDEL', stream_key, message_id)
    end
    -- docket.cancel() leaves a running task's cancelled state to its
    -- worker.  If that worker died, the redelivery sweep reclaims its
    -- message for another worker, whose claim takes this branch.  So
    -- this claim publishes the cancelled state.  Without it, a waiter in
    -- get_result() waits until its timeout, or forever without one.  For
    -- a task that was queued at the cancel, docket.cancel() published
    -- the same event already, so a subscriber can receive it twice.
    --
    -- The event takes completed_at from the runs hash, so it matches what
    -- sync() reads.  cjson isn't available on the in-memory backend, so
    -- this builds the JSON with string concatenation.  Python passes the
    -- key already JSON-encoded, and an ISO timestamp needs no escaping.
    local completed_at = redis.call('HGET', runs_key, 'completed_at')
        or started_at
    redis.call('PUBLISH', state_channel,
        '{"type": "state", "key": ' .. key_json ..
        ', "state": "cancelled", "completed_at": "' .. completed_at .. '"}')
    return {
        'CANCELLED',
        redis.call('HGETALL', runs_key),
        redis.call('HGETALL', progress_key)
    }
end

-- Update execution state to running
redis.call('HSET', runs_key,
    'state', 'running',
    'worker', worker,
    'started_at', started_at
)

-- Initialize progress tracking, tagged with the claimer's generation so
-- a stale predecessor finishing later can tell whether the progress hash
-- is still ours to clean up (see _terminal SUPERSEDED branch).  Also
-- drop any ``message``/``updated_at`` left behind by the previous
-- generation -- HSET doesn't remove optional fields, so without this
-- HDEL the successor's progress view would surface stale metadata.
redis.call('HSET', progress_key,
    'current', '0',
    'total', '100',
    'generation', generation
)
redis.call('HDEL', progress_key, 'message', 'updated_at')

-- Delete known/stream_id fields to allow task rescheduling
redis.call('HDEL', runs_key, 'known', 'stream_id')

-- TODO: Remove in next breaking release (v0.14.0) - legacy key cleanup
redis.call('DEL', known_key, stream_id_key)

redis.call('PUBLISH', state_channel, state_payload)

return {
    'OK',
    redis.call('HGETALL', runs_key),
    redis.call('HGETALL', progress_key)
}
