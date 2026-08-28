"""The Lua that moves one task key between lifecycle states.

Every transition in a task's life is one atomic script against the keys for
a single task: ``_schedule`` puts it on the stream or the queue, ``_claim``
takes it for a worker, ``_terminal`` records how it finished, and
``_cancel_task`` takes it back off.  The first three each check the
``generation`` counter in the runs hash, so a stale attempt can never
overwrite a newer one.

``Execution`` and ``Docket`` call these; the scripts hold no Python logic of
their own.
"""

from typing import Any

from ._lua import Arg, Args, Key, redis_script
from ._redis import RedisClient


@redis_script
async def _schedule(
    redis: RedisClient,
    *,
    stream_key: Key[str],
    known_key: Key[str],
    parked_key: Key[str],
    queue_key: Key[str],
    stream_id_key: Key[str],
    runs_key: Key[str],
    state_channel: Key[str],
    task_key: Arg[str],
    when_timestamp: Arg[float],
    is_immediate: Arg[bool],
    replace: Arg[bool],
    reschedule_message_id: Arg[bytes],
    expected_generation: Arg[int],
    worker_group_name: Arg[str],
    state_payload: Arg[str],
    message: Args[dict[bytes, bytes]],
) -> bytes | str:
    """
    -- TODO: Remove known_key / parked_key / queue_key / stream_id_key
    -- handling in v0.14.0 (legacy key locations).

    -- A caller that is rescheduling on behalf of the attempt it just ran
    -- (Perpetual's on_complete) passes the generation it holds, so the
    -- supersession check rides along with the schedule instead of costing
    -- its own HGET.  A newer stored generation means someone else has taken
    -- the key and this schedule must not touch it.  0 means "no check", both
    -- for callers that don't have a generation and for messages that predate
    -- generation tracking.
    if expected_generation > 0 then
        local stored = redis.call('HGET', runs_key, 'generation')
        local stored_generation = 0
        if stored then
            stored_generation = tonumber(stored)
        end
        if stored_generation > expected_generation then
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

    -- Increment generation counter
    local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
    if generation_index then
        message[generation_index] = tostring(new_gen)
    end

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
    """
    ...


@redis_script
async def _claim(
    redis: RedisClient,
    *,
    runs_key: Key[str],
    progress_key: Key[str],
    known_key: Key[str],
    stream_id_key: Key[str],
    state_channel: Key[str],
    stream_key: Key[str],
    worker: Arg[str],
    started_at: Arg[str],
    generation: Arg[int],
    state_payload: Arg[str],
    worker_group_name: Arg[str],
    message_id: Arg[bytes],
) -> list[Any]:
    """
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
    """
    ...


@redis_script
async def _terminal(
    redis: RedisClient,
    *,
    runs_key: Key[str],
    state_channel: Key[str],
    progress_key: Key[str],
    stream_key: Key[str],
    generation: Arg[int],
    state: Arg[str],
    completed_at: Arg[str],
    ttl_seconds: Arg[int],
    state_payload: Arg[str],
    worker_group_name: Arg[str],
    message_id: Arg[bytes],
    extra_fields: Args[list[str]],
) -> bytes:
    """
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
    if generation > 0 then
        local current = redis.call('HGET', runs_key, 'generation')
        if not current or tonumber(current) > generation then
            redis.call('PUBLISH', state_channel, state_payload)
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

    redis.call('PUBLISH', state_channel, state_payload)
    redis.call('DEL', progress_key)
    if message_id ~= '' then
        redis.call('XACK', stream_key, worker_group_name, message_id)
        redis.call('XDEL', stream_key, message_id)
    end

    return 'OK'
    """
    ...


@redis_script
async def _cancel_task(
    redis: RedisClient,
    *,
    stream_key: Key[str],
    known_key: Key[str],
    parked_key: Key[str],
    queue_key: Key[str],
    stream_id_key: Key[str],
    runs_key: Key[str],
    progress_key: Key[str],
    task_key: Arg[str],
    completed_at: Arg[str],
) -> bytes:
    """
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

    return 'OK'
    """
    ...
