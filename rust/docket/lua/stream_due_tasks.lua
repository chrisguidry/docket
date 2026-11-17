local queue_key = KEYS[1]
local stream_key = KEYS[2]
local now_timestamp = tonumber(ARGV[1])
local docket_prefix = ARGV[2]
local batch = tonumber(ARGV[3])
local completed_at = ARGV[4]
local ttl_seconds = tonumber(ARGV[5])

-- Each call moves at most `batch` due tasks, and the caller calls again
-- until one scans less than that.  A script that moved a large backlog at
-- once would block every other client for as long as it ran.  `batch` must
-- stay below Lua's limit on unpack(), about 8000 values.

-- Inline JSON-string escaper for the common cases (`\`, `"`, and the
-- three named whitespace controls).  Task keys are user-supplied: if a
-- caller passes a key containing other control characters (NUL, BEL,
-- VT, FF, ESC, etc.) the published payload will not parse as strict
-- JSON.  GIGO -- callers should give us readable keys.
local function json_escape(s)
    s = s:gsub('\\', '\\\\')
    s = s:gsub('"', '\\"')
    s = s:gsub('\n', '\\n')
    s = s:gsub('\r', '\\r')
    s = s:gsub('\t', '\\t')
    return s
end

local total_work = redis.call('ZCARD', queue_key)
local due_work = 0
local scanned = 0

if total_work > 0 then
    local tasks = redis.call('ZRANGEBYSCORE', queue_key, 0, now_timestamp, 'LIMIT', 0, batch)
    scanned = #tasks

    for i, key in ipairs(tasks) do
        local hash_key = docket_prefix .. ":" .. key
        local task_data = redis.call('HGETALL', hash_key)

        if #task_data > 0 then
            local task = {}
            for j = 1, #task_data, 2 do
                task[task_data[j]] = task_data[j+1]
            end

            redis.call('XADD', stream_key, '*',
                'key', task['key'],
                'when', task['when'],
                'function', task['function'],
                'args', task['args'],
                'kwargs', task['kwargs'],
                'attempt', task['attempt'],
                'generation', task['generation'] or '0'
            )
            redis.call('DEL', hash_key)

            -- Set run state to queued
            local run_key = docket_prefix .. ":runs:" .. task['key']
            redis.call('HSET', run_key, 'state', 'queued')

            -- Publish state change event to pub/sub
            local channel = docket_prefix .. ":state:" .. task['key']
            local payload = '{"type":"state","key":"' .. json_escape(task['key']) .. '","state":"queued","when":"' .. task['when'] .. '"}'
            redis.call('PUBLISH', channel, payload)

            due_work = due_work + 1
        else
            -- The parked data is gone (evicted, or deleted by hand), so
            -- this run can never start.  Left as scheduled, it would keep
            -- its key reserved, and a caller waiting on its result would
            -- wait forever, so it fails with an error that says why.  Only
            -- a scheduled run waits on parked data: a run in any other
            -- state has a stale queue entry, and stays as it is.
            local run_key = docket_prefix .. ":runs:" .. key
            if redis.call('HGET', run_key, 'state') == 'scheduled' then
                local error = 'The parked data of this scheduled task was missing when it came due'
                redis.call('HDEL', run_key, 'known', 'stream_id')
                redis.call('HSET', run_key,
                    'state', 'failed', 'completed_at', completed_at, 'error', error)
                redis.call('DEL', docket_prefix .. ":progress:" .. key)
                redis.call('PUBLISH', docket_prefix .. ":state:" .. key,
                    '{"type":"state","key":"' .. json_escape(key) ..
                    '","state":"failed","completed_at":"' .. completed_at ..
                    '","error":"' .. error .. '"}')
                if ttl_seconds > 0 then
                    redis.call('EXPIRE', run_key, ttl_seconds)
                else
                    redis.call('DEL', run_key)
                end
            end
        end
    end

    -- Remove exactly the scanned entries.  A score range would also remove
    -- due entries past this batch, whose parked data was never streamed.
    -- Entries without parked data go too, so they cannot fill every batch.
    if scanned > 0 then
        redis.call('ZREM', queue_key, unpack(tasks))
    end
end

return {total_work, due_work, scanned}
