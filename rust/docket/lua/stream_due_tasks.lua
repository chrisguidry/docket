local queue_key = KEYS[1]
local stream_key = KEYS[2]
local now_timestamp = tonumber(ARGV[1])
local docket_prefix = ARGV[2]

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

if total_work > 0 then
    local tasks = redis.call('ZRANGEBYSCORE', queue_key, 0, now_timestamp)

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
        end
    end
end

if due_work > 0 then
    redis.call('ZREMRANGEBYSCORE', queue_key, 0, now_timestamp)
end

return {total_work, due_work}
