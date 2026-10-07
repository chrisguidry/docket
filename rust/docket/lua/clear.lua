local stream_key = KEYS[1]
local queue_key = KEYS[2]
local docket_prefix = ARGV[1]
local completed_at = ARGV[2]
local ttl_seconds = tonumber(ARGV[3])

-- Clearing a docket cancels every task that has not started, the way
-- cancel_task.lua cancels one, so that each cleared key is free for a new
-- add() at once and a caller waiting on its result receives the cancelled
-- state.  One script reads the tasks and removes them, so a task added in
-- between cannot lose its message and keep its key reserved.  A worker that
-- read a queued task's message before the clear, but has not claimed it,
-- finds the task cancelled, and claim.lua refuses it.
--
-- The stream also holds the messages of tasks that are running.  Their
-- messages go too, but their runs hashes stay as they are, because each
-- one's worker records how it ends.

-- Inline JSON-string escaper for the common cases, as in
-- stream_due_tasks.lua.  Task keys are user-supplied.
local function json_escape(s)
    s = s:gsub('\\', '\\\\')
    s = s:gsub('"', '\\"')
    s = s:gsub('\n', '\\n')
    s = s:gsub('\r', '\\r')
    s = s:gsub('\t', '\\t')
    return s
end

local stream_count = redis.call('XLEN', stream_key)
local queue_count = redis.call('ZCARD', queue_key)

local task_keys = {}
local seen = {}
local function remember(task_key)
    if not seen[task_key] then
        seen[task_key] = true
        task_keys[#task_keys + 1] = task_key
    end
end

for _, task_key in ipairs(redis.call('ZRANGE', queue_key, 0, -1)) do
    remember(task_key)
end

-- XDEL each message, not DEL the stream, so the stream keeps its consumer
-- group.  The in-memory backend does not run XTRIM inside Lua.
for _, entry in ipairs(redis.call('XRANGE', stream_key, '-', '+')) do
    local fields = entry[2]
    for i = 1, #fields, 2 do
        if fields[i] == 'key' then
            remember(fields[i + 1])
            break
        end
    end
    redis.call('XDEL', stream_key, entry[1])
end
redis.call('DEL', queue_key)

local prefix = docket_prefix .. ':'
for _, task_key in ipairs(task_keys) do
    -- TODO: Remove known: and stream-id: in v0.14.0 (legacy key locations).
    redis.call('DEL',
        prefix .. task_key,
        prefix .. 'known:' .. task_key,
        prefix .. 'stream-id:' .. task_key
    )

    local runs_key = prefix .. 'runs:' .. task_key
    local state = redis.call('HGET', runs_key, 'state')
    if state == 'scheduled' or state == 'queued' then
        redis.call('HDEL', runs_key, 'known', 'stream_id')
        redis.call('HSET', runs_key, 'state', 'cancelled', 'completed_at', completed_at)
        redis.call('DEL', prefix .. 'progress:' .. task_key)
        redis.call('PUBLISH', prefix .. 'state:' .. task_key,
            '{"type":"state","key":"' .. json_escape(task_key) ..
            '","state":"cancelled","completed_at":"' .. completed_at .. '"}')
        if ttl_seconds > 0 then
            redis.call('EXPIRE', runs_key, ttl_seconds)
        else
            redis.call('DEL', runs_key)
        end
    end
end

return stream_count + queue_count
