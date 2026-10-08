local stream_key = KEYS[1]
local queue_key = KEYS[2]
local docket_prefix = ARGV[1]
local completed_at = ARGV[2]
local ttl_seconds = tonumber(ARGV[3])
local batch = tonumber(ARGV[4])

-- Clearing a docket cancels every task that has not started, the way
-- cancel_task.lua cancels one, so that each cleared key is free for a new
-- add() at once and a caller waiting on its result receives the cancelled
-- state.  Each call clears at most `batch` queue entries and `batch` stream
-- messages, and the caller calls again until one clears less than that.  A
-- script that cleared a large backlog at once would block every other
-- client for as long as it ran.  Each call reads its tasks and removes them, so a task
-- added in between cannot lose its message and keep its key reserved.  A
-- worker that read a queued task's message before the clear, but has not
-- claimed it, finds the task cancelled, and claim.lua refuses it.
--
-- The stream also holds the messages of tasks that are running.  Their
-- messages go too, but their runs hashes stay as they are, because each
-- one's worker records how it ends.

-- Task keys are user-supplied, and JSON allows no control character in a
-- string, so this escapes every one of them.  cjson is not available on the
-- in-memory backend.
local function json_escape(s)
    s = s:gsub('[\\"]', '\\%0')
    s = s:gsub('%c', function(c)
        return string.format('\\u%04x', c:byte())
    end)
    return s
end

local task_keys = {}
local seen = {}
local function remember(task_key)
    if not seen[task_key] then
        seen[task_key] = true
        task_keys[#task_keys + 1] = task_key
    end
end

-- Only a scheduled task has parked data, at the docket's prefix and its
-- key.  A key from the stream gets no DEL there: for an immediate task with
-- a key like "stream", that DEL would remove the stream itself.
local scheduled = {}
local queued = redis.call('ZRANGE', queue_key, 0, batch - 1)
for _, task_key in ipairs(queued) do
    scheduled[task_key] = true
    remember(task_key)
end
if #queued > 0 then
    redis.call('ZREM', queue_key, unpack(queued))
end

-- XDEL each message, not DEL the stream, so the stream keeps its consumer
-- group.  The in-memory backend does not run XTRIM inside Lua.
local messages = redis.call('XRANGE', stream_key, '-', '+', 'COUNT', batch)
for _, entry in ipairs(messages) do
    local fields = entry[2]
    for i = 1, #fields, 2 do
        if fields[i] == 'key' then
            remember(fields[i + 1])
            break
        end
    end
    redis.call('XDEL', stream_key, entry[1])
end

local prefix = docket_prefix .. ':'
for _, task_key in ipairs(task_keys) do
    if scheduled[task_key] then
        redis.call('DEL', prefix .. task_key)
    end
    -- TODO: Remove in v0.14.0 (legacy key locations).
    redis.call('DEL', prefix .. 'known:' .. task_key, prefix .. 'stream-id:' .. task_key)

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

return #messages + #queued
