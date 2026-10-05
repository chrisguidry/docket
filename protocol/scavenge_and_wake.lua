local slots_key = KEYS[1]
local waiters_stream = KEYS[2]
local stream_key = KEYS[3]
local queue_key = KEYS[4]
local max_concurrent = tonumber(ARGV[1])
local stale_threshold = tonumber(ARGV[2])
local runs_prefix = ARGV[3]
local state_prefix = ARGV[4]
local parked_prefix = ARGV[5]

-- Scavenge any stale slot holders and hand freed capacity to parked
-- waiters.  Called by the worker's concurrency-sweep loop to recover
-- the degenerate case where every slot holder crashed without
-- releasing AND no new tasks are arriving to trigger the normal
-- acquire-path scavenge.  Structurally identical to
-- _release_and_wake's post-release body, minus the self-ZREM.
--
-- Returns the number of waiters woken (zero means either no waiters
-- were parked, or no capacity was free to give them).

-- Inline JSON-string escaper (see _release_and_wake for the rationale).
local function json_escape(s)
    s = s:gsub('\\', '\\\\')
    s = s:gsub('"', '\\"')
    s = s:gsub('\n', '\\n')
    s = s:gsub('\r', '\\r')
    s = s:gsub('\t', '\\t')
    return s
end

local waiters_count = redis.call('XLEN', waiters_stream)
if waiters_count == 0 then
    redis.call('DEL', waiters_stream)
    return 0
end

-- Evict any stale slot holders; a live peer would be heartbeating every
-- redelivery_timeout/4, so anything older than redelivery_timeout belongs
-- to a dead worker.
local stale = redis.call('ZRANGEBYSCORE', slots_key, 0, stale_threshold)
for _, s in ipairs(stale) do
    redis.call('ZREM', slots_key, s)
end

local capacity = max_concurrent - redis.call('ZCARD', slots_key)
if capacity == 0 then
    return 0
end

local woken = 0
local entries = redis.call('XRANGE', waiters_stream, '-', '+', 'COUNT', capacity)
for _, entry in ipairs(entries) do
    local waiter_id = entry[1]
    local fields = entry[2]

    local waiter_task_key
    for i = 1, #fields, 2 do
        if fields[i] == 'key' then
            waiter_task_key = fields[i + 1]
            break
        end
    end

    local runs_key = runs_prefix .. waiter_task_key

    -- Skip cancelled/superseded waiters: drop the entry without forwarding.
    local current_state = redis.call('HGET', runs_key, 'state')
    local safeguard_key = '__safeguard__:' .. waiter_task_key
    if current_state ~= 'scheduled' then
        redis.call('XDEL', waiters_stream, waiter_id)
        redis.call('ZREM', queue_key, safeguard_key)
        redis.call('DEL', parked_prefix .. safeguard_key)
        redis.call('DEL', runs_prefix .. safeguard_key)
    else
        local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
        for i = 1, #fields, 2 do
            if fields[i] == 'generation' then
                fields[i + 1] = tostring(new_gen)
            end
        end

        local main_id = redis.call('XADD', stream_key, '*', unpack(fields))
        redis.call('XDEL', waiters_stream, waiter_id)
        redis.call('HSET', runs_key, 'state', 'queued', 'stream_id', main_id)
        redis.call('HDEL', runs_key, 'waiter_stream', 'waiter_entry_id')

        -- Cancel the safeguard task this waiter scheduled on park.
        redis.call('ZREM', queue_key, safeguard_key)
        redis.call('DEL', parked_prefix .. safeguard_key)
        redis.call('DEL', runs_prefix .. safeguard_key)

        local payload = '{"type":"state","key":"' .. json_escape(waiter_task_key) .. '","state":"queued"}'
        redis.call('PUBLISH', state_prefix .. waiter_task_key, payload)
        woken = woken + 1
    end
end

if redis.call('ZCARD', slots_key) == 0 then
    redis.call('DEL', slots_key)
end
if redis.call('XLEN', waiters_stream) == 0 then
    redis.call('DEL', waiters_stream)
end

return woken
