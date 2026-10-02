local slots_key = KEYS[1]
local waiters_stream = KEYS[2]
local stream_key = KEYS[3]
local queue_key = KEYS[4]
local task_key = ARGV[1]
local max_concurrent = tonumber(ARGV[2])
local stale_threshold = tonumber(ARGV[3])
local runs_prefix = ARGV[4]
local state_prefix = ARGV[5]
local parked_prefix = ARGV[6]

-- Release this task's slot and, if waiters are parked, hand the freed
-- capacity off by re-injecting the oldest waiter(s) into the main
-- stream.  Stale peer slots are scavenged opportunistically, but only
-- when waiters exist -- we don't want to prematurely evict slots held
-- by briefly-paused live workers.
--
-- Waiters whose runs hash has flipped to ``cancelled`` are XDEL'd
-- without being forwarded.  This is the correctness backstop for the
-- cancel-races-with-wake case where the dependency's cancel subscriber
-- might not have pulled the waiter entry off the stream in time (Redis
-- pub/sub is fire-and-forget); cancelled tasks must never run.

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

redis.call('ZREM', slots_key, task_key)

local waiters_count = redis.call('XLEN', waiters_stream)
if waiters_count > 0 then
    local stale = redis.call('ZRANGEBYSCORE', slots_key, 0, stale_threshold)
    for _, s in ipairs(stale) do
        redis.call('ZREM', slots_key, s)
    end
end

local capacity = max_concurrent - redis.call('ZCARD', slots_key)
if capacity > 0 then
    local entries = redis.call('XRANGE', waiters_stream, '-', '+', 'COUNT', capacity)
    for _, entry in ipairs(entries) do
        local waiter_id = entry[1]
        local fields = entry[2]

        -- Pull out the waiter's task_key so we can address its runs hash and
        -- its state-change pubsub channel.
        local waiter_task_key
        for i = 1, #fields, 2 do
            if fields[i] == 'key' then
                waiter_task_key = fields[i + 1]
                break
            end
        end

        local runs_key = runs_prefix .. waiter_task_key

        -- Forward only if the task is still in the parked state.  Anything
        -- else -- 'cancelled', a missing runs hash (DELed by a cancel with
        -- execution_ttl=0), or any other terminal state -- means the task
        -- has been superseded and must not be revived.  Drop the waiter
        -- entry without forwarding.
        local current_state = redis.call('HGET', runs_key, 'state')
        local safeguard_key = '__safeguard__:' .. waiter_task_key
        if current_state ~= 'scheduled' then
            redis.call('XDEL', waiters_stream, waiter_id)
            -- Cancelled/superseded waiter: drop its safeguard backstop too.
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
            -- It's no longer needed; leaving it in the future queue would
            -- keep the worker awake for one redelivery_timeout for nothing.
            redis.call('ZREM', queue_key, safeguard_key)
            redis.call('DEL', parked_prefix .. safeguard_key)
            redis.call('DEL', runs_prefix .. safeguard_key)

            local payload = '{"type":"state","key":"' .. json_escape(waiter_task_key) .. '","state":"queued"}'
            redis.call('PUBLISH', state_prefix .. waiter_task_key, payload)
        end
    end
end

if redis.call('ZCARD', slots_key) == 0 then
    redis.call('DEL', slots_key)
end
if redis.call('XLEN', waiters_stream) == 0 then
    redis.call('DEL', waiters_stream)
end
