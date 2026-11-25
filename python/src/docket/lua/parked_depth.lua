local waiter_index = KEYS[1]
local parked_total = KEYS[2]
local cursor = tonumber(ARGV[1])
local batch = tonumber(ARGV[2])

-- Returns how many tasks are parked on the docket's waiter streams, for a
-- worker's heartbeat.  The concurrency scripts keep the total.  A worker on
-- a release that keeps no count can change a stream and leave its count
-- wrong, so each call also compares `batch` of the indexed counts with their
-- streams, starting at `cursor`, and corrects the total.  A caller that adds
-- `batch` to `cursor` on each call reaches every count in turn.  The total
-- is the sum of the index's counts, so an empty index means a total of zero,
-- whatever the total key holds.

local total = tonumber(redis.call('GET', parked_total) or 0)
local corrected = total

local indexed = redis.call('ZCARD', waiter_index)
if indexed == 0 then
    corrected = 0
else
    -- The batch wraps past the end of the index to its start, so an index
    -- no longer than a batch is compared whole.
    local first = cursor % indexed
    local last = first + batch - 1
    local streams = redis.call('ZRANGE', waiter_index, first, last)
    if last >= indexed and first > 0 then
        local wrapped = redis.call('ZRANGE', waiter_index, 0, math.min(last - indexed, first - 1))
        for _, stream in ipairs(wrapped) do
            streams[#streams + 1] = stream
        end
    end
    for _, stream in ipairs(streams) do
        local now = redis.call('XLEN', stream)
        local before = tonumber(redis.call('ZSCORE', waiter_index, stream))
        if now == 0 then
            redis.call('ZREM', waiter_index, stream)
        elseif now ~= before then
            redis.call('ZADD', waiter_index, tostring(now), stream)
        end
        corrected = corrected + now - before
    end
end

if corrected ~= total then
    if corrected > 0 then
        redis.call('SET', parked_total, tostring(corrected))
    else
        redis.call('DEL', parked_total)
    end
end

return math.max(corrected, 0)
