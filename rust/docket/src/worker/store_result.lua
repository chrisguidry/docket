-- Stores a completed run's output, or removes an earlier run's output when
-- ARGV[2] is empty, but only while the run's generation is current.
local runs_key = KEYS[1]
local result_key = KEYS[2]
local generation = ARGV[1]
local output = ARGV[2]
local ttl_seconds = ARGV[3]

if redis.call('HGET', runs_key, 'generation') ~= generation then
    return
end
if output == '' then
    redis.call('DEL', result_key)
else
    redis.call('SET', result_key, output, 'EX', ttl_seconds)
end
