local progress_key = KEYS[1]
local payload = ARGV[1]
local clear_message = ARGV[2] == '1'
local fields_start = 3

local hset_args = {}
for i = fields_start, #ARGV, 2 do
    hset_args[#hset_args + 1] = ARGV[i]
    hset_args[#hset_args + 1] = ARGV[i + 1]
end
if #hset_args > 0 then
    redis.call('HSET', progress_key, unpack(hset_args))
end
if clear_message then
    redis.call('HDEL', progress_key, 'message')
end
redis.call('PUBLISH', progress_key, payload)
return 'OK'
