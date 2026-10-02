local key = KEYS[1]
local count = tonumber(ARGV[1])
local ratio = tonumber(ARGV[2])
local flag = ARGV[3] == '1'

-- Arithmetic and boolean operations on the typed locals the header binds.
local bumped = count + 1
local scaled = ratio * 10
local picked = 0
if flag then picked = 1 end
return {bumped, scaled, picked}
