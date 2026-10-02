local key = KEYS[1]
local leading = ARGV[1]
local also_leading = ARGV[2]
local items_start = 3

local pieces = {}
for i = items_start, #ARGV do
    pieces[#pieces + 1] = ARGV[i]
end
return table.concat(pieces, '|')
