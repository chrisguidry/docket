# The docket protocol

Every docket implementation runs these Lua scripts, so every state change of
a task and every admission check behaves the same in each language.

Each script starts with a header that binds `KEYS` and `ARGV` to local names,
then a blank line, then the body:

```lua
local runs_key = KEYS[1]
local worker = ARGV[1]
local generation = tonumber(ARGV[2])
local wake = ARGV[3] == '1'
local fields_start = 4
```

The header is the calling contract.  A caller passes the keys and arguments in
the order of the header.  Numbers go over the wire as decimal strings and
booleans as `"1"` or `"0"`, and a variadic argument spreads into the slots
from `<name>_start` to the end of `ARGV`.  Each language checks its own
declaration of a script against this header.

These files are the source.  Each package keeps a checked-in copy, because
every registry packages only its own directory, and a prek hook fails when a
copy differs from the source.  Edit the scripts here.
