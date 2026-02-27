"""Debounce (trailing-edge / settle) admission control dependency.

Waits for submissions to settle, then fires once.  Uses two Redis keys
(winner + last_seen) so only one task bounces while the rest immediately
drop.
"""

from __future__ import annotations

import time
from datetime import timedelta
from types import TracebackType
from typing import Any

from ._base import AdmissionBlocked, Dependency, current_docket, current_execution

# Lua script for atomic debounce logic.
#
# KEYS[1] = winner key    (holds the execution key of the chosen task)
# KEYS[2] = last_seen key (holds ms timestamp of most recent submission)
# ARGV[1] = my execution key
# ARGV[2] = settle window in milliseconds
# ARGV[3] = current time in milliseconds
# ARGV[4] = key TTL in milliseconds (settle * 10)
#
# Returns: {action, remaining_ms}
#   action: 1=PROCEED, 2=RESCHEDULE, 3=DROP
#   remaining_ms: ms until settle window expires (only for RESCHEDULE)
_DEBOUNCE_LUA = """
local winner_key  = KEYS[1]
local seen_key    = KEYS[2]
local my_key      = ARGV[1]
local settle_ms   = tonumber(ARGV[2])
local now_ms      = tonumber(ARGV[3])
local ttl_ms      = tonumber(ARGV[4])

local winner = redis.call('GET', winner_key)

if not winner then
    -- No winner: I become winner, record last_seen = now
    redis.call('SET', winner_key, my_key, 'PX', ttl_ms)
    redis.call('SET', seen_key, tostring(now_ms), 'PX', ttl_ms)
    return {2, settle_ms}
end

if winner == my_key then
    -- I'm the winner, returning from reschedule
    local last_seen_str = redis.call('GET', seen_key)
    local last_seen = tonumber(last_seen_str) or 0
    local elapsed = now_ms - last_seen

    if elapsed >= settle_ms then
        -- Settled: clean up and proceed
        redis.call('DEL', winner_key, seen_key)
        return {1, 0}
    else
        -- Not settled yet: refresh TTLs and reschedule for remaining time
        local remaining = settle_ms - elapsed
        redis.call('PEXPIRE', winner_key, ttl_ms)
        redis.call('PEXPIRE', seen_key, ttl_ms)
        return {2, remaining}
    end
end

-- Someone else is the winner: update last_seen and refresh TTLs
redis.call('SET', seen_key, tostring(now_ms), 'PX', ttl_ms)
redis.call('PEXPIRE', winner_key, ttl_ms)
return {3, 0}
"""

_ACTION_PROCEED = 1
_ACTION_RESCHEDULE = 2
_ACTION_DROP = 3


class Debounce(Dependency["Debounce"]):
    """Wait for submissions to settle, then fire once.

    Uses two Redis keys per scope — a "winner" key (which execution gets
    to proceed) and a "last_seen" timestamp.  Only the winner bounces
    via reschedule; all other submissions are immediately dropped.

    Works both as a default parameter and as ``Annotated`` metadata::

        # Per-task: wait for 5s of quiet, then execute once
        async def process_webhooks(
            debounce: Debounce = Debounce(timedelta(seconds=5)),
        ) -> None: ...

        # Per-parameter: independent settle window per customer
        async def sync_customer(
            customer_id: Annotated[int, Debounce(timedelta(seconds=5))],
        ) -> None: ...
    """

    single: bool = True

    def __init__(self, settle: timedelta, *, scope: str | None = None) -> None:
        self.settle = settle
        self.scope = scope
        self._argument_name: str | None = None
        self._argument_value: Any = None

    def bind_to_parameter(self, name: str, value: Any) -> Debounce:
        bound = Debounce(self.settle, scope=self.scope)
        bound._argument_name = name
        bound._argument_value = value
        return bound

    async def __aenter__(self) -> Debounce:
        execution = current_execution.get()
        docket = current_docket.get()

        scope = self.scope or docket.name
        if self._argument_name is not None:
            hash_tag = f"{self._argument_name}:{self._argument_value}"
            base_key = f"{scope}:debounce:{hash_tag}"
        else:
            hash_tag = execution.function_name
            base_key = f"{scope}:debounce:{hash_tag}"

        # Use a Redis hash tag {…} so both keys land on the same cluster slot
        winner_key = f"{base_key}:{{{hash_tag}}}:winner"
        seen_key = f"{base_key}:{{{hash_tag}}}:last_seen"

        settle_ms = int(self.settle.total_seconds() * 1000)
        now_ms = int(time.time() * 1000)
        ttl_ms = settle_ms * 10

        async with docket.redis() as redis:
            script = redis.register_script(_DEBOUNCE_LUA)
            result: list[int] = await script(
                keys=[winner_key, seen_key],
                args=[execution.key, settle_ms, now_ms, ttl_ms],
            )

        action = result[0]
        remaining_ms = result[1]

        if action == _ACTION_PROCEED:
            return self

        reason = f"debounce ({self.settle}) on {base_key}"

        if action == _ACTION_RESCHEDULE:
            raise AdmissionBlocked(
                execution,
                reason=reason,
                retry_delay=timedelta(milliseconds=remaining_ms),
            )

        # DROP
        raise AdmissionBlocked(execution, reason=reason, reschedule=False)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass
