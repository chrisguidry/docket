"""Rate limit admission control dependency.

Caps how many times a task (or a per-parameter scope) can execute within a
sliding window.  Uses a Redis sorted set as a sliding window log: members are
execution keys, scores are millisecond timestamps.
"""

from __future__ import annotations

import time
from datetime import timedelta
from types import TracebackType
from typing import Any

from ._base import AdmissionBlocked, Dependency, current_docket, current_execution

# Lua script for atomic sliding-window rate limit check.
#
# KEYS[1] = sorted set key (one per scope)
# ARGV[1] = execution key (member)
# ARGV[2] = current time in milliseconds
# ARGV[3] = window size in milliseconds
# ARGV[4] = max allowed count (limit)
# ARGV[5] = key TTL in milliseconds (window * 2, safety net)
#
# Returns: {action, retry_after_ms}
#   action: 1=PROCEED, 2=BLOCKED
#   retry_after_ms: ms until the oldest entry expires (only for BLOCKED)
_RATELIMIT_LUA = """
local key       = KEYS[1]
local member    = ARGV[1]
local now_ms    = tonumber(ARGV[2])
local window_ms = tonumber(ARGV[3])
local limit     = tonumber(ARGV[4])
local ttl_ms    = tonumber(ARGV[5])

-- Prune entries older than the window
local cutoff = now_ms - window_ms
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)

-- Count remaining entries
local count = redis.call('ZCARD', key)

if count < limit then
    -- Under limit: record this execution and set safety TTL
    redis.call('ZADD', key, now_ms, member)
    redis.call('PEXPIRE', key, ttl_ms)
    return {1, 0}
end

-- Over limit: compute when the oldest entry will expire
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local oldest_score = tonumber(oldest[2])
local retry_after = oldest_score + window_ms - now_ms
if retry_after < 1 then
    retry_after = 1
end
return {2, retry_after}
"""

_ACTION_PROCEED = 1
_ACTION_BLOCKED = 2


class RateLimit(Dependency["RateLimit"]):
    """Cap executions within a sliding time window.

    Uses a Redis sorted set as a sliding window log.  Each execution adds
    an entry; entries older than the window are pruned atomically.

    When the limit is reached:
    - ``drop=False`` (default): the task is rescheduled to when a slot opens.
    - ``drop=True``: the task is quietly dropped.

    Works both as a default parameter and as ``Annotated`` metadata::

        # Per-task: max 10 per minute, excess rescheduled
        async def sync_data(
            rate: RateLimit = RateLimit(10, per=timedelta(minutes=1)),
        ) -> None: ...

        # Per-parameter: max 5 per minute per customer, excess dropped
        async def process_customer(
            customer_id: Annotated[int, RateLimit(5, per=timedelta(minutes=1), drop=True)],
        ) -> None: ...
    """

    def __init__(
        self,
        limit: int,
        *,
        per: timedelta,
        drop: bool = False,
        scope: str | None = None,
    ) -> None:
        self.limit = limit
        self.per = per
        self.drop = drop
        self.scope = scope
        self._argument_name: str | None = None
        self._argument_value: Any = None

    def bind_to_parameter(self, name: str, value: Any) -> RateLimit:
        bound = RateLimit(self.limit, per=self.per, drop=self.drop, scope=self.scope)
        bound._argument_name = name
        bound._argument_value = value
        return bound

    async def __aenter__(self) -> RateLimit:
        execution = current_execution.get()
        docket = current_docket.get()

        scope = self.scope or docket.name
        if self._argument_name is not None:
            ratelimit_key = (
                f"{scope}:ratelimit:{self._argument_name}:{self._argument_value}"
            )
        else:
            ratelimit_key = f"{scope}:ratelimit:{execution.function_name}"

        window_ms = int(self.per.total_seconds() * 1000)
        now_ms = int(time.time() * 1000)
        ttl_ms = window_ms * 2

        async with docket.redis() as redis:
            script = redis.register_script(_RATELIMIT_LUA)
            result: list[int] = await script(
                keys=[ratelimit_key],
                args=[execution.key, now_ms, window_ms, self.limit, ttl_ms],
            )

        action = result[0]
        retry_after_ms = result[1]

        if action == _ACTION_PROCEED:
            return self

        reason = f"rate limit ({self.limit}/{self.per}) on {ratelimit_key}"

        if self.drop:
            raise AdmissionBlocked(execution, reason=reason, reschedule=False)

        raise AdmissionBlocked(
            execution,
            reason=reason,
            retry_delay=timedelta(milliseconds=retry_after_ms),
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass
