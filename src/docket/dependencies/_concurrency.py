"""Concurrency limiting dependency."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, overload

from opentelemetry import propagate

from .._cancellation import CANCEL_MSG_CLEANUP, cancel_task
from ..instrumentation import message_setter
from ._base import (
    AdmissionBlocked,
    Dependency,
    current_docket,
    current_execution,
    current_worker,
)

logger = logging.getLogger("docket.dependencies")

if TYPE_CHECKING:  # pragma: no cover
    from redis.asyncio import Redis

    from ..docket import Docket
    from ..execution import Execution


# Lease renewal happens this many times per redelivery_timeout period.
# Concurrency slot TTLs are set to this many redelivery_timeout periods.
# A factor of 4 means we renew 4x per period and TTLs last 4 periods.
LEASE_RENEWAL_FACTOR = 4

# Minimum TTL in seconds for Redis keys to avoid immediate expiration when
# redelivery_timeout is very small (e.g., in tests with 200ms timeouts).
MINIMUM_TTL_SECONDS = 1


# Acquire a concurrency slot, or park the task on the waiter stream atomically.
# Returns 1 if acquired (task should run), 0 if parked (the inflight stream
# message has been XACK+XDEL'd and re-XADD'd into the waiter stream; caller
# raises ConcurrencyBlocked(handled=True) so the worker takes no further
# action).
#
# KEYS[1]: slots (concurrency_key)
# KEYS[2]: waiters stream (concurrency_key:waiters)
# KEYS[3]: main stream_key
# KEYS[4]: runs_key for this task
# KEYS[5]: waiter-stream registry (docket-scoped HASH; see _concurrency_sweep_loop)
# ARGV: max_concurrent, task_key, current_time, is_redelivery, stale_threshold,
#       key_ttl, message_id, worker_group_name, state_channel,
#       field1, value1, field2, value2, ... (task message payload)
_ACQUIRE_OR_PARK = """
local slots_key = KEYS[1]
local waiters_stream = KEYS[2]
local stream_key = KEYS[3]
local runs_key = KEYS[4]
local registry_key = KEYS[5]

local max_concurrent = tonumber(ARGV[1])
local task_key = ARGV[2]
local current_time = tonumber(ARGV[3])
local is_redelivery = tonumber(ARGV[4])
local stale_threshold = tonumber(ARGV[5])
local key_ttl = tonumber(ARGV[6])
local message_id = ARGV[7]
local worker_group_name = ARGV[8]
local state_channel = ARGV[9]

-- If this task already has a slot (previous delivery attempt), only a
-- redelivery with a stale original holder can take it over.  Otherwise we
-- must not run a second time alongside a still-live peer.
local slot_time = redis.call('ZSCORE', slots_key, task_key)
if slot_time then
    slot_time = tonumber(slot_time)
    if is_redelivery == 1 and slot_time <= stale_threshold then
        redis.call('ZADD', slots_key, current_time, task_key)
        redis.call('EXPIRE', slots_key, key_ttl)
        return 1
    end
else
    if redis.call('ZCARD', slots_key) < max_concurrent then
        redis.call('ZADD', slots_key, current_time, task_key)
        redis.call('EXPIRE', slots_key, key_ttl)
        return 1
    end

    -- All slots full.  Scavenge any that have gone stale (holder is dead).
    local stale_slots = redis.call('ZRANGEBYSCORE', slots_key, 0, stale_threshold, 'LIMIT', 0, 1)
    if #stale_slots > 0 then
        redis.call('ZREM', slots_key, stale_slots[1])
        redis.call('ZADD', slots_key, current_time, task_key)
        redis.call('EXPIRE', slots_key, key_ttl)
        return 1
    end
end

-- Park: ACK the main-stream message, re-XADD the payload into the waiter
-- stream.  Doing this here, atomically with the acquire check, keeps a
-- concurrent slot release from missing us in the gap between "blocked" and
-- "parked".
redis.call('XACK', stream_key, worker_group_name, message_id)
redis.call('XDEL', stream_key, message_id)

-- Bump generation so stale redeliveries of this task are superseded on wake,
-- and splice the new value into the message as we forward it.
local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
local message = {}
local function_name, args_data, kwargs_data
for i = 10, #ARGV, 2 do
    local field_name = ARGV[i]
    local field_value = ARGV[i + 1]
    if field_name == 'generation' then
        field_value = tostring(new_gen)
    elseif field_name == 'function' then
        function_name = field_value
    elseif field_name == 'args' then
        args_data = field_value
    elseif field_name == 'kwargs' then
        kwargs_data = field_value
    end
    message[#message + 1] = field_name
    message[#message + 1] = field_value
end

redis.call('XADD', waiters_stream, '*', unpack(message))

-- Register this waiter stream so the sweep loop can find it.  We re-HSET on
-- every park so the max_concurrent always reflects the latest value (in
-- case the limit was bumped between parks).
redis.call('HSET', registry_key, waiters_stream, tostring(max_concurrent))

redis.call('HSET', runs_key,
    'state', 'scheduled',
    'waiter_stream', waiters_stream,
    'function', function_name,
    'args', args_data,
    'kwargs', kwargs_data
)
redis.call('HDEL', runs_key, 'stream_id')

local payload = '{"type":"state","key":"' .. task_key .. '","state":"scheduled"}'
redis.call('PUBLISH', state_channel, payload)

return 0
"""


# Release this task's slot and, if waiters are parked, hand the freed
# capacity off by re-injecting the oldest waiter(s) into the main stream.
# Stale peer slots are scavenged opportunistically, but only when waiters
# exist -- we don't want to prematurely evict slots held by briefly-paused
# live workers.
#
# KEYS[1]: slots (concurrency_key)
# KEYS[2]: waiters stream (concurrency_key:waiters)
# KEYS[3]: main stream_key
# KEYS[4]: waiter-stream registry (HASH, docket-scoped)
# ARGV: task_key (releasing), max_concurrent, stale_threshold, runs_prefix,
#       state_prefix
_RELEASE_AND_WAKE = """
local slots_key = KEYS[1]
local waiters_stream = KEYS[2]
local stream_key = KEYS[3]
local registry_key = KEYS[4]

local task_key = ARGV[1]
local max_concurrent = tonumber(ARGV[2])
local stale_threshold = tonumber(ARGV[3])
local runs_prefix = ARGV[4]
local state_prefix = ARGV[5]

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
        local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
        for i = 1, #fields, 2 do
            if fields[i] == 'generation' then
                fields[i + 1] = tostring(new_gen)
            end
        end

        local main_id = redis.call('XADD', stream_key, '*', unpack(fields))
        redis.call('XDEL', waiters_stream, waiter_id)
        redis.call('HSET', runs_key, 'state', 'queued', 'stream_id', main_id)
        redis.call('HDEL', runs_key, 'waiter_stream')

        local payload = '{"type":"state","key":"' .. waiter_task_key .. '","state":"queued"}'
        redis.call('PUBLISH', state_prefix .. waiter_task_key, payload)
    end
end

if redis.call('ZCARD', slots_key) == 0 then
    redis.call('DEL', slots_key)
end
if redis.call('XLEN', waiters_stream) == 0 then
    redis.call('DEL', waiters_stream)
    redis.call('HDEL', registry_key, waiters_stream)
end
"""


# Scavenge any stale slot holders and hand freed capacity to parked waiters.
# Called by the worker's concurrency-sweep loop to recover the degenerate
# case where every slot holder crashed without releasing AND no new tasks
# are arriving to trigger the normal acquire-path scavenge.  Structurally
# identical to _RELEASE_AND_WAKE's post-release body, minus the self-ZREM.
#
# Returns the number of waiters woken (zero means either no waiters were
# parked, or no capacity was free to give them).
#
# KEYS[1]: slots (concurrency_key)
# KEYS[2]: waiters stream
# KEYS[3]: main stream_key
# KEYS[4]: waiter-stream registry (HASH, docket-scoped)
# ARGV: max_concurrent, stale_threshold, runs_prefix, state_prefix
_SCAVENGE_AND_WAKE = """
local slots_key = KEYS[1]
local waiters_stream = KEYS[2]
local stream_key = KEYS[3]
local registry_key = KEYS[4]

local max_concurrent = tonumber(ARGV[1])
local stale_threshold = tonumber(ARGV[2])
local runs_prefix = ARGV[3]
local state_prefix = ARGV[4]

local waiters_count = redis.call('XLEN', waiters_stream)
if waiters_count == 0 then
    -- Stream already drained; drop its registry entry and bail out.
    redis.call('HDEL', registry_key, waiters_stream)
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
    local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
    for i = 1, #fields, 2 do
        if fields[i] == 'generation' then
            fields[i + 1] = tostring(new_gen)
        end
    end

    local main_id = redis.call('XADD', stream_key, '*', unpack(fields))
    redis.call('XDEL', waiters_stream, waiter_id)
    redis.call('HSET', runs_key, 'state', 'queued', 'stream_id', main_id)
    redis.call('HDEL', runs_key, 'waiter_stream')

    local payload = '{"type":"state","key":"' .. waiter_task_key .. '","state":"queued"}'
    redis.call('PUBLISH', state_prefix .. waiter_task_key, payload)
    woken = woken + 1
end

if redis.call('ZCARD', slots_key) == 0 then
    redis.call('DEL', slots_key)
end
if redis.call('XLEN', waiters_stream) == 0 then
    redis.call('DEL', waiters_stream)
    redis.call('HDEL', registry_key, waiters_stream)
end

return woken
"""


class ConcurrencyBlocked(AdmissionBlocked):
    """Raised when a task cannot start due to concurrency limits.

    ``__aenter__`` has already atomically parked the task in the
    waiter sorted set at ``_waiter_key`` (acking its stream message
    and storing its payload in the parked hash), so the worker's
    exception handler sees ``handled=True`` and does nothing further.
    """

    def __init__(self, execution: Execution, concurrency_key: str, max_concurrent: int):
        self.concurrency_key = concurrency_key
        self.max_concurrent = max_concurrent
        self._waiter_key = f"{concurrency_key}:waiters"
        reason = f"concurrency limit ({max_concurrent} max) on {concurrency_key}"
        super().__init__(execution, reason=reason, handled=True)


class ConcurrencyLimit(Dependency["ConcurrencyLimit"]):
    """Configures concurrency limits for task execution.

    Can limit concurrency globally for a task, or per specific argument value.

    Works both as a default parameter and as ``Annotated`` metadata::

        # Default-parameter style
        async def process_customer(
            customer_id: int,
            concurrency: ConcurrencyLimit = ConcurrencyLimit("customer_id", 1),
        ) -> None: ...

        # Annotated style (parameter name auto-inferred)
        async def process_customer(
            customer_id: Annotated[int, ConcurrencyLimit(1)],
        ) -> None: ...

        # Per-task (no argument grouping)
        async def expensive(
            concurrency: ConcurrencyLimit = ConcurrencyLimit(max_concurrent=3),
        ) -> None: ...
    """

    single: bool = True

    @overload
    def __init__(
        self,
        max_concurrent: int,
        /,
        *,
        scope: str | None = None,
    ) -> None:
        """Annotated style: ``Annotated[int, ConcurrencyLimit(1)]``."""

    @overload
    def __init__(
        self,
        argument_name: str,
        max_concurrent: int = 1,
        scope: str | None = None,
    ) -> None:
        """Default-param style with per-argument grouping."""

    @overload
    def __init__(
        self,
        *,
        max_concurrent: int = 1,
        scope: str | None = None,
    ) -> None:
        """Per-task concurrency (no argument grouping)."""

    def __init__(
        self,
        argument_name: str | int | None = None,
        max_concurrent: int = 1,
        scope: str | None = None,
    ) -> None:
        if isinstance(argument_name, int):
            self.argument_name: str | None = None
            self.max_concurrent: int = argument_name
        else:
            self.argument_name = argument_name
            self.max_concurrent = max_concurrent
        self.scope = scope
        self._concurrency_key: str | None = None
        self._initialized: bool = False
        self._task_key: str | None = None
        self._renewal_task: asyncio.Task[None] | None = None
        self._redelivery_timeout: timedelta | None = None

    def bind_to_parameter(self, name: str, value: Any) -> ConcurrencyLimit:
        """Bind to an ``Annotated`` parameter, inferring argument_name if not set."""
        argument_name = self.argument_name if self.argument_name is not None else name
        return ConcurrencyLimit(
            argument_name,
            max_concurrent=self.max_concurrent,
            scope=self.scope,
        )

    async def __aenter__(self) -> ConcurrencyLimit:
        from ._functional import _Depends

        execution = current_execution.get()
        docket = current_docket.get()
        worker = current_worker.get()

        assert execution.message_id is not None, (
            "ConcurrencyLimit requires an inflight stream message; acquire-or-park "
            "atomically ACKs the message when the task is blocked."
        )

        # Build the concurrency key.  Defaults to docket.prefix (hash-tagged in
        # Redis Cluster mode) so the slot, waiter, stream, parked, and runs
        # keys that the Lua script touches all share the same hash slot.  A
        # user-supplied scope bypasses the docket prefix: in cluster mode,
        # users sharing a concurrency limit across dockets must hash-tag their
        # scope themselves (e.g. "{shared}").
        scope = self.scope or docket.prefix
        if self.argument_name is not None:
            try:
                argument_value = execution.get_argument(self.argument_name)
            except KeyError as e:
                raise ValueError(
                    f"ConcurrencyLimit argument '{self.argument_name}' not found in "
                    f"task arguments. Available: {list(execution.kwargs.keys())}"
                ) from e
            concurrency_key = (
                f"{scope}:concurrency:{self.argument_name}:{argument_value}"
            )
        else:
            concurrency_key = f"{scope}:concurrency:{execution.function_name}"

        # Create a NEW instance for this specific task execution.  The
        # original (the default parameter value) is shared across all calls,
        # so its attributes must not be mutated.
        limit = ConcurrencyLimit(self.argument_name, self.max_concurrent, self.scope)
        limit._concurrency_key = concurrency_key
        limit._initialized = True
        limit._task_key = execution.key
        limit._redelivery_timeout = worker.redelivery_timeout

        waiters_stream = f"{concurrency_key}:waiters"
        redelivery_timeout = worker.redelivery_timeout

        message: dict[bytes, bytes] = execution.as_message()
        propagate.inject(message, setter=message_setter)

        current_time = datetime.now(timezone.utc).timestamp()
        stale_threshold = current_time - redelivery_timeout.total_seconds()
        key_ttl = max(
            MINIMUM_TTL_SECONDS,
            int(redelivery_timeout.total_seconds() * LEASE_RENEWAL_FACTOR),
        )

        # One atomic script: either acquire a slot, or XACK+XDEL the main
        # stream message and re-XADD the payload into the waiter stream.
        # Folding acquire-and-park together closes a race where a slot holder
        # releases in the gap between an acquire failure and a Python-side
        # park, leaving the blocked task with nothing to wake it.
        async with docket.redis() as redis:
            acquire_or_park = redis.register_script(_ACQUIRE_OR_PARK)
            result = await acquire_or_park(
                keys=[
                    concurrency_key,
                    waiters_stream,
                    docket.stream_key,
                    execution._redis_key,
                    docket.concurrency_waiter_registry_key,
                ],
                args=[
                    self.max_concurrent,
                    execution.key,
                    current_time,
                    1 if execution.redelivered else 0,
                    stale_threshold,
                    key_ttl,
                    execution.message_id,
                    docket.worker_group_name,
                    f"{docket.prefix}:state:{execution.key}",
                    *[
                        item
                        for field, value in message.items()
                        for item in (field, value)
                    ],
                ],
            )

        if not bool(result):  # pragma: no branch
            logger.debug(
                "⏳ Task %s parked on waiter stream %s",
                execution.key,
                waiters_stream,
            )
            raise ConcurrencyBlocked(execution, concurrency_key, self.max_concurrent)

        # Acquired.  Start heartbeating the slot and register the release
        # callback on the resolver's AsyncExitStack.  Order matters (LIFO):
        # release the slot first, then cancel the renewal task.
        limit._renewal_task = asyncio.create_task(
            limit._renew_lease_loop(redelivery_timeout),
            name=f"{docket.name} - concurrency lease:{execution.key}",
        )
        stack = _Depends.stack.get()
        stack.push_async_callback(limit._release_and_wake)
        stack.push_async_callback(cancel_task, limit._renewal_task, CANCEL_MSG_CLEANUP)

        return limit

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: type[BaseException] | None,
    ) -> None:
        # No-op.  Cleanup is registered on the resolver's AsyncExitStack
        # against the per-task instance created in __aenter__, so it runs
        # with the right state when the dependency context unwinds.
        pass

    async def _release_and_wake(self) -> None:
        """Release this task's slot and hand freed capacity to waiters."""
        assert self._concurrency_key and self._task_key and self._redelivery_timeout

        docket = current_docket.get()
        waiters_stream = f"{self._concurrency_key}:waiters"
        current_time = datetime.now(timezone.utc).timestamp()
        stale_threshold = current_time - self._redelivery_timeout.total_seconds()

        async with docket.redis() as redis:
            release = redis.register_script(_RELEASE_AND_WAKE)
            await release(
                keys=[
                    self._concurrency_key,
                    waiters_stream,
                    docket.stream_key,
                    docket.concurrency_waiter_registry_key,
                ],
                args=[
                    self._task_key,
                    self.max_concurrent,
                    stale_threshold,
                    f"{docket.prefix}:runs:",
                    f"{docket.prefix}:state:",
                ],
            )

    async def _renew_lease_loop(self, redelivery_timeout: timedelta) -> None:
        """Periodically refresh slot timestamp to prevent expiration."""
        docket = current_docket.get()
        renewal_interval = redelivery_timeout.total_seconds() / LEASE_RENEWAL_FACTOR
        key_ttl = max(
            MINIMUM_TTL_SECONDS,
            int(redelivery_timeout.total_seconds() * LEASE_RENEWAL_FACTOR),
        )

        while True:
            await asyncio.sleep(renewal_interval)
            try:
                async with docket.redis() as redis:
                    current_time = datetime.now(timezone.utc).timestamp()
                    await redis.zadd(
                        self._concurrency_key,
                        {self._task_key: current_time},  # type: ignore
                    )
                    await redis.expire(self._concurrency_key, key_ttl)  # type: ignore
            except Exception:  # pragma: no cover
                # Lease renewal is best-effort; if it fails, the slot will eventually
                # be scavenged as stale and the task can be redelivered
                logger.warning(
                    "Concurrency lease renewal failed for %s",
                    self._concurrency_key,
                    exc_info=True,
                )

    @property
    def concurrency_key(self) -> str:
        """Redis key used for tracking concurrency for this specific argument value.
        Raises RuntimeError if accessed before initialization."""
        if not self._initialized:
            raise RuntimeError(
                "ConcurrencyLimit not initialized - use within task context"
            )
        assert self._concurrency_key is not None
        return self._concurrency_key


async def sweep_concurrency_waiters(
    docket: "Docket",
    redis: "Redis",
    redelivery_timeout: timedelta,
) -> int:
    """Scavenge dead concurrency holders and wake any stranded waiters.

    Enumerates the docket's waiter-stream registry (populated lazily by
    the acquire-or-park Lua script) and runs ``_SCAVENGE_AND_WAKE`` on
    each active waiter stream.  If the registry is empty -- which is the
    common case when no concurrency-limited task is currently blocked --
    this function is effectively free: one HKEYS call and done.

    Returns the total number of waiters woken across all streams.  Used
    by the Worker's ``_concurrency_sweep_loop`` to recover the degenerate
    case where every slot holder crashed without releasing AND no new
    arriving task has triggered the normal acquire-path scavenge.
    """
    registry_key = docket.concurrency_waiter_registry_key
    entries: dict[bytes, bytes] = await redis.hgetall(registry_key)  # type: ignore[assignment,misc]
    if not entries:
        return 0

    scavenge = redis.register_script(_SCAVENGE_AND_WAKE)
    stale_threshold = (
        datetime.now(timezone.utc).timestamp() - redelivery_timeout.total_seconds()
    )
    runs_prefix = f"{docket.prefix}:runs:"
    state_prefix = f"{docket.prefix}:state:"

    total_woken = 0
    for waiters_stream_bytes, max_concurrent_bytes in entries.items():
        waiters_stream = waiters_stream_bytes.decode()
        concurrency_key = waiters_stream.removesuffix(":waiters")
        woken = await scavenge(
            keys=[
                concurrency_key,
                waiters_stream,
                docket.stream_key,
                registry_key,
            ],
            args=[
                int(max_concurrent_bytes),
                stale_threshold,
                runs_prefix,
                state_prefix,
            ],
        )
        total_woken += int(woken)
    return total_woken
