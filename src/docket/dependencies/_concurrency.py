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

    from ..execution import Execution


# Lease renewal happens this many times per redelivery_timeout period.
# Concurrency slot TTLs are set to this many redelivery_timeout periods.
# A factor of 4 means we renew 4x per period and TTLs last 4 periods.
LEASE_RENEWAL_FACTOR = 4

# Minimum TTL in seconds for Redis keys to avoid immediate expiration when
# redelivery_timeout is very small (e.g., in tests with 200ms timeouts).
MINIMUM_TTL_SECONDS = 1


class ConcurrencyBlocked(AdmissionBlocked):
    """Raised when a task cannot start due to concurrency limits."""

    def __init__(self, execution: Execution, concurrency_key: str, max_concurrent: int):
        self.concurrency_key = concurrency_key
        self.max_concurrent = max_concurrent
        reason = f"concurrency limit ({max_concurrent} max) on {concurrency_key}"
        super().__init__(
            execution,
            reason=reason,
            waiter_key=f"{concurrency_key}:waiters",
        )


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

        # Build concurrency key based on argument_name (if provided) or function name
        scope = self.scope or docket.name
        if self.argument_name is not None:
            # Per-argument concurrency: limit based on specific argument value
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
            # Per-task concurrency: limit based on task function name
            concurrency_key = f"{scope}:concurrency:{execution.function_name}"

        # Create a NEW instance for this specific task execution
        # This is critical because the original instance is shared across all tasks
        # (Python default arguments are evaluated once at function definition time)
        limit = ConcurrencyLimit(self.argument_name, self.max_concurrent, self.scope)
        limit._concurrency_key = concurrency_key
        limit._initialized = True
        limit._task_key = execution.key
        limit._redelivery_timeout = worker.redelivery_timeout

        # Acquire slot (atomically parks as waiter if blocked)
        async with docket.redis() as redis:
            acquired = await limit._acquire_slot(
                redis,
                execution,
                worker.redelivery_timeout,
            )
            if not acquired:  # pragma: no branch
                raise ConcurrencyBlocked(
                    execution, concurrency_key, self.max_concurrent
                )

        # Spawn background task for lease renewal
        limit._renewal_task = asyncio.create_task(
            limit._renew_lease_loop(worker.redelivery_timeout),
            name=f"{docket.name} - concurrency lease:{execution.key}",
        )

        # Register cleanup for this new instance with the AsyncExitStack
        # (The original instance's __aexit__ will also be called but does nothing)
        # Order matters (LIFO): release slot first, then cancel renewal task
        stack = _Depends.stack.get()
        stack.push_async_callback(limit._release_slot)
        stack.push_async_callback(cancel_task, limit._renewal_task, CANCEL_MSG_CLEANUP)

        return limit

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: type[BaseException] | None,
    ) -> None:
        # No-op: The original instance (used as default argument) has no state.
        # Actual cleanup is handled by _cleanup() on the per-task instance,
        # which is registered with the AsyncExitStack via push_async_callback.
        pass

    async def _acquire_slot(
        self,
        redis: Redis,
        execution: Execution,
        redelivery_timeout: timedelta,
    ) -> bool:
        """Atomically acquire a concurrency slot, or park as a waiter.

        Uses a Redis sorted set to track concurrency slots per task. Each entry
        is keyed by task_key with the timestamp as the score.

        When XAUTOCLAIM reclaims a message (because the original worker stopped
        renewing its lease), ``execution.redelivered`` signals that slot
        takeover is safe.  If the message is NOT a redelivery and a slot
        already exists, we block to prevent duplicate execution.

        Slots are refreshed during lease renewal every redelivery_timeout/4.
        If all slots are full, we scavenge any slot older than redelivery_timeout
        (meaning it hasn't been refreshed and the worker must be dead).

        When the task cannot acquire, the script atomically parks it in the
        waiter sorted set: the inflight stream message is XACK'd + XDEL'd, the
        task payload is stored in the parked hash, and the task_key is added
        to the waiter zset.  The caller raises ``ConcurrencyBlocked`` and the
        worker treats this as "already parked" (no further ack/reschedule).

        Doing acquire-and-park in one script closes the race where a slot
        holder releases in the gap between an acquire failure and a Python-side
        park: with two round-trips the release could pop no waiters and then
        the blocked task would park with nothing left to wake it.
        """
        assert execution._inflight_message_id is not None
        waiters_key = f"{self._concurrency_key}:waiters"
        docket = execution.docket

        message: dict[bytes, bytes] = execution.as_message()
        propagate.inject(message, setter=message_setter)

        current_time = datetime.now(timezone.utc).timestamp()
        stale_threshold = current_time - redelivery_timeout.total_seconds()
        key_ttl = max(
            MINIMUM_TTL_SECONDS,
            int(redelivery_timeout.total_seconds() * LEASE_RENEWAL_FACTOR),
        )

        acquire_script = redis.register_script(
            # KEYS[1]: slots_key (concurrency_key)
            # KEYS[2]: waiters_key
            # KEYS[3]: stream_key
            # KEYS[4]: parked_key (for this task)
            # KEYS[5]: runs_key (for this task)
            # ARGV[1]: max_concurrent
            # ARGV[2]: task_key
            # ARGV[3]: current_time
            # ARGV[4]: is_redelivery (0/1)
            # ARGV[5]: stale_threshold
            # ARGV[6]: key_ttl
            # ARGV[7]: message_id (for XACK+XDEL on park)
            # ARGV[8]: worker_group_name
            # ARGV[9]: state_channel (pub/sub channel for state change)
            # ARGV[10+]: alternating field/value pairs of the parked task payload
            """
            local slots_key = KEYS[1]
            local waiters_key = KEYS[2]
            local stream_key = KEYS[3]
            local parked_key = KEYS[4]
            local runs_key = KEYS[5]

            local max_concurrent = tonumber(ARGV[1])
            local task_key = ARGV[2]
            local current_time = tonumber(ARGV[3])
            local is_redelivery = tonumber(ARGV[4])
            local stale_threshold = tonumber(ARGV[5])
            local key_ttl = tonumber(ARGV[6])
            local message_id = ARGV[7]
            local worker_group_name = ARGV[8]
            local state_channel = ARGV[9]

            -- Check if this task already has a slot (from a previous delivery attempt)
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

                local stale_slots = redis.call('ZRANGEBYSCORE', slots_key, 0, stale_threshold, 'LIMIT', 0, 1)
                if #stale_slots > 0 then
                    redis.call('ZREM', slots_key, stale_slots[1])
                    redis.call('ZADD', slots_key, current_time, task_key)
                    redis.call('EXPIRE', slots_key, key_ttl)
                    return 1
                end
            end

            -- Cannot acquire; park atomically so a concurrent slot release can
            -- observe us in the waiter zset and wake us up.
            redis.call('XACK', stream_key, worker_group_name, message_id)
            redis.call('XDEL', stream_key, message_id)

            local message = {}
            local function_name = nil
            local args_data = nil
            local kwargs_data = nil
            local generation_index = nil
            for i = 10, #ARGV, 2 do
                local field_name = ARGV[i]
                local field_value = ARGV[i + 1]
                message[#message + 1] = field_name
                message[#message + 1] = field_value
                if field_name == 'function' then
                    function_name = field_value
                elseif field_name == 'args' then
                    args_data = field_value
                elseif field_name == 'kwargs' then
                    kwargs_data = field_value
                elseif field_name == 'generation' then
                    generation_index = #message
                end
            end

            local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
            if generation_index then
                message[generation_index] = tostring(new_gen)
            end

            redis.call('HSET', parked_key, unpack(message))
            redis.call('ZADD', waiters_key, current_time, task_key)

            redis.call('HSET', runs_key,
                'state', 'scheduled',
                'function', function_name,
                'args', args_data,
                'kwargs', kwargs_data
            )
            redis.call('HDEL', runs_key, 'stream_id')

            local payload = '{"type":"state","key":"' .. task_key .. '","state":"scheduled"}'
            redis.call('PUBLISH', state_channel, payload)

            return 0
            """
        )

        result = await acquire_script(
            keys=[
                self._concurrency_key,
                waiters_key,
                docket.stream_key,
                docket.parked_task_key(execution.key),
                execution._redis_key,
            ],
            args=[
                self.max_concurrent,
                self._task_key,
                current_time,
                1 if execution.redelivered else 0,
                stale_threshold,
                key_ttl,
                execution._inflight_message_id,
                docket.worker_group_name,
                f"{docket.prefix}:state:{execution.key}",
                *[item for field, value in message.items() for item in (field, value)],
            ],
        )

        return bool(result)

    async def _release_slot(self) -> None:
        """Release a concurrency slot and wake any parked waiters.

        Atomically:
        - Removes this task's slot from the sorted set
        - Opportunistically scavenges any other slots whose lease has gone
          stale (the worker holding them is dead and can't release)
        - For each unit of freed capacity, pops the oldest waiter, reads its
          parked payload, increments its generation, and XADDs it back into
          the stream so the woken task is picked up on the next worker poll
        - Cleans up emptied sorted sets and consumed parked hashes
        """
        assert self._concurrency_key and self._task_key and self._redelivery_timeout

        docket = current_docket.get()
        waiters_key = f"{self._concurrency_key}:waiters"
        current_time = datetime.now(timezone.utc).timestamp()
        stale_threshold = current_time - self._redelivery_timeout.total_seconds()

        async with docket.redis() as redis:
            release_script = redis.register_script(
                # KEYS[1]: slots (concurrency_key)
                # KEYS[2]: waiters (concurrency_key:waiters)
                # KEYS[3]: stream_key
                # ARGV[1]: task_key (releasing)
                # ARGV[2]: max_concurrent
                # ARGV[3]: stale_threshold
                # ARGV[4]: docket_prefix  (used to build {prefix}:{key} parked hash path)
                # ARGV[5]: runs_prefix    (used to build {prefix}:runs:{key})
                # ARGV[6]: state_prefix   (used to build {prefix}:state:{key} pubsub channel)
                """
                local slots_key = KEYS[1]
                local waiters_key = KEYS[2]
                local stream_key = KEYS[3]

                local task_key = ARGV[1]
                local max_concurrent = tonumber(ARGV[2])
                local stale_threshold = tonumber(ARGV[3])
                local docket_prefix = ARGV[4]
                local runs_prefix = ARGV[5]
                local state_prefix = ARGV[6]

                -- Release this task's slot
                redis.call('ZREM', slots_key, task_key)

                -- Only evict slots held by dead workers if waiters are queued:
                -- scavenging is on-demand elsewhere in the system, and we don't
                -- want to prematurely evict slots whose live holder is briefly
                -- paused.  When waiters exist, liveness matters more.
                local waiters_count = redis.call('ZCARD', waiters_key)
                if waiters_count > 0 then
                    local stale = redis.call('ZRANGEBYSCORE', slots_key, 0, stale_threshold)
                    for _, s in ipairs(stale) do
                        redis.call('ZREM', slots_key, s)
                    end
                end

                -- Wake up to (max - held) waiters, oldest first (FIFO)
                local capacity = max_concurrent - redis.call('ZCARD', slots_key)
                if capacity > 0 then
                    local waiters = redis.call('ZRANGE', waiters_key, 0, capacity - 1)
                    for _, w in ipairs(waiters) do
                        redis.call('ZREM', waiters_key, w)
                        local parked_key = docket_prefix .. ':' .. w
                        local runs_key = runs_prefix .. w
                        local fields = redis.call('HGETALL', parked_key)
                        if #fields > 0 then
                            -- Supersede any stale redeliveries of this task
                            local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
                            for i = 1, #fields, 2 do
                                if fields[i] == 'generation' then
                                    fields[i + 1] = tostring(new_gen)
                                end
                            end
                            local message_id = redis.call('XADD', stream_key, '*', unpack(fields))
                            redis.call('DEL', parked_key)
                            redis.call('HSET', runs_key,
                                'state', 'queued',
                                'stream_id', message_id
                            )
                            local payload = '{"type":"state","key":"' .. w .. '","state":"queued"}'
                            redis.call('PUBLISH', state_prefix .. w, payload)
                        end
                    end
                end

                if redis.call('ZCARD', slots_key) == 0 then
                    redis.call('DEL', slots_key)
                end
                if redis.call('ZCARD', waiters_key) == 0 then
                    redis.call('DEL', waiters_key)
                end
                """
            )
            await release_script(
                keys=[self._concurrency_key, waiters_key, docket.stream_key],
                args=[
                    self._task_key,
                    self.max_concurrent,
                    stale_threshold,
                    docket.prefix,
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
