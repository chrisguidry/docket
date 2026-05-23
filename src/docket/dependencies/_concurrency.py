"""Concurrency limiting dependency."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, AsyncIterator, overload

from opentelemetry import propagate

from .._cancellation import CANCEL_MSG_CLEANUP, cancel_task
from .._lua import Arg, Args, Key, redis_script
from .._redis import RedisClient
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
    from ..docket import Docket
    from ..execution import Execution
    from ..worker import Worker


# Lease renewal happens this many times per redelivery_timeout period.
# Concurrency slot TTLs are set to this many redelivery_timeout periods.
# A factor of 4 means we renew 4x per period and TTLs last 4 periods.
LEASE_RENEWAL_FACTOR = 4

# Minimum TTL in seconds for Redis keys to avoid immediate expiration when
# redelivery_timeout is very small (e.g., in tests with 200ms timeouts).
MINIMUM_TTL_SECONDS = 1


@redis_script
async def _acquire_or_park(
    redis: RedisClient,
    *,
    slots_key: Key[str],
    waiters_stream: Key[str],
    stream_key: Key[str],
    runs_key: Key[str],
    max_concurrent: Arg[int],
    task_key: Arg[str],
    current_time: Arg[float],
    is_redelivery: Arg[bool],
    stale_threshold: Arg[float],
    key_ttl: Arg[int],
    message_id: Arg[bytes],
    worker_group_name: Arg[str],
    state_channel: Arg[str],
    message: Args[dict[bytes, bytes]],
) -> int:
    """
    -- Acquire a concurrency slot, or park the task on the waiter stream
    -- atomically.  Returns 1 if acquired (task should run), 0 if parked
    -- (the inflight stream message has been XACK+XDEL'd and re-XADD'd
    -- into the waiter stream; caller raises ConcurrencyBlocked(handled=True)
    -- so the worker takes no further action).

    -- If this task already has a slot (previous delivery attempt), only a
    -- redelivery with a stale original holder can take it over.  Otherwise we
    -- must not run a second time alongside a still-live peer.
    local slot_time = redis.call('ZSCORE', slots_key, task_key)
    if slot_time then
        slot_time = tonumber(slot_time)
        if is_redelivery and slot_time <= stale_threshold then
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
    for i = message_start, #ARGV, 2 do
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

    local waiter_entry_id = redis.call('XADD', waiters_stream, '*', unpack(message))

    -- Record the waiter's location so the dependency's cancel subscriber can
    -- find and XDEL the waiter entry when the task transitions to 'cancelled'.
    redis.call('HSET', runs_key,
        'state', 'scheduled',
        'waiter_stream', waiters_stream,
        'waiter_entry_id', waiter_entry_id,
        'function', function_name,
        'args', args_data,
        'kwargs', kwargs_data
    )
    redis.call('HDEL', runs_key, 'stream_id')

    local payload = '{"type":"state","key":"' .. task_key .. '","state":"scheduled"}'
    redis.call('PUBLISH', state_channel, payload)

    return 0
    """
    ...


@redis_script
async def _release_and_wake(
    redis: RedisClient,
    *,
    slots_key: Key[str],
    waiters_stream: Key[str],
    stream_key: Key[str],
    queue_key: Key[str],
    task_key: Arg[str],
    max_concurrent: Arg[int],
    stale_threshold: Arg[float],
    runs_prefix: Arg[str],
    state_prefix: Arg[str],
    parked_prefix: Arg[str],
) -> None:
    """
    -- Release this task's slot and, if waiters are parked, hand the freed
    -- capacity off by re-injecting the oldest waiter(s) into the main
    -- stream.  Stale peer slots are scavenged opportunistically, but only
    -- when waiters exist -- we don't want to prematurely evict slots held
    -- by briefly-paused live workers.
    --
    -- Waiters whose runs hash has flipped to ``cancelled`` are XDEL'd
    -- without being forwarded.  This is the correctness backstop for the
    -- cancel-races-with-wake case where the dependency's cancel subscriber
    -- might not have pulled the waiter entry off the stream in time (Redis
    -- pub/sub is fire-and-forget); cancelled tasks must never run.

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

            -- Forward only if the task is still in the parked state.  Anything
            -- else -- 'cancelled', a missing runs hash (DELed by a cancel with
            -- execution_ttl=0), or any other terminal state -- means the task
            -- has been superseded and must not be revived.  Drop the waiter
            -- entry without forwarding.
            local current_state = redis.call('HGET', runs_key, 'state')
            local safeguard_key = '__safeguard__:' .. waiter_task_key
            if current_state ~= 'scheduled' then
                redis.call('XDEL', waiters_stream, waiter_id)
                -- Cancelled/superseded waiter: drop its safeguard backstop too.
                redis.call('ZREM', queue_key, safeguard_key)
                redis.call('DEL', parked_prefix .. safeguard_key)
                redis.call('DEL', runs_prefix .. safeguard_key)
            else
                local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
                for i = 1, #fields, 2 do
                    if fields[i] == 'generation' then
                        fields[i + 1] = tostring(new_gen)
                    end
                end

                local main_id = redis.call('XADD', stream_key, '*', unpack(fields))
                redis.call('XDEL', waiters_stream, waiter_id)
                redis.call('HSET', runs_key, 'state', 'queued', 'stream_id', main_id)
                redis.call('HDEL', runs_key, 'waiter_stream', 'waiter_entry_id')

                -- Cancel the safeguard task this waiter scheduled on park.
                -- It's no longer needed; leaving it in the future queue would
                -- keep the worker awake for one redelivery_timeout for nothing.
                redis.call('ZREM', queue_key, safeguard_key)
                redis.call('DEL', parked_prefix .. safeguard_key)
                redis.call('DEL', runs_prefix .. safeguard_key)

                local payload = '{"type":"state","key":"' .. waiter_task_key .. '","state":"queued"}'
                redis.call('PUBLISH', state_prefix .. waiter_task_key, payload)
            end
        end
    end

    if redis.call('ZCARD', slots_key) == 0 then
        redis.call('DEL', slots_key)
    end
    if redis.call('XLEN', waiters_stream) == 0 then
        redis.call('DEL', waiters_stream)
    end
    """
    ...


@redis_script
async def _scavenge_and_wake(
    redis: RedisClient,
    *,
    slots_key: Key[str],
    waiters_stream: Key[str],
    stream_key: Key[str],
    queue_key: Key[str],
    max_concurrent: Arg[int],
    stale_threshold: Arg[float],
    runs_prefix: Arg[str],
    state_prefix: Arg[str],
    parked_prefix: Arg[str],
) -> int:
    """
    -- Scavenge any stale slot holders and hand freed capacity to parked
    -- waiters.  Called by the worker's concurrency-sweep loop to recover
    -- the degenerate case where every slot holder crashed without
    -- releasing AND no new tasks are arriving to trigger the normal
    -- acquire-path scavenge.  Structurally identical to
    -- _release_and_wake's post-release body, minus the self-ZREM.
    --
    -- Returns the number of waiters woken (zero means either no waiters
    -- were parked, or no capacity was free to give them).

    local waiters_count = redis.call('XLEN', waiters_stream)
    if waiters_count == 0 then
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

        -- Skip cancelled/superseded waiters: drop the entry without forwarding.
        local current_state = redis.call('HGET', runs_key, 'state')
        local safeguard_key = '__safeguard__:' .. waiter_task_key
        if current_state ~= 'scheduled' then
            redis.call('XDEL', waiters_stream, waiter_id)
            redis.call('ZREM', queue_key, safeguard_key)
            redis.call('DEL', parked_prefix .. safeguard_key)
            redis.call('DEL', runs_prefix .. safeguard_key)
        else
            local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)
            for i = 1, #fields, 2 do
                if fields[i] == 'generation' then
                    fields[i + 1] = tostring(new_gen)
                end
            end

            local main_id = redis.call('XADD', stream_key, '*', unpack(fields))
            redis.call('XDEL', waiters_stream, waiter_id)
            redis.call('HSET', runs_key, 'state', 'queued', 'stream_id', main_id)
            redis.call('HDEL', runs_key, 'waiter_stream', 'waiter_entry_id')

            -- Cancel the safeguard task this waiter scheduled on park.
            redis.call('ZREM', queue_key, safeguard_key)
            redis.call('DEL', parked_prefix .. safeguard_key)
            redis.call('DEL', runs_prefix .. safeguard_key)

            local payload = '{"type":"state","key":"' .. waiter_task_key .. '","state":"queued"}'
            redis.call('PUBLISH', state_prefix .. waiter_task_key, payload)
            woken = woken + 1
        end
    end

    if redis.call('ZCARD', slots_key) == 0 then
        redis.call('DEL', slots_key)
    end
    if redis.call('XLEN', waiters_stream) == 0 then
        redis.call('DEL', waiters_stream)
    end

    return woken
    """
    ...


@redis_script
async def _cancel_cleanup(
    redis: RedisClient,
    *,
    waiters_stream: Key[str],
    progress_key: Key[str],
    runs_key: Key[str],
    waiter_entry_id: Arg[str],
) -> None:
    """
    -- Atomically tear down a cancelled task's waiter footprint.  Invoked
    -- by ConcurrencyLimit's pubsub-driven cancel subscriber after
    -- Docket.cancel flips the task to 'cancelled'.

    redis.call('XDEL', waiters_stream, waiter_entry_id)
    if redis.call('XLEN', waiters_stream) == 0 then
        redis.call('DEL', waiters_stream)
    end

    -- claim() creates a per-task progress hash before the concurrency gate
    -- runs, so a task that's cancelled while parked leaks one of these
    -- without an explicit DEL.
    redis.call('DEL', progress_key)

    redis.call('HDEL', runs_key, 'waiter_stream', 'waiter_entry_id')
    """
    ...


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

        # Build the concurrency key.  Always anchored under ``docket.prefix``
        # so the slot, waiter, stream, parked, and runs keys touched by the
        # Lua script share the same hash slot in Redis Cluster mode.  A
        # user-supplied ``scope`` is treated as a sub-namespace within the
        # docket; it cannot bypass the docket prefix because the
        # acquire/release/scavenge scripts now also reference the docket's
        # ``stream_key`` and ``runs:*`` keys, which would CROSSSLOT against
        # any independent prefix in cluster mode.
        scope = f"{docket.prefix}:{self.scope}" if self.scope else docket.prefix
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
            result = await _acquire_or_park(
                redis,
                slots_key=concurrency_key,
                waiters_stream=waiters_stream,
                stream_key=docket.stream_key,
                runs_key=execution._redis_key,
                max_concurrent=self.max_concurrent,
                task_key=execution.key,
                current_time=current_time,
                is_redelivery=execution.redelivered,
                stale_threshold=stale_threshold,
                key_ttl=key_ttl,
                message_id=execution.message_id,
                worker_group_name=docket.worker_group_name,
                state_channel=f"{docket.prefix}:state:{execution.key}",
                message=message,
            )

        if not bool(result):  # pragma: no branch
            logger.debug(
                "⏳ Task %s parked on waiter stream %s",
                execution.key,
                waiters_stream,
            )
            # Schedule a safeguard task to backstop the normal release path.
            # If a holder releases first, this becomes a no-op when it runs;
            # otherwise it scavenges any stale slots and wakes the waiters.
            safeguard_key = f"__safeguard__:{execution.key}"
            await docket.add(
                _safeguard_wake,
                when=datetime.now(timezone.utc) + redelivery_timeout,
                key=safeguard_key,
            )(waiter_stream=waiters_stream, max_concurrent=self.max_concurrent)
            # A wake-on-release may have fired between the park script
            # returning and the docket.add above.  Its safeguard ZREM was a
            # no-op (the safeguard hadn't been added yet), and we'd leak
            # the safeguard into the future queue -- preventing
            # ``run_until_finished`` from ever seeing an empty queue.
            # If the runs hash shows we've already been forwarded back to
            # the main stream, cancel the leftover safeguard.
            async with docket.redis() as redis:
                state = await redis.hget(  # type: ignore[misc]
                    execution._redis_key, "state"
                )
            if state != b"scheduled":
                await docket.cancel(safeguard_key)
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

    @classmethod
    @asynccontextmanager
    async def worker_lifecycle(
        cls, docket: "Docket", worker: "Worker"
    ) -> AsyncIterator[None]:
        """Worker-scoped setup/teardown for the ConcurrencyLimit dependency.

        Registers ``_safeguard_wake`` with the docket so the per-park
        backstop tasks are recognized when picked up from the future queue,
        and runs a cancel subscriber that cleans up parked waiter entries
        when their tasks transition to ``cancelled`` via ``Docket.cancel``.

        Both pieces are owned by the dependency -- the Worker just enters
        and exits this lifecycle around its main loop.
        """
        docket.register(_safeguard_wake)

        cancel_task_handle = asyncio.create_task(
            cls._cancel_subscriber(docket),
            name=f"{docket.name} - concurrency cancel subscriber",
        )
        try:
            yield
        finally:
            cancel_task_handle.cancel()
            await asyncio.gather(cancel_task_handle, return_exceptions=True)

    @classmethod
    async def _cancel_subscriber(cls, docket: "Docket") -> None:
        """Listen on Docket's cancel pubsub and tear down parked waiters.

        ``Docket.cancel`` publishes the task key on
        ``{docket.prefix}:cancel:{task_key}`` for every cancel call --
        regardless of whether the task is running, queued, scheduled, or
        parked.  We pattern-subscribe and, for any cancel of a task that
        has parked itself on one of our waiter streams, XDEL the entry
        and clean up the orphan ``progress:*`` hash that ``claim()`` left
        behind before the concurrency gate ran.

        Best-effort: Redis pubsub is fire-and-forget, so a missed publish
        leaves a cancelled waiter on its stream.  The wake-side
        ``runs.state == 'cancelled'`` check in ``_RELEASE_AND_WAKE`` /
        ``_SCAVENGE_AND_WAKE`` is the correctness backstop that ensures
        cancelled tasks never run regardless of whether we observe the
        publish.
        """
        pattern = f"{docket.prefix}:cancel:*"
        async with docket.redis() as redis:
            pubsub = redis.pubsub()
            try:
                await pubsub.psubscribe(pattern)
                async for message in pubsub.listen():
                    if message.get("type") != "pmessage":
                        continue
                    data = message.get("data")
                    if isinstance(data, bytes):
                        task_key = data.decode()
                    elif isinstance(data, str):
                        task_key = data
                    else:  # pragma: no cover
                        continue
                    if not task_key:  # pragma: no cover
                        continue
                    await cls._cleanup_cancelled_waiter(docket, task_key)
            except asyncio.CancelledError:
                pass
            finally:
                try:
                    await pubsub.punsubscribe(pattern)
                except Exception:  # pragma: no cover
                    pass
                try:
                    await pubsub.aclose()
                except Exception:  # pragma: no cover
                    pass

    @classmethod
    async def _cleanup_cancelled_waiter(cls, docket: "Docket", task_key: str) -> None:
        """Remove a cancelled task's parked entry from its waiter stream.

        Looks up the waiter location recorded on the task's runs hash by
        ``_ACQUIRE_OR_PARK``; if absent the task wasn't parked and there's
        nothing to do.  Otherwise XDEL the entry and DEL the
        ``progress:*`` hash that ``claim()`` left behind before the
        concurrency gate parked the task.
        """
        runs_key = f"{docket.prefix}:runs:{task_key}"
        async with docket.redis() as redis:
            waiter_stream_b, waiter_entry_id_b = await redis.hmget(  # type: ignore[misc]
                runs_key, "waiter_stream", "waiter_entry_id"
            )
            if not waiter_stream_b or not waiter_entry_id_b:
                return
            waiter_stream = waiter_stream_b.decode()
            waiter_entry_id = waiter_entry_id_b.decode()
            await _cancel_cleanup(
                redis,
                waiters_stream=waiter_stream,
                progress_key=f"{docket.prefix}:progress:{task_key}",
                runs_key=runs_key,
                waiter_entry_id=waiter_entry_id,
            )
        # Drop the safeguard task this waiter scheduled at park time.
        # Cancelling routes through the same cancel pubsub we're subscribed
        # to, but the resulting cleanup callback for the safeguard's own
        # key is a no-op (no waiter_stream on the safeguard's runs hash).
        await docket.cancel(f"__safeguard__:{task_key}")

    async def _release_and_wake(self) -> None:
        """Release this task's slot and hand freed capacity to waiters."""
        assert self._concurrency_key and self._task_key and self._redelivery_timeout

        docket = current_docket.get()
        waiters_stream = f"{self._concurrency_key}:waiters"
        current_time = datetime.now(timezone.utc).timestamp()
        stale_threshold = current_time - self._redelivery_timeout.total_seconds()

        async with docket.redis() as redis:
            await _release_and_wake(
                redis,
                slots_key=self._concurrency_key,
                waiters_stream=waiters_stream,
                stream_key=docket.stream_key,
                queue_key=docket.queue_key,
                task_key=self._task_key,
                max_concurrent=self.max_concurrent,
                stale_threshold=stale_threshold,
                runs_prefix=f"{docket.prefix}:runs:",
                state_prefix=f"{docket.prefix}:state:",
                parked_prefix=f"{docket.prefix}:",
            )

    async def _renew_lease_loop(self, redelivery_timeout: timedelta) -> None:
        """Periodically refresh slot timestamp to prevent expiration."""
        # Lease renewal is only scheduled when a slot was acquired, which
        # requires both keys to be set.
        assert self._concurrency_key and self._task_key
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
                        {self._task_key: current_time},
                    )
                    await redis.expire(self._concurrency_key, key_ttl)
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


async def _safeguard_wake(waiter_stream: str, max_concurrent: int) -> None:
    """Recover a waiter stream that the normal release path may not reach.

    Scheduled by ``ConcurrencyLimit.__aenter__`` as a future-queue task
    immediately after a successful park.  By the time it runs (one
    ``redelivery_timeout`` later), one of three things is true:

    1. The normal release path has already drained the stream -- the
       script's ``XLEN == 0`` short-circuit returns immediately.
    2. The stream still has waiters but live capacity has freed up by
       other means -- the script wakes whatever fits.
    3. Every slot holder died without releasing and nothing else has
       arrived to scavenge them -- the script evicts the stale slots
       and wakes the waiters.

    This is the dependency-owned recovery for the "burst then idle"
    pathology.  No background loop, no central registry: each park
    schedules its own backstop.
    """
    docket = current_docket.get()
    worker = current_worker.get()

    if not waiter_stream.endswith(":waiters"):  # pragma: no cover
        return
    concurrency_key = waiter_stream[: -len(":waiters")]

    redelivery_timeout = worker.redelivery_timeout
    stale_threshold = (
        datetime.now(timezone.utc).timestamp() - redelivery_timeout.total_seconds()
    )

    async with docket.redis() as redis:
        await _scavenge_and_wake(
            redis,
            slots_key=concurrency_key,
            waiters_stream=waiter_stream,
            stream_key=docket.stream_key,
            queue_key=docket.queue_key,
            max_concurrent=max_concurrent,
            stale_threshold=stale_threshold,
            runs_prefix=f"{docket.prefix}:runs:",
            state_prefix=f"{docket.prefix}:state:",
            parked_prefix=f"{docket.prefix}:",
        )
