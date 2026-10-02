"""Concurrency limiting dependency."""

from __future__ import annotations

import asyncio
import json
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
    state_payload: Arg[str],
    message: Args[dict[bytes, bytes]],
) -> int: ...


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
) -> None: ...


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
) -> int: ...


@redis_script
async def _cancel_cleanup(
    redis: RedisClient,
    *,
    waiters_stream: Key[str],
    progress_key: Key[str],
    runs_key: Key[str],
    waiter_entry_id: Arg[str],
) -> None: ...


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
        park_state_payload = json.dumps(
            {"type": "state", "key": execution.key, "state": "scheduled"}
        )
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
                state_payload=park_state_payload,
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
        async with docket._pubsub() as pubsub:
            try:
                await pubsub.psubscribe(pattern)
                async for message in pubsub.listen():  # pragma: no branch
                    if message.get("type") != "pmessage":
                        continue
                    data = message.get("data")
                    if isinstance(data, bytes):
                        task_key = data.decode()
                    elif isinstance(data, str):  # pragma: no cover
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
            waiter_stream_b = await redis.hget(runs_key, "waiter_stream")
            waiter_entry_id_b = await redis.hget(runs_key, "waiter_entry_id")
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
