import importlib
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import TracebackType
from typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Hashable,
    Iterable,
    Mapping,
    ParamSpec,
    TypeVar,
    overload,
)

import redis.exceptions
from key_value.aio.protocols.key_value import AsyncKeyValue
from opentelemetry import trace
from typing_extensions import Self

from ._docket_snapshot import DocketSnapshot as DocketSnapshot
from ._docket_snapshot import DocketSnapshotMixin
from ._docket_snapshot import RunningExecution as RunningExecution
from ._docket_snapshot import WorkerInfo as WorkerInfo
from ._lua import Arg, Key, redis_script
from ._redis import (
    PubSubClient,
    RedisClient,
    RedisConnection,
    RedisMessage as RedisMessage,
    RedisMessageID as RedisMessageID,
    RedisMessages as RedisMessages,
    RedisReadGroupResponse as RedisReadGroupResponse,
    RedisStream as RedisStream,
    RedisStreamID as RedisStreamID,
    RedisStreamPendingMessage as RedisStreamPendingMessage,
)
from ._result_store import ResultStorage
from ._uuid7 import uuid7
from .execution import (
    Disposition,
    Execution,
    TaskCall,
    TaskFunction,
    schedule_many,
)
from .instrumentation import (
    TASKS_ADDED,
    TASKS_CANCELLED,
    TASKS_REPLACED,
    TASKS_SCHEDULED,
    TASKS_STRICKEN,
)
from .strikelist import (
    LiteralOperator,
    Operator,
    Restore,
    Strike,
    StrikeList,
)

logger: logging.Logger = logging.getLogger(__name__)
tracer: trace.Tracer = trace.get_tracer(__name__)


@redis_script
async def _cancel_task(
    redis: RedisClient,
    *,
    stream_key: Key[str],
    known_key: Key[str],
    parked_key: Key[str],
    queue_key: Key[str],
    stream_id_key: Key[str],
    runs_key: Key[str],
    progress_key: Key[str],
    task_key: Arg[str],
    completed_at: Arg[str],
) -> bytes:
    """
    -- TODO: Remove known_key / parked_key / stream_id_key handling in
    -- v0.14.0 (legacy key locations).

    -- Get stream ID (check new location first, then legacy)
    local message_id = redis.call('HGET', runs_key, 'stream_id')

    -- TODO: Remove in next breaking release (v0.14.0) - check legacy location
    if not message_id then
        message_id = redis.call('GET', stream_id_key)
    end

    -- Delete from stream if message ID exists
    if message_id then
        redis.call('XDEL', stream_key, message_id)
    end

    -- Clean up legacy keys and parked data
    redis.call('DEL', known_key, parked_key, stream_id_key)
    redis.call('ZREM', queue_key, task_key)

    -- Drop the per-task progress hash that ``Execution.claim``
    -- creates -- without a TTL of its own, it would otherwise
    -- leak when a task is cancelled after being claimed but
    -- before it completes (e.g. parked on a side channel).
    redis.call('DEL', progress_key)

    -- Clear scheduling markers so add() can reschedule this key
    redis.call('HDEL', runs_key, 'known', 'stream_id')

    -- Only set CANCELLED if not already in a terminal state
    local current_state = redis.call('HGET', runs_key, 'state')
    if current_state ~= 'completed' and current_state ~= 'failed' and current_state ~= 'cancelled' then
        redis.call('HSET', runs_key, 'state', 'cancelled', 'completed_at', completed_at)
    end

    return 'OK'
    """
    ...


P = ParamSpec("P")
R = TypeVar("R")

TaskCollection = Iterable[TaskFunction]


class Docket(DocketSnapshotMixin):
    """A Docket represents a collection of tasks that may be scheduled for later
    execution.  With a Docket, you can add, replace, and cancel tasks.
    Example:

    ```python
    @task
    async def my_task(greeting: str, recipient: str) -> None:
        print(f"{greeting}, {recipient}!")

    async with Docket() as docket:
        docket.add(my_task)("Hello", recipient="world")
    ```
    """

    tasks: dict[str, TaskFunction]
    strike_list: StrikeList

    _redis: RedisConnection
    _result_storage: ResultStorage | None
    _stack: AsyncExitStack

    def __init__(
        self,
        name: str = "docket",
        url: str = "redis://localhost:6379/0",
        heartbeat_interval: timedelta = timedelta(seconds=2),
        missed_heartbeats: int = 5,
        execution_ttl: timedelta = timedelta(minutes=15),
        result_storage: AsyncKeyValue | None = None,
        enable_internal_instrumentation: bool = False,
    ) -> None:
        """
        Args:
            name: The name of the docket.
            url: The URL of the Redis server or in-memory backend.  For example:
                - "redis://localhost:6379/0"
                - "redis://user:password@localhost:6379/0"
                - "redis://user:password@localhost:6379/0?ssl=true"
                - "rediss://localhost:6379/0"
                - "redis+sentinel://sentinel-a:26379,sentinel-b:26379/mymaster/0"
                  (Redis Sentinel master discovery)
                - "unix:///path/to/redis.sock"
                - "memory://" (in-memory backend for testing)
            heartbeat_interval: How often workers send heartbeat messages to the docket.
            missed_heartbeats: How many heartbeats a worker can miss before it is
                considered dead.
            execution_ttl: How long to keep completed or failed execution state records
                in Redis before they expire. Defaults to 15 minutes.
            enable_internal_instrumentation: Whether to enable OpenTelemetry spans
                for internal Redis polling operations like strike stream monitoring.
                Defaults to False.
        """
        self.name = name
        self.url = url
        self.heartbeat_interval = heartbeat_interval
        self.missed_heartbeats = missed_heartbeats
        self.execution_ttl = execution_ttl
        self.enable_internal_instrumentation = enable_internal_instrumentation
        self._user_result_storage = result_storage
        self._redis = RedisConnection(url)

        from .tasks import standard_tasks

        self.tasks: dict[str, TaskFunction] = {fn.__name__: fn for fn in standard_tasks}

    @property
    def worker_group_name(self) -> str:
        return "docket-workers"

    @property
    def prefix(self) -> str:
        """Return the key prefix for this docket.

        All Redis keys for this docket are prefixed with this value.

        For Redis Cluster mode, returns a hash-tagged prefix like "{myapp}"
        to ensure all keys hash to the same slot.
        """
        return self._redis.prefix(self.name)

    def key(self, suffix: str) -> str:
        """Return a Redis key with the docket prefix.

        Args:
            suffix: The key suffix (e.g., "queue", "stream", "runs:task-123")

        Returns:
            Full Redis key like "docket:queue" or "docket:stream"
        """
        return f"{self.prefix}:{suffix}"

    async def __aenter__(self) -> Self:
        self._stack = AsyncExitStack()
        await self._stack.__aenter__()

        self.strike_list = StrikeList(
            url=self.url,
            name=self.name,
            enable_internal_instrumentation=self.enable_internal_instrumentation,
        )

        # Connect to Redis (handles cluster vs standalone)
        await self._stack.enter_async_context(self._redis)

        # Connect the strike list to Redis and start monitoring
        await self._stack.enter_async_context(self.strike_list)

        # Initialize result storage
        if self._user_result_storage is not None:
            self.result_storage: AsyncKeyValue = self._user_result_storage
            self._result_storage = None
            # User-provided storage should handle its own initialization
            if hasattr(self.result_storage, "setup"):
                await self.result_storage.setup()  # type: ignore[union-attr]
        else:
            self._result_storage = ResultStorage(self._redis, self.results_collection)
            await self._stack.enter_async_context(self._result_storage)
            self._stack.callback(lambda: setattr(self, "_result_storage", None))
            self.result_storage = self._result_storage
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self._stack.__aexit__(exc_type, exc_value, traceback)
        finally:
            del self._stack

    @asynccontextmanager
    async def redis(self) -> AsyncGenerator[RedisClient, None]:
        async with self._redis.client() as r:
            yield r

    @asynccontextmanager
    async def _pubsub(self) -> AsyncGenerator[PubSubClient, None]:
        async with self._redis.pubsub() as pubsub:
            yield pubsub

    async def _publish(self, channel: str, message: str) -> int:
        """Publish a message to a pub/sub channel.

        This handles both standalone and cluster modes transparently.

        Args:
            channel: The pub/sub channel to publish to
            message: The message to publish

        Returns:
            Number of subscribers that received the message
        """
        return await self._redis.publish(channel, message)

    def register(self, function: TaskFunction, names: list[str] | None = None) -> None:
        """Register a task with the Docket.

        Args:
            function: The task to register.
            names: Names to register the task under. Defaults to [function.__name__].
        """
        from .dependencies import validate_dependencies

        validate_dependencies(function)

        if not names:
            names = [function.__name__]

        for name in names:
            self.tasks[name] = function

    def register_collection(self, collection_path: str) -> None:
        """
        Register a collection of tasks.

        Args:
            collection_path: A path in the format "module:collection".
        """
        module_name, _, member_name = collection_path.rpartition(":")
        module = importlib.import_module(module_name)
        collection = getattr(module, member_name)
        for function in collection:
            self.register(function)

    def labels(self) -> Mapping[str, str]:
        return {
            "docket.name": self.name,
        }

    @overload
    def add(
        self,
        function: Callable[P, Awaitable[R]],
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[P, Awaitable[Execution]]:
        """Add a task to the Docket.

        If a task with the same key is already scheduled or running, this is
        a no-op; use `replace()` to overwrite.

        Args:
            function: The task function to add.
            when: The time to schedule the task.
            key: The key to schedule the task under.
        """

    @overload
    def add(
        self,
        function: str,
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[..., Awaitable[Execution]]:
        """Add a task to the Docket.

        If a task with the same key is already scheduled or running, this is
        a no-op; use `replace()` to overwrite.

        Args:
            function: The name of a task to add.
            when: The time to schedule the task.
            key: The key to schedule the task under.
        """

    def add(
        self,
        function: Callable[P, Awaitable[R]] | str,
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[..., Awaitable[Execution]]:
        """Add a task to the Docket.

        If a task with the same key is already scheduled or running, this is
        a no-op; use `replace()` to overwrite.

        Args:
            function: The task to add.
            when: The time to schedule the task.
            key: The key to schedule the task under.

        Returns:
            A callable that, when invoked with the task's arguments, returns
            an :class:`Execution`. The returned execution's ``disposition``
            reports the outcome of the scheduling attempt:
            ``Disposition.SCHEDULED`` if the task was placed,
            ``Disposition.ALREADY_SCHEDULED`` if a task with the same key was
            already known and the prior schedule was preserved, or
            ``Disposition.STRUCK`` if a strike rule blocked the call.
        """
        function_name: str | None = None
        if isinstance(function, str):
            function_name = function
            function = self.tasks[function]
        else:
            self.register(function)

        if when is None:
            when = datetime.now(timezone.utc)

        if key is None:
            key = str(uuid7())

        async def scheduler(*args: P.args, **kwargs: P.kwargs) -> Execution:
            execution = Execution(
                self,
                function,
                args,
                kwargs,
                key,
                when,
                attempt=1,
                function_name=function_name,
            )

            with tracer.start_as_current_span(
                "docket.add",
                attributes={
                    **self.labels(),
                    **execution.specific_labels(),
                    "code.function.name": execution.function_name,
                },
            ) as span:
                if self._check_stricken(execution):
                    span.set_attribute(
                        "docket.disposition", execution.disposition.value
                    )
                    return execution

                # Schedule atomically (includes state record write)
                await execution.schedule(replace=False)
                span.set_attribute("docket.disposition", execution.disposition.value)

            self._record_schedule_metrics(execution, replace=False)

            return execution

        return scheduler

    @overload
    def replace(
        self,
        function: Callable[P, Awaitable[R]],
        when: datetime,
        key: str,
    ) -> Callable[P, Awaitable[Execution]]:
        """Replace a previously scheduled task on the Docket.

        Args:
            function: The task function to replace.
            when: The time to schedule the task.
            key: The key to schedule the task under.
        """

    @overload
    def replace(
        self,
        function: str,
        when: datetime,
        key: str,
    ) -> Callable[..., Awaitable[Execution]]:
        """Replace a previously scheduled task on the Docket.

        Args:
            function: The name of a task to replace.
            when: The time to schedule the task.
            key: The key to schedule the task under.
        """

    def replace(
        self,
        function: Callable[P, Awaitable[R]] | str,
        when: datetime,
        key: str,
    ) -> Callable[..., Awaitable[Execution]]:
        """Replace a previously scheduled task on the Docket.

        Args:
            function: The task to replace.
            when: The time to schedule the task.
            key: The key to schedule the task under.

        Returns:
            A callable that, when invoked with the task's arguments, returns
            an :class:`Execution`. The returned execution's ``disposition`` is
            ``Disposition.SCHEDULED`` if the task was placed (overwriting any
            prior schedule for ``key``), or ``Disposition.STRUCK`` if a strike
            rule blocked the call.
        """
        function_name: str | None = None
        if isinstance(function, str):
            function_name = function
            function = self.tasks[function]
        else:
            self.register(function)

        async def scheduler(*args: P.args, **kwargs: P.kwargs) -> Execution:
            execution = Execution(
                self,
                function,
                args,
                kwargs,
                key,
                when,
                attempt=1,
                function_name=function_name,
            )

            with tracer.start_as_current_span(
                "docket.replace",
                attributes={
                    **self.labels(),
                    **execution.specific_labels(),
                    "code.function.name": execution.function_name,
                },
            ) as span:
                if self._check_stricken(execution):
                    span.set_attribute(
                        "docket.disposition", execution.disposition.value
                    )
                    return execution

                # Schedule atomically (includes state record write)
                await execution.schedule(replace=True)
                span.set_attribute("docket.disposition", execution.disposition.value)

            self._record_schedule_metrics(execution, replace=True)

            return execution

        return scheduler

    @overload
    def call(
        self,
        function: Callable[P, Awaitable[R]],
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[P, TaskCall]:
        """Build a TaskCall for batch scheduling.

        Args:
            function: The task function to call.
            when: The time to schedule the task.
            key: The key to schedule the task under.
        """

    @overload
    def call(
        self,
        function: str,
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[..., TaskCall]:
        """Build a TaskCall for batch scheduling.

        Args:
            function: The name of a task to call.
            when: The time to schedule the task.
            key: The key to schedule the task under.
        """

    def call(
        self,
        function: Callable[P, Awaitable[R]] | str,
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[..., TaskCall]:
        """Build a :class:`TaskCall` for :meth:`add_many` / :meth:`replace_many`.

        Mirrors the currying of :meth:`add` and :meth:`replace`, but instead
        of scheduling immediately, the returned callable captures the task's
        arguments into a ``TaskCall`` spec.  Nothing touches Redis until the
        spec is passed to a batch method.

        Args:
            function: The task (or registered task name) to call.
            when: The time to schedule the task.  Defaults to now.
            key: The key to schedule the task under.  Defaults to a fresh
                uuid7, exactly like :meth:`add`.

        Returns:
            A callable that, when invoked with the task's arguments, returns
            a ``TaskCall``.

        Raises:
            KeyError: If ``function`` is a name that isn't registered.
        """
        function_name: str | None = None
        if isinstance(function, str):
            function_name = function
            function = self.tasks[function]
        else:
            self.register(function)

        if when is None:
            when = datetime.now(timezone.utc)

        if key is None:
            key = str(uuid7())

        def specifier(*args: P.args, **kwargs: P.kwargs) -> TaskCall:
            return TaskCall(
                function=function,
                args=args,
                kwargs=kwargs,
                key=key,
                when=when,
                function_name=function_name,
            )

        return specifier

    async def add_many(
        self, calls: Iterable[TaskCall], *, chunk_size: int | None = 1000
    ) -> list[Execution]:
        """Add a batch of tasks to the Docket in pipelined round-trips.

        Semantically equivalent to awaiting :meth:`add` for each
        :class:`TaskCall`, but schedules travel to Redis in pipelined chunks
        of ``chunk_size``, so a batch of N costs O(N / chunk_size)
        round-trips instead of O(N).  Per-execution semantics are unchanged:
        each task is individually checked against the strike list,
        deduplicated by key (an already-known key preserves its prior
        schedule), and scheduled atomically.  There is no atomicity across
        the batch, and a Redis error for one task marks only that execution
        failed.

        Args:
            calls: TaskCall specs built with :meth:`call`.
            chunk_size: How many schedules to buffer client-side and send
                per round-trip; ``None`` sends the whole batch as one
                pipeline (fastest, but buffers every task's payload in
                memory at once).

        Returns:
            One :class:`Execution` per call, in input order.  Each
            execution's ``disposition`` reports its own outcome:
            ``Disposition.SCHEDULED``, ``Disposition.ALREADY_SCHEDULED``,
            ``Disposition.STRUCK``, or ``Disposition.FAILED`` (with the
            error attached as ``Execution.schedule_exception``).
        """
        return await self._schedule_many(calls, replace=False, chunk_size=chunk_size)

    async def replace_many(
        self, calls: Iterable[TaskCall], *, chunk_size: int | None = 1000
    ) -> list[Execution]:
        """Replace a batch of tasks on the Docket in pipelined round-trips.

        Semantically equivalent to awaiting :meth:`replace` for each
        :class:`TaskCall`, but schedules travel to Redis in pipelined chunks
        of ``chunk_size``, so a batch of N costs O(N / chunk_size)
        round-trips instead of O(N).  Each task's prior schedule (if any) is
        cancelled and overwritten individually and atomically; there is no
        atomicity across the batch, and a Redis error for one task marks
        only that execution failed.

        Args:
            calls: TaskCall specs built with :meth:`call`.  Pass an explicit
                ``key`` to :meth:`call` to target the schedule to replace.
            chunk_size: How many schedules to buffer client-side and send
                per round-trip; ``None`` sends the whole batch as one
                pipeline (fastest, but buffers every task's payload in
                memory at once).

        Returns:
            One :class:`Execution` per call, in input order, with
            ``disposition`` set to ``Disposition.SCHEDULED``,
            ``Disposition.STRUCK``, or ``Disposition.FAILED`` (with the
            error attached as ``Execution.schedule_exception``).
        """
        return await self._schedule_many(calls, replace=True, chunk_size=chunk_size)

    def _check_stricken(self, execution: Execution) -> bool:
        """Mark and count an execution blocked by a strike rule.

        Shared by every scheduling path (``add``, ``replace``, ``schedule``,
        and the batch methods) so the warning, the ``TASKS_STRICKEN``
        counter, and the ``STRUCK`` disposition stay identical everywhere.
        Returns True when the execution was blocked.
        """
        if not self.strike_list.is_stricken(execution):
            return False

        logger.warning(
            "%r is stricken, skipping schedule of %r",
            execution.function_name,
            execution.key,
        )
        TASKS_STRICKEN.add(
            1,
            {
                **self.labels(),
                **execution.general_labels(),
                "docket.where": "docket",
            },
        )
        execution.disposition = Disposition.STRUCK
        return True

    def _record_schedule_metrics(self, execution: Execution, *, replace: bool) -> None:
        """Increment the scheduling counters for one non-stricken execution,
        exactly as the single-call ``add`` / ``replace`` paths always have.

        Executions whose Redis command failed during a batch don't count:
        nothing was added, replaced, or scheduled for them.
        """
        if execution.disposition is Disposition.FAILED:
            return

        labels = {**self.labels(), **execution.general_labels()}
        if replace:
            TASKS_REPLACED.add(1, labels)
            TASKS_CANCELLED.add(1, labels)
            TASKS_SCHEDULED.add(1, labels)
        else:
            TASKS_ADDED.add(1, labels)
            if execution.disposition is Disposition.SCHEDULED:
                TASKS_SCHEDULED.add(1, labels)

    async def _schedule_many(
        self, calls: Iterable[TaskCall], *, replace: bool, chunk_size: int | None
    ) -> list[Execution]:
        # Validate here as well as in schedule_many(), so a bad chunk_size is
        # rejected even when the batch is empty or entirely stricken and
        # nothing ever reaches Redis.
        if chunk_size is not None and chunk_size < 1:
            raise ValueError(f"chunk_size must be at least 1, got {chunk_size}")

        executions = [
            Execution(
                self,
                task_call.function,
                task_call.args,
                task_call.kwargs,
                task_call.key,
                task_call.when,
                attempt=1,
                function_name=task_call.function_name,
            )
            for task_call in calls
        ]

        span_name = "docket.replace_many" if replace else "docket.add_many"
        with tracer.start_as_current_span(
            span_name,
            attributes={**self.labels(), "docket.batch.count": len(executions)},
        ) as span:
            to_schedule = [
                execution
                for execution in executions
                if not self._check_stricken(execution)
            ]

            if to_schedule:
                async with self.redis() as redis:
                    await schedule_many(
                        redis, to_schedule, replace=replace, chunk_size=chunk_size
                    )

            span.set_attribute(
                "docket.batch.stricken", len(executions) - len(to_schedule)
            )

        for execution in executions:
            if execution.disposition is not Disposition.STRUCK:
                self._record_schedule_metrics(execution, replace=replace)

        return executions

    async def schedule(self, execution: Execution) -> None:
        with tracer.start_as_current_span(
            "docket.schedule",
            attributes={
                **self.labels(),
                **execution.specific_labels(),
                "code.function.name": execution.function_name,
            },
        ) as span:
            if self._check_stricken(execution):
                span.set_attribute("docket.disposition", execution.disposition.value)
                return

            # Schedule atomically (includes state record write)
            await execution.schedule(replace=False)
            span.set_attribute("docket.disposition", execution.disposition.value)

        if execution.disposition is Disposition.SCHEDULED:
            TASKS_SCHEDULED.add(1, {**self.labels(), **execution.general_labels()})

    async def cancel(self, key: str) -> None:
        """Cancel a previously scheduled task on the Docket.

        If the task is scheduled (in the queue or stream), it will be removed.
        If the task is currently running, a cancellation signal will be sent
        to the worker, which will attempt to cancel the asyncio task. This is
        best-effort: if the task completes before the signal is processed,
        the cancellation will have no effect.

        Args:
            key: The key of the task to cancel.
        """
        with tracer.start_as_current_span(
            "docket.cancel",
            attributes={**self.labels(), "docket.key": key},
        ):
            async with self.redis() as redis:
                await self._cancel(redis, key)

            # Publish cancellation signal for running tasks (best-effort)
            await self._publish(self.cancel_channel(key), key)

        TASKS_CANCELLED.add(1, self.labels())

    async def get_execution(self, key: str) -> Execution | None:
        """Get a task Execution from the Docket by its key.

        Args:
            key: The task key.

        Returns:
            The Execution if found, None if the key doesn't exist.

        Example:
            # Claim check pattern: schedule a task, save the key,
            # then retrieve the execution later to check status or get results
            execution = await docket.add(my_task, key="important-task")(args)
            task_key = execution.key

            # Later, retrieve the execution by key
            execution = await docket.get_execution(task_key)
            if execution:
                await execution.get_result()
        """
        import cloudpickle

        async with self.redis() as redis:
            data = await redis.hgetall(self.runs_key(key))

            if not data:
                return None

            # Extract task definition from runs hash
            function_name = data.get(b"function")
            args_data = data.get(b"args")
            kwargs_data = data.get(b"kwargs")

            if not function_name or not args_data or not kwargs_data:
                return None

            # Look up function in registry, or create a placeholder if not found
            function_name_str = function_name.decode()
            function = self.tasks.get(function_name_str)
            if not function:
                # Create a placeholder function for display purposes (e.g., CLI watch)
                # This allows viewing task state even if function isn't registered
                async def placeholder() -> None:
                    pass  # pragma: no cover

                placeholder.__name__ = function_name_str
                function = placeholder

            # Deserialize args and kwargs
            args = cloudpickle.loads(args_data)
            kwargs = cloudpickle.loads(kwargs_data)

            # Extract scheduling metadata
            when_str = data.get(b"when")
            if not when_str:  # pragma: no cover
                return None
            when = datetime.fromtimestamp(float(when_str.decode()), tz=timezone.utc)

            # Build execution (attempt defaults to 1 for initial scheduling)
            from docket.execution import Execution

            execution = Execution(
                docket=self,
                function=function,
                args=args,
                kwargs=kwargs,
                key=key,
                when=when,
                attempt=1,
            )

            # Sync with current state from Redis
            await execution.sync()

            return execution

    @property
    def queue_key(self) -> str:
        return self.key("queue")

    @property
    def stream_key(self) -> str:
        return self.key("stream")

    def known_task_key(self, task_key: str) -> str:
        return self.key(f"known:{task_key}")

    def parked_task_key(self, task_key: str) -> str:
        return self.key(task_key)

    def stream_id_key(self, task_key: str) -> str:
        return self.key(f"stream-id:{task_key}")

    def runs_key(self, task_key: str) -> str:
        """Return the Redis key for storing execution state for a task."""
        return self.key(f"runs:{task_key}")

    def cancel_channel(self, task_key: str) -> str:
        """Return the Redis pub/sub channel for cancellation signals for a task."""
        return self.key(f"cancel:{task_key}")

    @property
    def results_collection(self) -> str:
        """Return the collection name for result storage."""
        return self.key("results")

    async def _ensure_stream_and_group(self) -> None:
        """Create stream and consumer group if they don't exist (idempotent).

        This is safe to call from multiple workers racing to initialize - the
        BUSYGROUP error is silently ignored since it just means another worker
        created the group first.
        """
        try:
            async with self.redis() as r:
                await r.xgroup_create(
                    groupname=self.worker_group_name,
                    name=self.stream_key,
                    id="0-0",
                    mkstream=True,
                )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise  # pragma: no cover

    async def _cancel(self, redis: RedisClient, key: str) -> None:
        """Cancel a task atomically.

        Handles cancellation regardless of task location:
        - From the stream (using stored message ID)
        - From the queue (scheduled tasks)
        - Cleans up all associated metadata keys

        Dependencies that park tasks on side channels (e.g. ConcurrencyLimit's
        waiter streams) clean up via the state-transition pub/sub channel
        published by ``Docket.cancel`` -- Docket itself stays unaware of any
        dependency-specific storage.
        """
        # Create tombstone with CANCELLED state
        completed_at = datetime.now(timezone.utc).isoformat()
        task_runs_key = self.runs_key(key)

        # Execute the cancellation script
        await _cancel_task(
            redis,
            stream_key=self.stream_key,
            known_key=self.known_task_key(key),
            parked_key=self.parked_task_key(key),
            queue_key=self.queue_key,
            stream_id_key=self.stream_id_key(key),
            runs_key=task_runs_key,
            progress_key=self.key(f"progress:{key}"),
            task_key=key,
            completed_at=completed_at,
        )

        # Apply TTL or delete tombstone based on execution_ttl
        if self.execution_ttl:
            ttl_seconds = int(self.execution_ttl.total_seconds())
            await redis.expire(task_runs_key, ttl_seconds)
        else:
            # execution_ttl=0 means no observability - delete tombstone immediately
            await redis.delete(task_runs_key)

    async def strike(
        self,
        function: Callable[P, Awaitable[R]] | str | None = None,
        parameter: str | None = None,
        operator: Operator | LiteralOperator = "==",
        value: Hashable | None = None,
    ) -> None:
        """Strike a task from the Docket.

        Args:
            function: The task to strike (function or name), or None for all tasks.
            parameter: The parameter to strike on, or None for entire task.
            operator: The comparison operator to use.
            value: The value to strike on.
        """
        function_name = function.__name__ if callable(function) else function

        instruction = Strike(function_name, parameter, Operator(operator), value)
        with tracer.start_as_current_span(
            "docket.strike",
            attributes={**self.labels(), **instruction.labels()},
        ):
            await self.strike_list.send_instruction(instruction)

    async def restore(
        self,
        function: Callable[P, Awaitable[R]] | str | None = None,
        parameter: str | None = None,
        operator: Operator | LiteralOperator = "==",
        value: Hashable | None = None,
    ) -> None:
        """Restore a previously stricken task to the Docket.

        Args:
            function: The task to restore (function or name), or None for all tasks.
            parameter: The parameter to restore on, or None for entire task.
            operator: The comparison operator to use.
            value: The value to restore on.
        """
        function_name = function.__name__ if callable(function) else function

        instruction = Restore(function_name, parameter, Operator(operator), value)
        with tracer.start_as_current_span(
            "docket.restore",
            attributes={**self.labels(), **instruction.labels()},
        ):
            await self.strike_list.send_instruction(instruction)

    async def wait_for_strikes_loaded(self) -> None:
        """Wait for all existing strikes to be loaded from the stream.

        This method blocks until the strike monitor has completed its initial
        non-blocking read of all existing strike messages. Call this before
        making decisions that depend on the current strike state, such as
        scheduling automatic perpetual tasks.
        """
        await self.strike_list.wait_for_strikes_loaded()

    async def clear(self) -> int:
        """Clear all queued and scheduled tasks from the docket.

        This removes all tasks from the stream (immediate tasks) and queue
        (scheduled tasks), along with their associated parked data. Running
        tasks are not affected.

        Returns:
            The total number of tasks that were cleared.
        """
        with tracer.start_as_current_span(
            "docket.clear",
            attributes=self.labels(),
        ):
            async with self.redis() as redis:
                async with redis.pipeline() as pipeline:
                    # Get counts before clearing
                    pipeline.xlen(self.stream_key)
                    pipeline.zcard(self.queue_key)
                    pipeline.zrange(self.queue_key, 0, -1)

                    stream_count: int
                    queue_count: int
                    scheduled_keys: list[bytes]
                    stream_count, queue_count, scheduled_keys = await pipeline.execute()

                # Get keys from stream messages before trimming
                stream_keys: list[str] = []
                if stream_count > 0:
                    # Read all messages from the stream
                    messages = await redis.xrange(self.stream_key, "-", "+")
                    for message_id, fields in messages:
                        # Extract the key field from the message
                        if b"key" in fields:  # pragma: no branch
                            stream_keys.append(fields[b"key"].decode())

                async with redis.pipeline() as pipeline:
                    # Clear all data
                    # Trim stream to 0 messages instead of deleting it to preserve consumer group
                    if stream_count > 0:
                        pipeline.xtrim(self.stream_key, maxlen=0, approximate=False)
                    pipeline.delete(self.queue_key)

                    # Clear parked task data and known task keys for scheduled tasks
                    for key_bytes in scheduled_keys:
                        task_key = key_bytes.decode()
                        pipeline.delete(self.parked_task_key(task_key))
                        pipeline.delete(self.known_task_key(task_key))
                        pipeline.delete(self.stream_id_key(task_key))

                        # Handle runs hash: set TTL or delete based on execution_ttl
                        task_runs_key = self.runs_key(task_key)
                        if self.execution_ttl:
                            ttl_seconds = int(self.execution_ttl.total_seconds())
                            pipeline.expire(task_runs_key, ttl_seconds)
                        else:
                            pipeline.delete(task_runs_key)

                    # Handle runs hash for immediate tasks from stream
                    for task_key in stream_keys:
                        task_runs_key = self.runs_key(task_key)
                        if self.execution_ttl:
                            ttl_seconds = int(self.execution_ttl.total_seconds())
                            pipeline.expire(task_runs_key, ttl_seconds)
                        else:
                            pipeline.delete(task_runs_key)

                    await pipeline.execute()

                    total_cleared = stream_count + queue_count
                    return total_cleared
