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
    Protocol,
    Sequence,
    TypedDict,
    TypeVar,
    cast,
    overload,
)

import redis.exceptions
from key_value.aio.protocols.key_value import AsyncKeyValue
from opentelemetry import trace
from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from redis.asyncio.cluster import RedisCluster
from typing_extensions import Self

from ._docket_snapshot import DocketSnapshot as DocketSnapshot
from ._docket_snapshot import DocketSnapshotMixin
from ._docket_snapshot import RunningExecution as RunningExecution
from ._docket_snapshot import WorkerInfo as WorkerInfo
from ._redis import RedisConnection
from ._result_store import ResultStorage
from ._uuid7 import uuid7
from .execution import (
    Execution,
    TaskFunction,
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


class _cancel_task(Protocol):
    async def __call__(
        self, keys: list[str], args: list[str]
    ) -> str: ...  # pragma: no cover


P = ParamSpec("P")
R = TypeVar("R")

TaskCollection = Iterable[TaskFunction]

RedisStreamID = bytes
RedisMessageID = bytes
RedisMessage = dict[bytes, bytes]
RedisMessages = Sequence[tuple[RedisMessageID, RedisMessage]]
RedisStream = tuple[RedisStreamID, RedisMessages]
RedisReadGroupResponse = Sequence[RedisStream]


class RedisStreamPendingMessage(TypedDict):
    message_id: bytes
    consumer: bytes
    time_since_delivered: int
    times_delivered: int


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
    _cancel_task_script: _cancel_task | None
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
        self._cancel_task_script = None
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
    async def redis(self) -> AsyncGenerator[Redis | RedisCluster, None]:
        async with self._redis.client() as r:
            yield r

    @asynccontextmanager
    async def _pubsub(self) -> AsyncGenerator[PubSub, None]:
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

        Args:
            function: The task to add.
            when: The time to schedule the task.
            key: The key to schedule the task under.
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
            ):
                # Check if task is stricken before scheduling
                if self.strike_list.is_stricken(execution):
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
                    return execution

                # Schedule atomically (includes state record write)
                await execution.schedule(replace=False)

            TASKS_ADDED.add(1, {**self.labels(), **execution.general_labels()})
            TASKS_SCHEDULED.add(1, {**self.labels(), **execution.general_labels()})

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
            ):
                # Check if task is stricken before scheduling
                if self.strike_list.is_stricken(execution):
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
                    return execution

                # Schedule atomically (includes state record write)
                await execution.schedule(replace=True)

            TASKS_REPLACED.add(1, {**self.labels(), **execution.general_labels()})
            TASKS_CANCELLED.add(1, {**self.labels(), **execution.general_labels()})
            TASKS_SCHEDULED.add(1, {**self.labels(), **execution.general_labels()})

            return execution

        return scheduler

    async def schedule(self, execution: Execution) -> None:
        with tracer.start_as_current_span(
            "docket.schedule",
            attributes={
                **self.labels(),
                **execution.specific_labels(),
                "code.function.name": execution.function_name,
            },
        ):
            # Check if task is stricken before scheduling
            if self.strike_list.is_stricken(execution):
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
                return

            # Schedule atomically (includes state record write)
            await execution.schedule(replace=False)

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

    async def _cancel(self, redis: Redis | RedisCluster, key: str) -> None:
        """Cancel a task atomically.

        Handles cancellation regardless of task location:
        - From the stream (using stored message ID)
        - From the queue (scheduled tasks)
        - Cleans up all associated metadata keys
        """
        if self._cancel_task_script is None:
            self._cancel_task_script = cast(
                _cancel_task,
                redis.register_script(
                    # KEYS: stream_key, known_key, parked_key, queue_key, stream_id_key, runs_key
                    # ARGV: task_key, completed_at
                    """
                    local stream_key = KEYS[1]
                    -- TODO: Remove in next breaking release (v0.14.0) - legacy key locations
                    local known_key = KEYS[2]
                    local parked_key = KEYS[3]
                    local queue_key = KEYS[4]
                    local stream_id_key = KEYS[5]
                    local runs_key = KEYS[6]
                    local task_key = ARGV[1]
                    local completed_at = ARGV[2]

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

                    -- Only set CANCELLED if not already in a terminal state
                    local current_state = redis.call('HGET', runs_key, 'state')
                    if current_state ~= 'completed' and current_state ~= 'failed' and current_state ~= 'cancelled' then
                        redis.call('HSET', runs_key, 'state', 'cancelled', 'completed_at', completed_at)
                    end

                    return 'OK'
                    """
                ),
            )
        cancel_task = self._cancel_task_script

        # Create tombstone with CANCELLED state
        completed_at = datetime.now(timezone.utc).isoformat()
        task_runs_key = self.runs_key(key)

        # Execute the cancellation script
        await cancel_task(
            keys=[
                self.stream_key,
                self.known_task_key(key),
                self.parked_task_key(key),
                self.queue_key,
                self.stream_id_key(key),
                task_runs_key,
            ],
            args=[key, completed_at],
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
