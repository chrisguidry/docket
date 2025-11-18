import asyncio
import importlib
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import TracebackType
from typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Collection,
    Hashable,
    Iterable,
    Mapping,
    NoReturn,
    ParamSpec,
    Protocol,
    Sequence,
    TypedDict,
    TypeVar,
    cast,
    overload,
)

from key_value.aio.stores.base import BaseContextManagerStore
from typing_extensions import Self

import redis.exceptions
from opentelemetry import trace
from redis.asyncio import ConnectionPool, Redis
from ._uuid7 import uuid7

from .execution import (
    Execution,
    ExecutionState,
    LiteralOperator,
    Operator,
    Restore,
    Strike,
    StrikeInstruction,
    StrikeList,
    TaskFunction,
)
from key_value.aio.protocols.key_value import AsyncKeyValue
from key_value.aio.stores.redis import RedisStore
from key_value.aio.stores.memory import MemoryStore

from .instrumentation import (
    REDIS_DISRUPTIONS,
    STRIKES_IN_EFFECT,
    TASKS_ADDED,
    TASKS_CANCELLED,
    TASKS_REPLACED,
    TASKS_SCHEDULED,
    TASKS_STRICKEN,
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


@dataclass
class WorkerInfo:
    name: str
    last_seen: datetime
    tasks: set[str]


class RunningExecution(Execution):
    worker: str
    started: datetime

    def __init__(
        self,
        execution: Execution,
        worker: str,
        started: datetime,
    ) -> None:
        # Call parent constructor to properly initialize immutable fields
        super().__init__(
            docket=execution.docket,
            function=execution.function,
            args=execution.args,
            kwargs=execution.kwargs,
            key=execution.key,
            when=execution.when,
            attempt=execution.attempt,
            trace_context=execution.trace_context,
            redelivered=execution.redelivered,
        )
        # Copy over mutable state fields
        self.state: ExecutionState = execution.state
        self.started_at: datetime | None = execution.started_at
        self.completed_at: datetime | None = execution.completed_at
        self.error: str | None = execution.error
        self.result_key: str | None = execution.result_key
        # Set RunningExecution-specific fields
        self.worker = worker
        self.started = started


@dataclass
class DocketSnapshot:
    taken: datetime
    total_tasks: int
    future: Sequence[Execution]
    running: Sequence[RunningExecution]
    workers: Collection[WorkerInfo]


class Docket:
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

    _monitor_strikes_task: asyncio.Task[None]
    _connection_pool: ConnectionPool
    _cancel_task_script: _cancel_task | None

    def __init__(
        self,
        name: str = "docket",
        url: str = "redis://localhost:6379/0",
        heartbeat_interval: timedelta = timedelta(seconds=2),
        missed_heartbeats: int = 5,
        execution_ttl: timedelta = timedelta(minutes=15),
        result_storage: AsyncKeyValue | None = None,
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
        """
        self.name = name
        self.url = url
        self.heartbeat_interval = heartbeat_interval
        self.missed_heartbeats = missed_heartbeats
        self.execution_ttl = execution_ttl
        self._cancel_task_script = None

        self.result_storage: AsyncKeyValue
        if url.startswith("memory://"):
            self.result_storage = MemoryStore()
        else:
            self.result_storage = RedisStore(
                url=url, default_collection=f"{name}:results"
            )

    @property
    def worker_group_name(self) -> str:
        return "docket-workers"

    async def __aenter__(self) -> Self:
        from .tasks import standard_tasks

        self.tasks = {fn.__name__: fn for fn in standard_tasks}
        self.strike_list = StrikeList()

        # Check if we should use in-memory backend (fakeredis)
        # Support memory:// URLs for in-memory dockets
        if self.url.startswith("memory://"):
            try:
                from fakeredis.aioredis import FakeConnection, FakeServer

                # All memory:// URLs share a single FakeServer instance
                # Multiple dockets with different names are isolated by Redis key prefixes
                # (e.g., docket1:stream vs docket2:stream)
                if not hasattr(Docket, "_memory_server"):
                    Docket._memory_server = FakeServer()  # type: ignore

                server = Docket._memory_server  # type: ignore
                self._connection_pool = ConnectionPool(
                    connection_class=FakeConnection, server=server
                )
            except ImportError as e:
                raise ImportError(
                    "fakeredis is required for memory:// URLs. "
                    "Install with: pip install pydocket[memory]"
                ) from e
        else:
            self._connection_pool = ConnectionPool.from_url(self.url)  # type: ignore

        self._monitor_strikes_task = asyncio.create_task(self._monitor_strikes())

        # Ensure that the stream and worker group exist
        try:
            async with self.redis() as r:
                await r.xgroup_create(
                    groupname=self.worker_group_name,
                    name=self.stream_key,
                    id="0-0",
                    mkstream=True,
                )
        except redis.exceptions.RedisError as e:
            if "BUSYGROUP" not in repr(e):
                raise

        if isinstance(self.result_storage, BaseContextManagerStore):
            await self.result_storage.__aenter__()
        else:
            await self.result_storage.setup()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if isinstance(self.result_storage, BaseContextManagerStore):
            await self.result_storage.__aexit__(exc_type, exc_value, traceback)

        del self.tasks
        del self.strike_list

        self._monitor_strikes_task.cancel()
        try:
            await self._monitor_strikes_task
        except asyncio.CancelledError:
            pass

        await asyncio.shield(self._connection_pool.disconnect())
        del self._connection_pool

    @asynccontextmanager
    async def redis(self) -> AsyncGenerator[Redis, None]:
        r = Redis(connection_pool=self._connection_pool)
        await r.__aenter__()
        try:
            yield r
        finally:
            await asyncio.shield(r.__aexit__(None, None, None))

    def register(self, function: TaskFunction) -> None:
        """Register a task with the Docket.

        Args:
            function: The task to register.
        """
        from .dependencies import validate_dependencies

        validate_dependencies(function)

        self.tasks[function.__name__] = function

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
        if isinstance(function, str):
            function = self.tasks[function]
        else:
            self.register(function)

        if when is None:
            when = datetime.now(timezone.utc)

        if key is None:
            key = str(uuid7())

        async def scheduler(*args: P.args, **kwargs: P.kwargs) -> Execution:
            execution = Execution(self, function, args, kwargs, key, when, attempt=1)

            # Check if task is stricken before scheduling
            if self.strike_list.is_stricken(execution):
                logger.warning(
                    "%r is stricken, skipping schedule of %r",
                    execution.function.__name__,
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
        if isinstance(function, str):
            function = self.tasks[function]

        async def scheduler(*args: P.args, **kwargs: P.kwargs) -> Execution:
            execution = Execution(self, function, args, kwargs, key, when, attempt=1)

            # Check if task is stricken before scheduling
            if self.strike_list.is_stricken(execution):
                logger.warning(
                    "%r is stricken, skipping schedule of %r",
                    execution.function.__name__,
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
                "code.function.name": execution.function.__name__,
            },
        ):
            # Check if task is stricken before scheduling
            if self.strike_list.is_stricken(execution):
                logger.warning(
                    "%r is stricken, skipping schedule of %r",
                    execution.function.__name__,
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

        Args:
            key: The key of the task to cancel.
        """
        with tracer.start_as_current_span(
            "docket.cancel",
            attributes={**self.labels(), "docket.key": key},
        ):
            async with self.redis() as redis:
                await self._cancel(redis, key)

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
            runs_key = f"{self.name}:runs:{key}"
            data = await redis.hgetall(runs_key)

            if not data:
                return None

            # Extract task definition from runs hash
            function_name = data.get(b"function")
            args_data = data.get(b"args")
            kwargs_data = data.get(b"kwargs")

            # TODO: Remove in next breaking release (v0.14.0) - fallback for 0.13.0 compatibility
            # Check parked hash if runs hash incomplete (0.13.0 didn't store task data in runs hash)
            if not function_name or not args_data or not kwargs_data:
                parked_key = self.parked_task_key(key)
                parked_data = await redis.hgetall(parked_key)
                if parked_data:
                    function_name = parked_data.get(b"function")
                    args_data = parked_data.get(b"args")
                    kwargs_data = parked_data.get(b"kwargs")

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
            if not when_str:
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
        return f"{self.name}:queue"

    @property
    def stream_key(self) -> str:
        return f"{self.name}:stream"

    def known_task_key(self, key: str) -> str:
        return f"{self.name}:known:{key}"

    def parked_task_key(self, key: str) -> str:
        return f"{self.name}:{key}"

    def stream_id_key(self, key: str) -> str:
        return f"{self.name}:stream-id:{key}"

    async def _cancel(self, redis: Redis, key: str) -> None:
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

                    -- Create tombstone: set CANCELLED state with completed_at timestamp
                    redis.call('HSET', runs_key, 'state', 'cancelled', 'completed_at', completed_at)

                    return 'OK'
                    """
                ),
            )
        cancel_task = self._cancel_task_script

        # Create tombstone with CANCELLED state
        completed_at = datetime.now(timezone.utc).isoformat()
        runs_key = f"{self.name}:runs:{key}"

        # Execute the cancellation script
        await cancel_task(
            keys=[
                self.stream_key,
                self.known_task_key(key),
                self.parked_task_key(key),
                self.queue_key,
                self.stream_id_key(key),
                runs_key,
            ],
            args=[key, completed_at],
        )

        # Apply TTL or delete tombstone based on execution_ttl
        if self.execution_ttl:
            ttl_seconds = int(self.execution_ttl.total_seconds())
            await redis.expire(runs_key, ttl_seconds)
        else:
            # execution_ttl=0 means no observability - delete tombstone immediately
            await redis.delete(runs_key)

    @property
    def strike_key(self) -> str:
        return f"{self.name}:strikes"

    async def strike(
        self,
        function: Callable[P, Awaitable[R]] | str | None = None,
        parameter: str | None = None,
        operator: Operator | LiteralOperator = "==",
        value: Hashable | None = None,
    ) -> None:
        """Strike a task from the Docket.

        Args:
            function: The task to strike.
            parameter: The parameter to strike on.
            operator: The operator to use.
            value: The value to strike on.
        """
        if not isinstance(function, (str, type(None))):
            function = function.__name__

        operator = Operator(operator)

        strike = Strike(function, parameter, operator, value)
        return await self._send_strike_instruction(strike)

    async def restore(
        self,
        function: Callable[P, Awaitable[R]] | str | None = None,
        parameter: str | None = None,
        operator: Operator | LiteralOperator = "==",
        value: Hashable | None = None,
    ) -> None:
        """Restore a previously stricken task to the Docket.

        Args:
            function: The task to restore.
            parameter: The parameter to restore on.
            operator: The operator to use.
            value: The value to restore on.
        """
        if not isinstance(function, (str, type(None))):
            function = function.__name__

        operator = Operator(operator)

        restore = Restore(function, parameter, operator, value)
        return await self._send_strike_instruction(restore)

    async def _send_strike_instruction(self, instruction: StrikeInstruction) -> None:
        with tracer.start_as_current_span(
            f"docket.{instruction.direction}",
            attributes={
                **self.labels(),
                **instruction.labels(),
            },
        ):
            async with self.redis() as redis:
                message = instruction.as_message()
                await redis.xadd(self.strike_key, message)  # type: ignore[arg-type]
            self.strike_list.update(instruction)

    async def _monitor_strikes(self) -> NoReturn:
        last_id = "0-0"
        while True:
            try:
                async with self.redis() as r:
                    while True:
                        streams: RedisReadGroupResponse = await r.xread(
                            {self.strike_key: last_id},
                            count=100,
                            block=60_000,
                        )
                        for _, messages in streams:
                            for message_id, message in messages:
                                last_id = message_id
                                instruction = StrikeInstruction.from_message(message)
                                self.strike_list.update(instruction)
                                logger.info(
                                    "%s %r",
                                    (
                                        "Striking"
                                        if instruction.direction == "strike"
                                        else "Restoring"
                                    ),
                                    instruction.call_repr(),
                                    extra=self.labels(),
                                )

                                STRIKES_IN_EFFECT.add(
                                    1 if instruction.direction == "strike" else -1,
                                    {
                                        **self.labels(),
                                        **instruction.labels(),
                                    },
                                )

            except redis.exceptions.ConnectionError:  # pragma: no cover
                REDIS_DISRUPTIONS.add(1, {"docket": self.name})
                logger.warning("Connection error, sleeping for 1 second...")
                await asyncio.sleep(1)
            except Exception:  # pragma: no cover
                logger.exception("Error monitoring strikes")
                await asyncio.sleep(1)

    async def snapshot(self) -> DocketSnapshot:
        """Get a snapshot of the Docket, including which tasks are scheduled or currently
        running, as well as which workers are active.

        Returns:
            A snapshot of the Docket.
        """
        running: list[RunningExecution] = []
        future: list[Execution] = []

        async with self.redis() as r:
            async with r.pipeline() as pipeline:
                pipeline.xlen(self.stream_key)

                pipeline.zcard(self.queue_key)

                pipeline.xpending_range(
                    self.stream_key,
                    self.worker_group_name,
                    min="-",
                    max="+",
                    count=1000,
                )

                pipeline.xrange(self.stream_key, "-", "+", count=1000)

                pipeline.zrange(self.queue_key, 0, -1)

                total_stream_messages: int
                total_schedule_messages: int
                pending_messages: list[RedisStreamPendingMessage]
                stream_messages: list[tuple[RedisMessageID, RedisMessage]]
                scheduled_task_keys: list[bytes]

                now = datetime.now(timezone.utc)
                (
                    total_stream_messages,
                    total_schedule_messages,
                    pending_messages,
                    stream_messages,
                    scheduled_task_keys,
                ) = await pipeline.execute()

                for task_key in scheduled_task_keys:
                    pipeline.hgetall(self.parked_task_key(task_key.decode()))

                # Because these are two separate pipeline commands, it's possible that
                # a message has been moved from the schedule to the stream in the
                # meantime, which would end up being an empty `{}` message
                queued_messages: list[RedisMessage] = [
                    m for m in await pipeline.execute() if m
                ]

        total_tasks = total_stream_messages + total_schedule_messages

        pending_lookup: dict[RedisMessageID, RedisStreamPendingMessage] = {
            pending["message_id"]: pending for pending in pending_messages
        }

        for message_id, message in stream_messages:
            execution = await Execution.from_message(self, message)
            if message_id in pending_lookup:
                worker_name = pending_lookup[message_id]["consumer"].decode()
                started = now - timedelta(
                    milliseconds=pending_lookup[message_id]["time_since_delivered"]
                )
                running.append(RunningExecution(execution, worker_name, started))
            else:
                future.append(execution)  # pragma: no cover

        for message in queued_messages:
            execution = await Execution.from_message(self, message)
            future.append(execution)

        workers = await self.workers()

        return DocketSnapshot(now, total_tasks, future, running, workers)

    @property
    def workers_set(self) -> str:
        return f"{self.name}:workers"

    def worker_tasks_set(self, worker_name: str) -> str:
        return f"{self.name}:worker-tasks:{worker_name}"

    def task_workers_set(self, task_name: str) -> str:
        return f"{self.name}:task-workers:{task_name}"

    async def workers(self) -> Collection[WorkerInfo]:
        """Get a list of all workers that have sent heartbeats to the Docket.

        Returns:
            A list of all workers that have sent heartbeats to the Docket.
        """
        workers: list[WorkerInfo] = []

        oldest = datetime.now(timezone.utc).timestamp() - (
            self.heartbeat_interval.total_seconds() * self.missed_heartbeats
        )

        async with self.redis() as r:
            await r.zremrangebyscore(self.workers_set, 0, oldest)

            worker_name_bytes: bytes
            last_seen_timestamp: float

            for worker_name_bytes, last_seen_timestamp in await r.zrange(
                self.workers_set, 0, -1, withscores=True
            ):
                worker_name = worker_name_bytes.decode()
                last_seen = datetime.fromtimestamp(last_seen_timestamp, timezone.utc)

                task_names: set[str] = {
                    task_name_bytes.decode()
                    for task_name_bytes in cast(
                        set[bytes], await r.smembers(self.worker_tasks_set(worker_name))
                    )
                }

                workers.append(WorkerInfo(worker_name, last_seen, task_names))

        return workers

    async def task_workers(self, task_name: str) -> Collection[WorkerInfo]:
        """Get a list of all workers that are able to execute a given task.

        Args:
            task_name: The name of the task.

        Returns:
            A list of all workers that are able to execute the given task.
        """
        workers: list[WorkerInfo] = []
        oldest = datetime.now(timezone.utc).timestamp() - (
            self.heartbeat_interval.total_seconds() * self.missed_heartbeats
        )

        async with self.redis() as r:
            await r.zremrangebyscore(self.task_workers_set(task_name), 0, oldest)

            worker_name_bytes: bytes
            last_seen_timestamp: float

            for worker_name_bytes, last_seen_timestamp in await r.zrange(
                self.task_workers_set(task_name), 0, -1, withscores=True
            ):
                worker_name = worker_name_bytes.decode()
                last_seen = datetime.fromtimestamp(last_seen_timestamp, timezone.utc)

                task_names: set[str] = {
                    task_name_bytes.decode()
                    for task_name_bytes in cast(
                        set[bytes], await r.smembers(self.worker_tasks_set(worker_name))
                    )
                }

                workers.append(WorkerInfo(worker_name, last_seen, task_names))

        return workers

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
                        key = key_bytes.decode()
                        pipeline.delete(self.parked_task_key(key))
                        pipeline.delete(self.known_task_key(key))
                        pipeline.delete(self.stream_id_key(key))

                        # Handle runs hash: set TTL or delete based on execution_ttl
                        runs_key = f"{self.name}:runs:{key}"
                        if self.execution_ttl:
                            ttl_seconds = int(self.execution_ttl.total_seconds())
                            pipeline.expire(runs_key, ttl_seconds)
                        else:
                            pipeline.delete(runs_key)

                    # Handle runs hash for immediate tasks from stream
                    for key in stream_keys:
                        runs_key = f"{self.name}:runs:{key}"
                        if self.execution_ttl:
                            ttl_seconds = int(self.execution_ttl.total_seconds())
                            pipeline.expire(runs_key, ttl_seconds)
                        else:
                            pipeline.delete(runs_key)

                    await pipeline.execute()

                    total_cleared = stream_count + queue_count
                    return total_cleared
