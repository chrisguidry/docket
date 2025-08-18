import asyncio
import importlib
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import TracebackType
from typing import (
    Any,
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
    Self,
    Sequence,
    TypedDict,
    TypeVar,
    cast,
    overload,
)

import redis.exceptions
from opentelemetry import propagate, trace
from redis.asyncio import ConnectionPool, Redis
from uuid_extensions import uuid7

from .execution import (
    Execution,
    LiteralOperator,
    Operator,
    Restore,
    Strike,
    StrikeInstruction,
    StrikeList,
    TaskFunction,
)
from .instrumentation import (
    REDIS_DISRUPTIONS,
    STRIKES_IN_EFFECT,
    TASKS_ADDED,
    TASKS_CANCELLED,
    TASKS_REPLACED,
    TASKS_SCHEDULED,
    TASKS_STRICKEN,
    message_setter,
)

logger: logging.Logger = logging.getLogger(__name__)
tracer: trace.Tracer = trace.get_tracer(__name__)


class _schedule_task(Protocol):
    async def __call__(
        self, keys: list[str], args: list[str | float | bytes]
    ) -> str: ...  # pragma: no cover


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
        self.function: TaskFunction = execution.function
        self.args: tuple[Any, ...] = execution.args
        self.kwargs: dict[str, Any] = execution.kwargs
        self.when: datetime = execution.when
        self.key: str = execution.key
        self.attempt: int = execution.attempt
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
    _schedule_task_script: _schedule_task | None
    _cancel_task_script: _cancel_task | None

    def __init__(
        self,
        name: str = "docket",
        url: str = "redis://localhost:6379/0",
        heartbeat_interval: timedelta = timedelta(seconds=2),
        missed_heartbeats: int = 5,
    ) -> None:
        """
        Args:
            name: The name of the docket.
            url: The URL of the Redis server.  For example:
                - "redis://localhost:6379/0"
                - "redis://user:password@localhost:6379/0"
                - "redis://user:password@localhost:6379/0?ssl=true"
                - "rediss://localhost:6379/0"
                - "unix:///path/to/redis.sock"
            heartbeat_interval: How often workers send heartbeat messages to the docket.
            missed_heartbeats: How many heartbeats a worker can miss before it is
                considered dead.
        """
        self.name = name
        self.url = url
        self.heartbeat_interval = heartbeat_interval
        self.missed_heartbeats = missed_heartbeats
        self._schedule_task_script = None
        self._cancel_task_script = None

    @property
    def worker_group_name(self) -> str:
        return "docket-workers"

    async def __aenter__(self) -> Self:
        from .tasks import standard_tasks

        self.tasks = {fn.__name__: fn for fn in standard_tasks}
        self.strike_list = StrikeList()

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

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
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
            execution = Execution(function, args, kwargs, when, key, attempt=1)

            async with self.redis() as redis:
                await self._schedule(redis, execution, replace=False)

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
            execution = Execution(function, args, kwargs, when, key, attempt=1)

            async with self.redis() as redis:
                await self._schedule(redis, execution, replace=True)

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
            async with self.redis() as redis:
                await self._schedule(redis, execution, replace=False)

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

    async def _schedule(
        self,
        redis: Redis,
        execution: Execution,
        replace: bool = False,
    ) -> None:
        """Schedule a task atomically.

        Handles:
        - Checking for task existence
        - Cancelling existing tasks when replacing
        - Adding tasks to stream (immediate) or queue (future)
        - Tracking stream message IDs for later cancellation
        """
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
                    **execution.specific_labels(),
                    "docket.where": "docket",
                },
            )
            return

        message: dict[bytes, bytes] = execution.as_message()
        propagate.inject(message, setter=message_setter)

        key = execution.key
        when = execution.when
        known_task_key = self.known_task_key(key)
        is_immediate = when <= datetime.now(timezone.utc)

        # Lock per task key to prevent race conditions between concurrent operations
        async with redis.lock(f"{known_task_key}:lock", timeout=10):
            if self._schedule_task_script is None:
                self._schedule_task_script = cast(
                    _schedule_task,
                    redis.register_script(
                        # KEYS: stream_key, known_key, parked_key, queue_key, stream_id_key
                        # ARGV: task_key, when_timestamp, is_immediate, replace, ...message_fields
                        """
                        local stream_key = KEYS[1]
                        local known_key = KEYS[2]
                        local parked_key = KEYS[3]
                        local queue_key = KEYS[4]
                        local stream_id_key = KEYS[5]

                        local task_key = ARGV[1]
                        local when_timestamp = ARGV[2]
                        local is_immediate = ARGV[3] == '1'
                        local replace = ARGV[4] == '1'

                        -- Extract message fields from ARGV[5] onwards
                        local message = {}
                        for i = 5, #ARGV, 2 do
                            message[#message + 1] = ARGV[i]     -- field name
                            message[#message + 1] = ARGV[i + 1] -- field value
                        end

                        -- Handle replacement: cancel existing task if needed
                        if replace then
                            local existing_message_id = redis.call('GET', stream_id_key)
                            if existing_message_id then
                                redis.call('XDEL', stream_key, existing_message_id)
                            end
                            redis.call('DEL', known_key, parked_key, stream_id_key)
                            redis.call('ZREM', queue_key, task_key)
                        else
                            -- Check if task already exists
                            if redis.call('EXISTS', known_key) == 1 then
                                return 'EXISTS'
                            end
                        end

                        if is_immediate then
                            -- Add to stream and store message ID for later cancellation
                            local message_id = redis.call('XADD', stream_key, '*', unpack(message))
                            redis.call('SET', known_key, when_timestamp)
                            redis.call('SET', stream_id_key, message_id)
                            return message_id
                        else
                            -- Add to queue with task data in parked hash
                            redis.call('SET', known_key, when_timestamp)
                            redis.call('HSET', parked_key, unpack(message))
                            redis.call('ZADD', queue_key, when_timestamp, task_key)
                            return 'QUEUED'
                        end
                        """
                    ),
                )
            schedule_task = self._schedule_task_script

            await schedule_task(
                keys=[
                    self.stream_key,
                    known_task_key,
                    self.parked_task_key(key),
                    self.queue_key,
                    self.stream_id_key(key),
                ],
                args=[
                    key,
                    str(when.timestamp()),
                    "1" if is_immediate else "0",
                    "1" if replace else "0",
                    *[
                        item
                        for field, value in message.items()
                        for item in (field, value)
                    ],
                ],
            )

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
                    # KEYS: stream_key, known_key, parked_key, queue_key, stream_id_key
                    # ARGV: task_key
                    """
                    local stream_key = KEYS[1]
                    local known_key = KEYS[2]
                    local parked_key = KEYS[3]
                    local queue_key = KEYS[4]
                    local stream_id_key = KEYS[5]
                    local task_key = ARGV[1]

                    -- Delete from stream if message ID exists
                    local message_id = redis.call('GET', stream_id_key)
                    if message_id then
                        redis.call('XDEL', stream_key, message_id)
                    end

                    -- Clean up all task-related keys
                    redis.call('DEL', known_key, parked_key, stream_id_key)
                    redis.call('ZREM', queue_key, task_key)

                    return 'OK'
                    """
                ),
            )
        cancel_task = self._cancel_task_script

        # Execute the cancellation script
        await cancel_task(
            keys=[
                self.stream_key,
                self.known_task_key(key),
                self.parked_task_key(key),
                self.queue_key,
                self.stream_id_key(key),
            ],
            args=[key],
        )

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
            function = self.tasks[message[b"function"].decode()]
            execution = Execution.from_message(function, message)
            if message_id in pending_lookup:
                worker_name = pending_lookup[message_id]["consumer"].decode()
                started = now - timedelta(
                    milliseconds=pending_lookup[message_id]["time_since_delivered"]
                )
                running.append(RunningExecution(execution, worker_name, started))
            else:
                future.append(execution)  # pragma: no cover

        for message in queued_messages:
            function = self.tasks[message[b"function"].decode()]
            execution = Execution.from_message(function, message)
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
        """Clear all pending and scheduled tasks from the docket.

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

                    # Clear all data
                    # Trim stream to 0 messages instead of deleting it to preserve consumer group
                    if stream_count > 0:
                        pipeline.xtrim(self.stream_key, maxlen=0, approximate=False)
                    pipeline.delete(self.queue_key)

                    # Clear parked task data and known task keys
                    for key_bytes in scheduled_keys:
                        key = key_bytes.decode()
                        pipeline.delete(self.parked_task_key(key))
                        pipeline.delete(self.known_task_key(key))
                        pipeline.delete(self.stream_id_key(key))

                    await pipeline.execute()

                    total_cleared = stream_count + queue_count
                    return total_cleared
