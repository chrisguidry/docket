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
    NoReturn,
    ParamSpec,
    Self,
    Sequence,
    TypedDict,
    TypeVar,
    cast,
    overload,
)
from uuid import uuid4

import redis.exceptions
from opentelemetry import propagate, trace
from redis.asyncio import Redis

from .execution import (
    Execution,
    LiteralOperator,
    Operator,
    Restore,
    Strike,
    StrikeInstruction,
    StrikeList,
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


P = ParamSpec("P")
R = TypeVar("R")

TaskCollection = Iterable[Callable[..., Awaitable[Any]]]

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
        self.function: Callable[..., Awaitable[Any]] = execution.function
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
    tasks: dict[str, Callable[..., Awaitable[Any]]]
    strike_list: StrikeList

    def __init__(
        self,
        name: str = "docket",
        url: str = "redis://localhost:6379/0",
        heartbeat_interval: timedelta = timedelta(seconds=1),
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
        """
        self.name = name
        self.url = url
        self.heartbeat_interval = heartbeat_interval
        self.missed_heartbeats = missed_heartbeats

    @property
    def worker_group_name(self) -> str:
        return "docket-workers"

    async def __aenter__(self) -> Self:
        from .tasks import standard_tasks

        self.tasks = {fn.__name__: fn for fn in standard_tasks}
        self.strike_list = StrikeList()

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

    @asynccontextmanager
    async def redis(self) -> AsyncGenerator[Redis, None]:
        redis: Redis | None = None
        try:
            redis = await Redis.from_url(
                self.url,
                single_connection_client=True,
            )
            async with redis:
                yield redis
        finally:
            # redis 4.6.0 doesn't automatically disconnect and leaves connections open
            if redis:
                await redis.connection_pool.disconnect()

    def register(self, function: Callable[..., Awaitable[Any]]) -> None:
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

    @overload
    def add(
        self,
        function: Callable[P, Awaitable[R]],
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[P, Awaitable[Execution]]: ...  # pragma: no cover

    @overload
    def add(
        self,
        function: str,
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[..., Awaitable[Execution]]: ...  # pragma: no cover

    def add(
        self,
        function: Callable[P, Awaitable[R]] | str,
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[..., Awaitable[Execution]]:
        if isinstance(function, str):
            function = self.tasks[function]
        else:
            self.register(function)

        if when is None:
            when = datetime.now(timezone.utc)

        if key is None:
            key = f"{function.__name__}:{uuid4()}"

        async def scheduler(*args: P.args, **kwargs: P.kwargs) -> Execution:
            execution = Execution(function, args, kwargs, when, key, attempt=1)
            await self.schedule(execution)

            TASKS_ADDED.add(1, {"docket": self.name, "task": function.__name__})

            return execution

        return scheduler

    @overload
    def replace(
        self,
        function: Callable[P, Awaitable[R]],
        when: datetime,
        key: str,
    ) -> Callable[P, Awaitable[Execution]]: ...  # pragma: no cover

    @overload
    def replace(
        self,
        function: str,
        when: datetime,
        key: str,
    ) -> Callable[..., Awaitable[Execution]]: ...  # pragma: no cover

    def replace(
        self,
        function: Callable[P, Awaitable[R]] | str,
        when: datetime,
        key: str,
    ) -> Callable[..., Awaitable[Execution]]:
        if isinstance(function, str):
            function = self.tasks[function]

        async def scheduler(*args: P.args, **kwargs: P.kwargs) -> Execution:
            execution = Execution(function, args, kwargs, when, key, attempt=1)
            await self.cancel(key)
            await self.schedule(execution)

            TASKS_REPLACED.add(1, {"docket": self.name, "task": function.__name__})

            return execution

        return scheduler

    @property
    def queue_key(self) -> str:
        return f"{self.name}:queue"

    @property
    def stream_key(self) -> str:
        return f"{self.name}:stream"

    def parked_task_key(self, key: str) -> str:
        return f"{self.name}:{key}"

    async def schedule(self, execution: Execution) -> None:
        if self.strike_list.is_stricken(execution):
            logger.warning(
                "%r is stricken, skipping schedule of %r",
                execution.function.__name__,
                execution.key,
            )
            TASKS_STRICKEN.add(
                1,
                {
                    "docket": self.name,
                    "task": execution.function.__name__,
                    "where": "docket",
                },
            )
            return

        message: dict[bytes, bytes] = execution.as_message()
        propagate.inject(message, setter=message_setter)

        with tracer.start_as_current_span(
            "docket.schedule",
            attributes={
                "docket.name": self.name,
                "docket.execution.when": execution.when.isoformat(),
                "docket.execution.key": execution.key,
                "docket.execution.attempt": execution.attempt,
                "code.function.name": execution.function.__name__,
            },
        ):
            key = execution.key
            when = execution.when

            async with self.redis() as redis:
                # if the task is already in the queue, retain it
                if await redis.zscore(self.queue_key, key) is not None:
                    return

                if when <= datetime.now(timezone.utc):
                    await redis.xadd(self.stream_key, message)  # type: ignore[arg-type]
                else:
                    async with redis.pipeline() as pipe:
                        pipe.hset(self.parked_task_key(key), mapping=message)  # type: ignore[arg-type]
                        pipe.zadd(self.queue_key, {key: when.timestamp()})
                        await pipe.execute()

        TASKS_SCHEDULED.add(
            1, {"docket": self.name, "task": execution.function.__name__}
        )

    async def cancel(self, key: str) -> None:
        with tracer.start_as_current_span(
            "docket.cancel",
            attributes={
                "docket.name": self.name,
                "docket.execution.key": key,
            },
        ):
            async with self.redis() as redis:
                async with redis.pipeline() as pipe:
                    pipe.delete(self.parked_task_key(key))
                    pipe.zrem(self.queue_key, key)
                    await pipe.execute()

        TASKS_CANCELLED.add(1, {"docket": self.name})

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
        if not isinstance(function, (str, type(None))):
            function = function.__name__

        operator = Operator(operator)

        restore = Restore(function, parameter, operator, value)
        return await self._send_strike_instruction(restore)

    async def _send_strike_instruction(self, instruction: StrikeInstruction) -> None:
        with tracer.start_as_current_span(
            f"docket.{instruction.direction}",
            attributes={
                "docket.name": self.name,
                **instruction.as_span_attributes(),
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
                                    extra={"docket": self.name},
                                )

                                counter_labels = {"docket": self.name}
                                if instruction.function:
                                    counter_labels["task"] = instruction.function
                                if instruction.parameter:
                                    counter_labels["parameter"] = instruction.parameter

                                STRIKES_IN_EFFECT.add(
                                    1 if instruction.direction == "strike" else -1,
                                    counter_labels,
                                )

            except redis.exceptions.ConnectionError:  # pragma: no cover
                REDIS_DISRUPTIONS.add(1, {"docket": self.name})
                logger.warning("Connection error, sleeping for 1 second...")
                await asyncio.sleep(1)
            except Exception:  # pragma: no cover
                logger.exception("Error monitoring strikes")
                await asyncio.sleep(1)

    async def snapshot(self) -> DocketSnapshot:
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
