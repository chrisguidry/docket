import asyncio
import importlib
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Hashable,
    Iterable,
    Literal,
    NoReturn,
    ParamSpec,
    Self,
    Sequence,
    TypeVar,
    overload,
)
from uuid import uuid4

import redis.exceptions
from opentelemetry import propagate, trace
from redis.asyncio import Redis

from .execution import (
    Execution,
    Operator,
    Restore,
    Strike,
    StrikeInstruction,
    StrikeList,
)
from .instrumentation import (
    TASKS_ADDED,
    TASKS_CANCELLED,
    TASKS_REPLACED,
    TASKS_SCHEDULED,
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


class Docket:
    tasks: dict[str, Callable[..., Awaitable[Any]]]
    strike_list: StrikeList

    def __init__(
        self,
        name: str = "docket",
        url: str = "redis://localhost:6379/0",
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

    async def __aenter__(self) -> Self:
        from .tasks import standard_tasks

        self.tasks = {fn.__name__: fn for fn in standard_tasks}
        self.strike_list = StrikeList()

        self._monitor_strikes_task = asyncio.create_task(self._monitor_strikes())

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
        async with Redis.from_url(self.url) as redis:
            yield redis

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
                    await redis.xadd(self.stream_key, message)
                else:
                    async with redis.pipeline() as pipe:
                        pipe.hset(self.parked_task_key(key), mapping=message)
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
        operator: Operator = "==",
        value: Hashable | None = None,
    ) -> None:
        if not isinstance(function, (str, type(None))):
            function = function.__name__

        strike = Strike(function, parameter, operator, value)
        return await self._send_strike_instruction(strike)

    async def restore(
        self,
        function: Callable[P, Awaitable[R]] | str | None = None,
        parameter: str | None = None,
        operator: Literal["==", "!=", ">", ">=", "<", "<=", "between"] = "==",
        value: Hashable | None = None,
    ) -> None:
        if not isinstance(function, (str, type(None))):
            function = function.__name__

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
                await redis.xadd(self.strike_key, instruction.as_message())
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
            except redis.exceptions.ConnectionError:  # pragma: no cover
                logger.warning("Connection error, sleeping for 1 second...")
                await asyncio.sleep(1)
            except Exception:  # pragma: no cover
                logger.exception("Error monitoring strikes")
                await asyncio.sleep(1)
