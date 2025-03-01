import importlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Iterable,
    ParamSpec,
    Self,
    TypeVar,
    overload,
)
from uuid import uuid4

from opentelemetry import propagate, trace
from redis.asyncio import Redis

from .execution import Execution
from .instrumentation import (
    TASKS_ADDED,
    TASKS_CANCELLED,
    TASKS_REPLACED,
    TASKS_SCHEDULED,
    message_setter,
)

tracer: trace.Tracer = trace.get_tracer(__name__)


P = ParamSpec("P")
R = TypeVar("R")

TaskCollection = Iterable[Callable[..., Awaitable[Any]]]


class Docket:
    tasks: dict[str, Callable[..., Awaitable[Any]]]

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

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
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
