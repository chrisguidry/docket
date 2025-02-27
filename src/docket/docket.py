from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    ParamSpec,
    Self,
    TypeVar,
    overload,
)
from uuid import uuid4

from redis.asyncio import Redis

from .execution import Execution

P = ParamSpec("P")
R = TypeVar("R")


class Docket:
    tasks: dict[str, Callable[..., Awaitable[Any]]]

    def __init__(
        self,
        name: str = "docket",
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
    ) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.db = db
        self.password = password

    async def __aenter__(self) -> Self:
        self.tasks = {}
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
        async with Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            single_connection_client=True,
        ) as redis:
            yield redis

    def register(self, function: Callable[..., Awaitable[Any]]) -> None:
        from .dependencies import validate_dependencies

        validate_dependencies(function)

        self.tasks[function.__name__] = function

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

    async def cancel(self, key: str) -> None:
        async with self.redis() as redis:
            async with redis.pipeline() as pipe:
                pipe.delete(self.parked_task_key(key))
                pipe.zrem(self.queue_key, key)
                await pipe.execute()
