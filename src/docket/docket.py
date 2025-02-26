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

import cloudpickle
from redis.asyncio import Redis

P = ParamSpec("P")
R = TypeVar("R")


class Execution:
    def __init__(
        self,
        function: Callable[..., Awaitable[Any]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        when: datetime,
        key: str,
    ) -> None:
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.when = when
        self.key = key


class Docket:
    executions: list[Execution]
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
        self.executions = []
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
        self.tasks[function.__name__] = function

    @overload
    def add(
        self,
        function: Callable[P, Awaitable[R]],
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[P, Awaitable[Execution]]: ...

    @overload
    def add(
        self,
        function: str,
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[..., Awaitable[Execution]]: ...

    def add(
        self,
        function: Callable[P, Awaitable[R]] | str,
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[..., Awaitable[Execution]]:
        if isinstance(function, str):
            function = self.tasks[function]

        if when is None:
            when = datetime.now(timezone.utc)

        if key is None:
            key = f"{function.__name__}:{uuid4()}"

        self.register(function)

        async def scheduler(*args: P.args, **kwargs: P.kwargs) -> Execution:
            execution = Execution(function, args, kwargs, when, key)
            serialized: dict[bytes, bytes] = {
                b"key": key.encode(),
                b"when": when.isoformat().encode(),
                b"function": function.__name__.encode(),
                b"args": cloudpickle.dumps(args),
                b"kwargs": cloudpickle.dumps(kwargs),
            }

            async with self.redis() as redis:
                if when <= datetime.now(timezone.utc):
                    await redis.xadd(f"{self.name}:stream", serialized)
                else:
                    async with redis.pipeline() as pipe:
                        pipe.hset(f"{self.name}:{key}", mapping=serialized)
                        pipe.zadd(f"{self.name}:queue", {key: when.timestamp()})
                        await pipe.execute()

            return execution

        return scheduler

    async def cancel(self, key: str) -> None:
        async with self.redis() as redis:
            async with redis.pipeline() as pipe:
                pipe.delete(f"{self.name}:{key}")
                pipe.zrem(f"{self.name}:queue", key)
                await pipe.execute()
