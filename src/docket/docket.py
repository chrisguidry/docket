from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Awaitable, Callable, ParamSpec, Self, TypeVar, overload

P = ParamSpec("P")
R = TypeVar("R")


class Execution:
    def __init__(
        self,
        function: Callable[P, Awaitable[R]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        when: datetime,
    ) -> None:
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.when = when


class Docket:
    executions: list[Execution]
    tasks: dict[str, Callable[..., Awaitable[Any]]]

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

    def add(self, function: Callable[..., Awaitable[Any]]) -> None:
        self.tasks[function.__name__] = function

    @overload
    def run(
        self, function: Callable[P, Awaitable[R]]
    ) -> Callable[P, Awaitable[None]]: ...

    @overload
    def run(self, function: str) -> Callable[..., Awaitable[None]]: ...

    def run(
        self, function: Callable[P, Awaitable[R]] | str
    ) -> Callable[..., Awaitable[None]]:
        return self.schedule(function, datetime.now(timezone.utc))

    @overload
    def schedule(
        self, function: Callable[P, Awaitable[R]], when: datetime
    ) -> Callable[P, Awaitable[None]]: ...

    @overload
    def schedule(
        self, function: str, when: datetime
    ) -> Callable[..., Awaitable[None]]: ...

    def schedule(
        self, function: Callable[P, Awaitable[R]] | str, when: datetime
    ) -> Callable[..., Awaitable[None]]:
        async def scheduler(*args: P.args, **kwargs: P.kwargs) -> None:
            nonlocal function
            if isinstance(function, str):
                function = self.tasks[function]

            self.executions.append(Execution(function, args, kwargs, when))

        return scheduler
