from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Awaitable, Callable, ParamSpec, Self, TypeVar, overload
from uuid import uuid4

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

        async def scheduler(*args: P.args, **kwargs: P.kwargs) -> Execution:
            nonlocal key
            if key is None:
                key = f"{function.__name__}-{uuid4()}"

            execution = Execution(function, args, kwargs, when, key)

            for i, prior in enumerate(self.executions):
                if execution.key == prior.key:
                    self.executions[i] = execution
                    return execution

            self.executions.append(execution)
            return execution

        return scheduler

    async def cancel(self, key: str) -> None:
        for i, execution in enumerate(self.executions):
            if execution.key == key:
                self.executions.pop(i)
                return
