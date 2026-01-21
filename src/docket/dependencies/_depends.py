"""Core Depends dependency and parameter resolution."""

from __future__ import annotations

import inspect
from contextlib import AsyncExitStack
from contextvars import ContextVar
from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    Callable,
    ContextManager,
    Generic,
    TypeVar,
    cast,
)

from ..execution import TaskFunction, get_signature
from ..instrumentation import CACHE_SIZE
from ._base import Dependency
from ._context import _TaskArgument

R = TypeVar("R")

DependencyFunction = Callable[
    ..., R | Awaitable[R] | ContextManager[R] | AsyncContextManager[R]
]


_parameter_cache: dict[
    TaskFunction | DependencyFunction[Any],
    dict[str, Dependency],
] = {}


def get_dependency_parameters(
    function: TaskFunction | DependencyFunction[Any],
) -> dict[str, Dependency]:
    if function in _parameter_cache:
        CACHE_SIZE.set(len(_parameter_cache), {"cache": "parameter"})
        return _parameter_cache[function]

    dependencies: dict[str, Dependency] = {}

    signature = get_signature(function)

    for parameter, param in signature.parameters.items():
        if not isinstance(param.default, Dependency):
            continue

        dependencies[parameter] = param.default

    _parameter_cache[function] = dependencies
    CACHE_SIZE.set(len(_parameter_cache), {"cache": "parameter"})
    return dependencies


class _Depends(Dependency, Generic[R]):
    dependency: DependencyFunction[R]

    cache: ContextVar[dict[DependencyFunction[Any], Any]] = ContextVar("cache")
    stack: ContextVar[AsyncExitStack] = ContextVar("stack")

    def __init__(
        self,
        dependency: Callable[
            [], R | Awaitable[R] | ContextManager[R] | AsyncContextManager[R]
        ],
    ) -> None:
        self.dependency = dependency

    async def _resolve_parameters(
        self,
        function: TaskFunction | DependencyFunction[Any],
    ) -> dict[str, Any]:
        stack = self.stack.get()

        arguments: dict[str, Any] = {}
        parameters = get_dependency_parameters(function)

        for parameter, dependency in parameters.items():
            # Special case for TaskArguments, they are "magical" and infer the parameter
            # they refer to from the parameter name (unless otherwise specified)
            if isinstance(dependency, _TaskArgument) and not dependency.parameter:
                dependency.parameter = parameter

            arguments[parameter] = await stack.enter_async_context(dependency)

        return arguments

    async def __aenter__(self) -> R:
        cache = self.cache.get()

        if self.dependency in cache:
            return cache[self.dependency]

        stack = self.stack.get()
        arguments = await self._resolve_parameters(self.dependency)

        raw_value: R | Awaitable[R] | ContextManager[R] | AsyncContextManager[R] = (
            self.dependency(**arguments)
        )

        # Handle different return types from the dependency function
        resolved_value: R
        if isinstance(raw_value, AsyncContextManager):
            # Async context manager: await enter_async_context
            resolved_value = await stack.enter_async_context(raw_value)
        elif isinstance(raw_value, ContextManager):
            # Sync context manager: use enter_context (no await needed)
            resolved_value = stack.enter_context(raw_value)
        elif inspect.iscoroutine(raw_value) or isinstance(raw_value, Awaitable):
            # Async function returning awaitable: await it
            resolved_value = await cast(Awaitable[R], raw_value)
        else:
            # Sync function returning a value directly, use as-is
            resolved_value = cast(R, raw_value)

        cache[self.dependency] = resolved_value
        return resolved_value


def Depends(dependency: DependencyFunction[R]) -> R:
    """Include a user-defined function as a dependency.  Dependencies may be:
    - Synchronous functions returning a value
    - Asynchronous functions returning a value (awaitable)
    - Synchronous context managers (using @contextmanager)
    - Asynchronous context managers (using @asynccontextmanager)

    If a dependency returns a context manager, it will be entered and exited around
    the task, giving an opportunity to control the lifetime of a resource.

    **Important**: Synchronous dependencies should NOT include blocking I/O operations
    (file access, network calls, database queries, etc.). Use async dependencies for
    any I/O. Sync dependencies are best for:
    - Pure computations
    - In-memory data structure access
    - Configuration lookups from memory
    - Non-blocking transformations

    Examples:

    ```python
    # Sync dependency - pure computation, no I/O
    def get_config() -> dict:
        # Access in-memory config, no I/O
        return {"api_url": "https://api.example.com", "timeout": 30}

    # Sync dependency - compute value from arguments
    def build_query_params(
        user_id: int = TaskArgument(),
        config: dict = Depends(get_config)
    ) -> dict:
        # Pure computation, no I/O
        return {"user_id": user_id, "timeout": config["timeout"]}

    # Async dependency - I/O operations
    async def get_user(user_id: int = TaskArgument()) -> User:
        # Network I/O - must be async
        return await fetch_user_from_api(user_id)

    # Async context manager - I/O resource management
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def get_db_connection():
        # I/O operations - must be async
        conn = await db.connect()
        try:
            yield conn
        finally:
            await conn.close()

    @task
    async def my_task(
        params: dict = Depends(build_query_params),
        user: User = Depends(get_user),
        db: Connection = Depends(get_db_connection),
    ) -> None:
        await db.execute("UPDATE users SET ...", params)
    ```
    """
    return cast(R, _Depends(dependency))
