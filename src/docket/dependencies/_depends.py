"""Core Depends and Shared dependency resolution."""

from __future__ import annotations

import asyncio
import inspect
from contextlib import AsyncExitStack
from contextvars import ContextVar
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    Awaitable,
    Callable,
    ClassVar,
    ContextManager,
    Generic,
    TypeVar,
    cast,
)

from ..execution import TaskFunction, get_signature
from ..instrumentation import CACHE_SIZE
from ._base import Dependency
from ._contextual import _TaskArgument

if TYPE_CHECKING:  # pragma: no cover
    from ..docket import Docket
    from ..worker import Worker

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


class _Shared(Dependency, Generic[R]):
    """Worker-scoped dependency resolved once and shared across all tasks.

    Unlike Depends (which resolves per-task), Shared dependencies initialize once
    at worker startup (or lazily on first use) and the same instance is provided
    to all tasks throughout the worker's lifetime.
    """

    def __init__(self, factory: DependencyFunction[R]) -> None:
        self.factory = factory

    async def __aenter__(self) -> R:
        resolved = SharedContext.resolved.get()

        # Fast path: already resolved (keyed by factory function)
        if self.factory in resolved:
            return resolved[self.factory]

        # Resolve factory's dependencies OUTSIDE the lock to avoid deadlock
        # when a Shared depends on another Shared
        arguments = await self._resolve_parameters()

        # Now acquire lock to check/store the resolved value
        async with SharedContext.lock.get():
            # Double-check after acquiring lock (another task may have resolved)
            if self.factory in resolved:  # pragma: no cover
                return resolved[self.factory]

            # Enter the context and store the value
            stack = SharedContext.stack.get()
            context_manager = self.factory(**arguments)
            value = await stack.enter_async_context(context_manager)
            resolved[self.factory] = value
            return value

    async def _resolve_parameters(self) -> dict[str, Any]:
        """Resolve parameters for the factory function."""
        stack = SharedContext.stack.get()
        arguments: dict[str, Any] = {}
        parameters = get_dependency_parameters(self.factory)

        for parameter, dependency in parameters.items():
            arguments[parameter] = await stack.enter_async_context(dependency)

        return arguments


class SharedContext:
    """Manages worker-scoped Shared dependency lifecycle.

    Created by the Worker to set up ContextVars for Shared dependencies.
    Handles initialization of the AsyncExitStack and cleanup on worker exit.
    """

    # ContextVars for Shared dependency state
    resolved: ClassVar[ContextVar[dict[DependencyFunction[Any], Any]]] = ContextVar(
        "shared_resolved"
    )
    lock: ClassVar[ContextVar[asyncio.Lock]] = ContextVar("shared_lock")
    stack: ClassVar[ContextVar[AsyncExitStack]] = ContextVar("shared_stack")

    def __init__(self, docket: Docket, worker: Worker) -> None:
        self._docket = docket
        self._worker = worker

    async def __aenter__(self) -> SharedContext:
        self._stack = AsyncExitStack()
        await self._stack.__aenter__()

        self._docket_token = Dependency.docket.set(self._docket)
        self._worker_token = Dependency.worker.set(self._worker)
        self._resolved_token = SharedContext.resolved.set({})
        self._lock_token = SharedContext.lock.set(asyncio.Lock())
        self._stack_token = SharedContext.stack.set(self._stack)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # Close Shared dependencies (context managers exit in reverse order)
        await self._stack.__aexit__(exc_type, exc_value, traceback)

        SharedContext.stack.reset(self._stack_token)
        SharedContext.lock.reset(self._lock_token)
        SharedContext.resolved.reset(self._resolved_token)
        Dependency.worker.reset(self._worker_token)
        Dependency.docket.reset(self._docket_token)


def Shared(factory: DependencyFunction[R]) -> R:
    """Declare a worker-scoped dependency shared across all tasks.

    The factory must be an async context manager (decorated with @asynccontextmanager).
    It initializes once when first needed and the yielded value is shared by all tasks
    for the lifetime of the worker.

    Identity is the factory function - multiple Shared(same_factory) calls anywhere
    in the codebase resolve to the same cached value.

    Example:

    ```python
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def create_db_pool():
        pool = await AsyncConnectionPool.create(conninfo="...")
        try:
            yield pool
        finally:
            await pool.close()

    @task
    async def my_task(pool: Pool = Shared(create_db_pool)):
        async with pool.connection() as conn:
            await conn.execute("SELECT ...")

    @task
    async def other_task(pool: Pool = Shared(create_db_pool)):
        # Same pool instance as my_task!
        ...
    ```

    Shared dependencies can depend on other Shared dependencies, Depends, and
    contextual dependencies like CurrentDocket and CurrentWorker:

    ```python
    @asynccontextmanager
    async def create_pool(
        docket: Docket = CurrentDocket(),
        url: str = Depends(get_connection_string),
    ):
        logger.info(f"Creating pool for {docket.name}")
        pool = await create_pool(url)
        yield pool
        await pool.close()
    ```
    """
    return cast(R, _Shared(factory))
