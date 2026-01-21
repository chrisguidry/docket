"""Worker-scoped Shared dependencies."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from contextvars import ContextVar
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    Callable,
    ClassVar,
    Generic,
    TypeVar,
    cast,
)

from ._base import Dependency
from ._depends import get_dependency_parameters

if TYPE_CHECKING:  # pragma: no cover
    from ..docket import Docket
    from ..worker import Worker

R = TypeVar("R")

SharedFactory = Callable[..., AsyncContextManager[R]]


class _Shared(Dependency, Generic[R]):
    """Worker-scoped dependency resolved once and shared across all tasks.

    Unlike Depends (which resolves per-task), Shared dependencies initialize once
    at worker startup (or lazily on first use) and the same instance is provided
    to all tasks throughout the worker's lifetime.
    """

    def __init__(self, factory: SharedFactory[R]) -> None:
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
    resolved: ClassVar[ContextVar[dict[SharedFactory[Any], Any]]] = ContextVar(
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


def Shared(factory: SharedFactory[R]) -> R:
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
    context dependencies like CurrentDocket and CurrentWorker:

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
