"""Functional dependencies: Depends and Shared."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AsyncExitStack
from contextvars import ContextVar
from types import TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, cast

from uncalled_for import (
    DependencyFactory,
    Shared as Shared,
    SharedContext as _UncalledForSharedContext,
    _Depends as _UncalledForDepends,
    _parameter_cache as _parameter_cache,
    get_dependency_parameters,
)

from ._base import current_docket, current_worker
from ._contextual import _TaskArgument

if TYPE_CHECKING:  # pragma: no cover
    from ..docket import Docket
    from ..worker import Worker

R = TypeVar("R")

DependencyFunction = DependencyFactory


class _Depends(_UncalledForDepends[R]):
    """Docket's call-scoped dependency with TaskArgument inference."""

    async def _resolve_parameters(
        self,
        function: Callable[..., Any],
    ) -> dict[str, Any]:
        stack = self.stack.get()
        arguments: dict[str, Any] = {}
        parameters = get_dependency_parameters(function)

        for parameter, dependency in parameters.items():
            if isinstance(dependency, _TaskArgument) and not dependency.parameter:
                dependency.parameter = parameter

            arguments[parameter] = await stack.enter_async_context(dependency)

        return arguments


def Depends(dependency: DependencyFactory[R]) -> R:
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


class SharedContext:
    """Manages worker-scoped Shared dependency lifecycle.

    Wraps uncalled_for.SharedContext, adding docket/worker ContextVar management.
    """

    resolved: ClassVar[ContextVar[dict[DependencyFactory[Any], Any]]] = (
        _UncalledForSharedContext.resolved
    )
    lock: ClassVar[ContextVar[asyncio.Lock]] = _UncalledForSharedContext.lock
    stack: ClassVar[ContextVar[AsyncExitStack]] = _UncalledForSharedContext.stack

    def __init__(self, docket: Docket, worker: Worker) -> None:
        self._docket = docket
        self._worker = worker
        self._inner = _UncalledForSharedContext()

    async def __aenter__(self) -> SharedContext:
        await self._inner.__aenter__()

        self._docket_token = current_docket.set(self._docket)
        self._worker_token = current_worker.set(self._worker)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._inner.__aexit__(exc_type, exc_value, traceback)

        current_worker.reset(self._worker_token)
        current_docket.reset(self._docket_token)
