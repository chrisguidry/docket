"""Base Dependency class and ContextVars for dependency injection."""

from __future__ import annotations

import abc
from contextvars import ContextVar
from types import TracebackType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from ..docket import Docket
    from ..execution import Execution
    from ..worker import Worker


class Dependency(abc.ABC):
    """Base class for all dependencies."""

    single: bool = False

    docket: ContextVar[Docket] = ContextVar("docket")
    worker: ContextVar[Worker] = ContextVar("worker")
    execution: ContextVar[Execution] = ContextVar("execution")

    @abc.abstractmethod
    async def __aenter__(self) -> Any: ...  # pragma: no cover

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> bool: ...  # pragma: no cover
