"""Base Dependency class and ContextVars for dependency injection."""

from __future__ import annotations

import abc
from contextvars import ContextVar
from types import TracebackType
from typing import TYPE_CHECKING, Any, Awaitable, Callable

if TYPE_CHECKING:  # pragma: no cover
    from ..docket import Docket
    from ..execution import Execution
    from ..worker import Worker


class AdmissionBlocked(Exception):
    """Raised when a task cannot start due to admission control.

    This is the base exception for admission control mechanisms like
    concurrency limits, rate limits, or health gates.
    """

    def __init__(self, execution: Execution, reason: str = "admission control"):
        self.execution = execution
        self.reason = reason
        super().__init__(f"Task {execution.key} blocked by {reason}")


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


class Runtime(Dependency):
    """Base class for dependencies that control task execution.

    Only one Runtime dependency can be active per task (single=True).
    The Worker will call run() to execute the task.
    """

    single = True

    @abc.abstractmethod
    async def run(
        self,
        execution: Execution,
        function: Callable[..., Awaitable[Any]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        """Execute the function with this runtime's behavior.

        Args:
            execution: The task execution context
            function: The task function to call
            args: Positional arguments for the function
            kwargs: Keyword arguments including resolved dependencies
        """
        ...  # pragma: no cover
