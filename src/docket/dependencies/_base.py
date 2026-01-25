"""Base Dependency class and ContextVars for dependency injection."""

from __future__ import annotations

import abc
import logging
from contextvars import ContextVar
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Any, Awaitable, Callable

if TYPE_CHECKING:  # pragma: no cover
    from ..docket import Docket
    from ..execution import Execution
    from ..worker import Worker


def format_duration(seconds: float) -> str:
    """Format a duration for log output."""
    if seconds < 100:
        return f"{seconds * 1000:6.0f}ms"
    else:
        return f"{seconds:6.0f}s "


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


class FailureHandler(Dependency):
    """Base class for dependencies that control what happens when a task fails.

    Called on exceptions. If handle_failure() returns True, the handler
    took responsibility (e.g., scheduled a retry) and Worker won't mark
    the execution as failed.

    Only one FailureHandler per task (single=True).
    """

    single = True

    @abc.abstractmethod
    async def handle_failure(
        self,
        execution: Execution,
        call: str,
        exception: BaseException,
        duration: timedelta,
        logger: logging.LoggerAdapter[logging.Logger],
    ) -> bool:
        """Handle a task failure.

        If handling (e.g., retrying), the implementation should:
        - Schedule the retry
        - Log the outcome (using provided logger)
        - Record any metrics

        Args:
            execution: The execution context
            call: Formatted task call string for logging
            exception: The exception that was raised
            duration: How long the task ran before failing
            logger: LoggerAdapter with context already attached

        Returns:
            True if handled (Worker won't mark as failed)
            False if not handled (Worker proceeds normally)
        """
        ...  # pragma: no cover


class CompletionHandler(Dependency):
    """Base class for dependencies that control what happens after task completion.

    Called after execution is truly done (success, or failure with no retry).
    If on_complete() returns True, the handler took responsibility (e.g.,
    scheduled follow-up work) and did its own logging.

    Only one CompletionHandler per task (single=True).
    """

    single = True

    @abc.abstractmethod
    async def on_complete(
        self,
        execution: Execution,
        call: str,
        result: Any,
        exception: BaseException | None,
        duration: timedelta,
        logger: logging.LoggerAdapter[logging.Logger],
    ) -> bool:
        """Handle task completion.

        If handling (e.g., scheduling follow-up), the implementation should:
        - Schedule the follow-up work
        - Log the outcome (using provided logger)
        - Record any metrics

        Args:
            execution: The execution context
            call: Formatted task call string for logging
            result: The return value (None if exception)
            exception: The exception if failed (None if success)
            duration: How long the task ran
            logger: LoggerAdapter with context already attached

        Returns:
            True if handled (did own logging/metrics)
            False if not handled (Worker does normal logging)
        """
        ...  # pragma: no cover
