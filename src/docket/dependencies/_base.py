"""Base Dependency class and ContextVars for dependency injection."""

from __future__ import annotations

import abc
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Awaitable, Callable, TypeVar

from uncalled_for import Dependency as Dependency

if TYPE_CHECKING:  # pragma: no cover
    from ..docket import Docket
    from ..execution import Execution
    from ..worker import Worker

T = TypeVar("T", covariant=True)

current_docket: ContextVar[Docket] = ContextVar("current_docket")
current_worker: ContextVar[Worker] = ContextVar("current_worker")
current_execution: ContextVar[Execution] = ContextVar("current_execution")

# Backwards compatibility: prior to 0.18, docket defined its own Dependency base
# class with class-level ContextVars (Dependency.execution, Dependency.docket,
# Dependency.worker).  Now that the base Dependency class comes from uncalled-for,
# those ContextVars live at module scope above.  However, downstream consumers
# (notably FastMCP) access them as Dependency.execution.get(), so we monkeypatch
# them back onto the class to avoid breaking existing code.  This shim can be
# removed once all known consumers have migrated to the module-level ContextVars.
Dependency.execution = current_execution  # type: ignore[attr-defined]
Dependency.docket = current_docket  # type: ignore[attr-defined]
Dependency.worker = current_worker  # type: ignore[attr-defined]


def format_duration(seconds: float) -> str:
    """Format a duration for log output."""
    if seconds < 100:
        return f"{seconds * 1000:6.0f}ms"
    else:
        return f"{seconds:6.0f}s "


@dataclass
class TaskOutcome:
    """Captures the outcome of a task execution for handlers."""

    duration: timedelta
    result: Any = field(default=None)
    exception: BaseException | None = field(default=None)


class AdmissionBlocked(Exception):
    """Raised when a task cannot start due to admission control.

    This is the base exception for admission control mechanisms like
    concurrency limits, rate limits, or health gates.

    When ``reschedule`` is True (default), the worker re-queues the task
    with a short delay.  When False, the task is quietly acknowledged
    and dropped with an INFO-level log (appropriate for debounce/cooldown
    where re-trying would just hit the same window).

    ``retry_delay`` overrides the default reschedule delay when set.
    """

    def __init__(
        self,
        execution: Execution,
        reason: str = "admission control",
        *,
        reschedule: bool = True,
        retry_delay: timedelta | None = None,
    ):
        self.execution = execution
        self.reason = reason
        self.reschedule = reschedule
        self.retry_delay = retry_delay
        super().__init__(f"Task {execution.key} blocked by {reason}")


class Runtime(Dependency[T]):
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


class FailureHandler(Dependency[T]):
    """Base class for dependencies that control what happens when a task fails.

    Called on exceptions. If handle_failure() returns True, the handler
    took responsibility (e.g., scheduled a retry) and Worker won't mark
    the execution as failed.

    Only one FailureHandler per task (single=True).
    """

    single = True

    @abc.abstractmethod
    async def handle_failure(self, execution: Execution, outcome: TaskOutcome) -> bool:
        """Handle a task failure.

        Args:
            execution: The task execution context
            outcome: The task outcome containing duration and exception

        Returns:
            True if handled (Worker won't mark as failed)
            False if not handled (Worker proceeds normally)
        """
        ...  # pragma: no cover


class CompletionHandler(Dependency[T]):
    """Base class for dependencies that control what happens after task completion.

    Called after execution is truly done (success, or failure with no retry).
    If on_complete() returns True, the handler took responsibility (e.g.,
    scheduled follow-up work) and did its own logging.

    Only one CompletionHandler per task (single=True).
    """

    single = True

    @abc.abstractmethod
    async def on_complete(self, execution: Execution, outcome: TaskOutcome) -> bool:
        """Handle task completion.

        Args:
            execution: The task execution context
            outcome: The task outcome containing duration, result, and exception

        Returns:
            True if handled (did own logging/metrics)
            False if not handled (Worker does normal logging)
        """
        ...  # pragma: no cover
