"""Perpetual task dependency."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from ._base import CompletionHandler, TaskOutcome, format_duration

if TYPE_CHECKING:  # pragma: no cover
    from ..execution import Execution

from ..instrumentation import TASKS_PERPETUATED, TASKS_SUPERSEDED

logger = logging.getLogger(__name__)


class Perpetual(CompletionHandler):
    """Declare a task that should be run perpetually.  Perpetual tasks are automatically
    rescheduled for the future after they finish (whether they succeed or fail).  A
    perpetual task can be scheduled at worker startup with the `automatic=True`.

    Example:

    ```python
    @task
    async def my_task(perpetual: Perpetual = Perpetual()) -> None:
        ...
    ```
    """

    single = True

    every: timedelta
    automatic: bool

    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    cancelled: bool
    _next_when: datetime | None

    def __init__(
        self,
        every: timedelta = timedelta(0),
        automatic: bool = False,
    ) -> None:
        """
        Args:
            every: The target interval between task executions.
            automatic: If set, this task will be automatically scheduled during worker
                startup and continually through the worker's lifespan.  This ensures
                that the task will always be scheduled despite crashes and other
                adverse conditions.  Automatic tasks must not require any arguments.
        """
        self.every = every
        self.automatic = automatic
        self.cancelled = False
        self._next_when = None

    async def __aenter__(self) -> Perpetual:
        execution = self.execution.get()
        perpetual = Perpetual(every=self.every, automatic=self.automatic)
        perpetual.args = execution.args
        perpetual.kwargs = execution.kwargs
        return perpetual

    @property
    def initial_when(self) -> datetime | None:
        """Return None to schedule for immediate execution at worker startup."""
        return None

    def cancel(self) -> None:
        self.cancelled = True

    def perpetuate(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs

    def after(self, delay: timedelta) -> None:
        """Schedule the next execution after the given delay."""
        self._next_when = datetime.now(timezone.utc) + delay

    def at(self, when: datetime) -> None:
        """Schedule the next execution at the given time."""
        self._next_when = when

    async def on_complete(self, execution: Execution, outcome: TaskOutcome) -> bool:
        """Handle completion by scheduling the next execution."""
        if self.cancelled:
            docket = self.docket.get()
            async with docket.redis() as redis:
                await docket._cancel(redis, execution.key)
            return False

        if await execution.is_superseded():
            worker = self.worker.get()
            TASKS_SUPERSEDED.add(
                1,
                {
                    **worker.labels(),
                    **execution.general_labels(),
                    "docket.where": "on_complete",
                },
            )
            logger.info(
                "↬ [%s] %s (superseded)",
                format_duration(outcome.duration.total_seconds()),
                execution.call_repr(),
            )
            return True

        docket = self.docket.get()
        worker = self.worker.get()

        if self._next_when:
            when = self._next_when
        else:
            now = datetime.now(timezone.utc)
            when = max(now, now + self.every - outcome.duration)

        await docket.replace(execution.function, when, execution.key)(
            *self.args,
            **self.kwargs,
        )

        TASKS_PERPETUATED.add(1, {**worker.labels(), **execution.general_labels()})
        logger.info(
            "↫ [%s] %s",
            format_duration(outcome.duration.total_seconds()),
            execution.call_repr(),
        )

        return True
