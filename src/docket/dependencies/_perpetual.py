"""Perpetual task dependency."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from ._base import Dependency


class Perpetual(Dependency):
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

    async def __aenter__(self) -> Perpetual:
        execution = self.execution.get()
        perpetual = Perpetual(every=self.every)
        perpetual.args = execution.args
        perpetual.kwargs = execution.kwargs
        return perpetual

    def cancel(self) -> None:
        self.cancelled = True

    def perpetuate(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs
