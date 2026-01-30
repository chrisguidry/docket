"""Cron-style scheduling dependency."""

from __future__ import annotations

from datetime import datetime, timezone, tzinfo
from typing import TYPE_CHECKING

from croniter import croniter

from ._perpetual import Perpetual

if TYPE_CHECKING:  # pragma: no cover
    from ._base import TaskOutcome
    from ..execution import Execution


class Cron(Perpetual):
    """Declare a task that should run on a cron schedule. Cron tasks are automatically
    rescheduled for the next matching time after they finish (whether they succeed or
    fail). By default, a cron task is scheduled at worker startup with `automatic=True`.

    Unlike `Perpetual` which schedules based on intervals from the current time, `Cron`
    schedules based on wall-clock time, ensuring tasks run at consistent times regardless
    of execution duration or delays.

    Supports standard cron expressions and Vixie cron-style keywords (@daily, @hourly, etc.)
    via the croniter library.

    Example:

    ```python
    from zoneinfo import ZoneInfo

    @task
    async def weekly_report(cron: Cron = Cron("0 9 * * 1")) -> None:
        # Runs every Monday at 9:00 AM UTC
        ...

    @task
    async def daily_cleanup(cron: Cron = Cron("@daily")) -> None:
        # Runs every day at midnight UTC
        ...

    @task
    async def morning_standup(
        cron: Cron = Cron("0 9 * * 1-5", tz=ZoneInfo("America/Los_Angeles"))
    ) -> None:
        # Runs weekdays at 9:00 AM Pacific (handles DST automatically)
        ...
    ```
    """

    expression: str
    tz: tzinfo

    _croniter: croniter[datetime]

    def __init__(
        self,
        expression: str,
        automatic: bool = True,
        tz: tzinfo = timezone.utc,
    ) -> None:
        """
        Args:
            expression: A cron expression string. Supports:
                - Standard 5-field syntax: "minute hour day month weekday"
                  (e.g., "0 9 * * 1" for Mondays at 9 AM)
                - Vixie cron keywords: @yearly, @annually, @monthly, @weekly,
                  @daily, @midnight, @hourly
            automatic: If set, this task will be automatically scheduled during worker
                startup and continually through the worker's lifespan. This ensures
                that the task will always be scheduled despite crashes and other
                adverse conditions. Automatic tasks must not require any arguments.
            tz: Timezone for interpreting the cron expression. Defaults to UTC.
                Use `ZoneInfo("America/Los_Angeles")` for Pacific time, etc.
                This correctly handles daylight saving time transitions.
        """
        super().__init__(automatic=automatic)
        self.expression = expression
        self.tz = tz
        self._croniter = croniter(self.expression, datetime.now(self.tz), datetime)

    async def __aenter__(self) -> Cron:
        execution = self.execution.get()
        cron = Cron(expression=self.expression, automatic=self.automatic, tz=self.tz)
        cron.args = execution.args
        cron.kwargs = execution.kwargs
        return cron

    @property
    def initial_when(self) -> datetime:
        """Return the next cron time for initial scheduling."""
        return self._croniter.get_next()

    async def on_complete(self, execution: Execution, outcome: TaskOutcome) -> bool:
        """Handle completion by scheduling the next execution at the exact cron time.

        This overrides Perpetual's on_complete to ensure we hit the exact wall-clock
        time rather than adjusting for task duration.
        """
        self.at(self._croniter.get_next())
        return await super().on_complete(execution, outcome)
