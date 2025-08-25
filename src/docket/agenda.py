"""
Agenda - A collection of tasks that can be scheduled together.

The Agenda class provides a way to collect multiple tasks and then scatter them
evenly over a time period to avoid overwhelming the system with immediate work.
"""

import random
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Iterator, ParamSpec, TypeVar, overload

from .docket import Docket
from .execution import Execution, TaskFunction

P = ParamSpec("P")
R = TypeVar("R")


class Agenda:
    """A collection of tasks to be scheduled together on a Docket.

    The Agenda allows you to build up a collection of tasks with their arguments,
    then schedule them all at once using various timing strategies like scattering.

    Example:
        >>> agenda = Agenda()
        >>> agenda.add(process_item)(item1)
        >>> agenda.add(process_item)(item2)
        >>> agenda.add(send_email)(email)
        >>> await agenda.scatter(docket, over=timedelta(minutes=50))
    """

    def __init__(self) -> None:
        """Initialize an empty Agenda."""
        self._tasks: list[
            tuple[TaskFunction | str, tuple[Any, ...], dict[str, Any]]
        ] = []

    def __len__(self) -> int:
        """Return the number of tasks in the agenda."""
        return len(self._tasks)

    def __iter__(
        self,
    ) -> Iterator[tuple[TaskFunction | str, tuple[Any, ...], dict[str, Any]]]:
        """Iterate over tasks in the agenda."""
        return iter(self._tasks)

    @overload
    def add(
        self,
        function: Callable[P, Awaitable[R]],
    ) -> Callable[P, None]:
        """Add a task function to the agenda.

        Args:
            function: The task function to add.

        Returns:
            A callable that accepts the task arguments.
        """

    @overload
    def add(
        self,
        function: str,
    ) -> Callable[..., None]:
        """Add a task by name to the agenda.

        Args:
            function: The name of a registered task.

        Returns:
            A callable that accepts the task arguments.
        """

    def add(
        self,
        function: Callable[P, Awaitable[R]] | str,
    ) -> Callable[..., None]:
        """Add a task to the agenda.

        Args:
            function: The task function or name to add.

        Returns:
            A callable that accepts the task arguments and adds them to the agenda.
        """

        def scheduler(*args: Any, **kwargs: Any) -> None:
            self._tasks.append((function, args, kwargs))

        return scheduler

    def clear(self) -> None:
        """Clear all tasks from the agenda."""
        self._tasks.clear()

    async def scatter(
        self,
        docket: Docket,
        over: timedelta,
        start: datetime | None = None,
        jitter: timedelta | None = None,
    ) -> list[Execution]:
        """Scatter the tasks in this agenda over a time period.

        Tasks are distributed evenly across the specified time window,
        optionally with random jitter to prevent thundering herd effects.

        Args:
            docket: The Docket to schedule tasks on.
            over: Time period to scatter tasks over (required).
            start: When to start scattering from. Defaults to now.
            jitter: Maximum random offset to add/subtract from each scheduled time.

        Returns:
            List of Execution objects for the scheduled tasks.
        """
        if not self._tasks:
            return []

        if start is None:
            start = datetime.now(timezone.utc)

        executions: list[Execution] = []

        # Calculate even distribution over the time period
        task_count = len(self._tasks)

        if task_count == 1:
            # Single task goes in the middle of the window
            schedule_times = [start + over / 2]
        else:
            # Distribute tasks evenly across the window
            # For n tasks, we want n points from start to start+over inclusive
            interval = over / (task_count - 1)
            schedule_times = [start + interval * i for i in range(task_count)]

        # Apply jitter if specified
        if jitter:
            jittered_times: list[datetime] = []
            for schedule_time in schedule_times:
                # Random offset between -jitter and +jitter
                offset = timedelta(
                    seconds=random.uniform(
                        -jitter.total_seconds(), jitter.total_seconds()
                    )
                )
                jittered_times.append(schedule_time + offset)
            schedule_times = jittered_times

        # Schedule each task at its calculated time
        for (task_func, args, kwargs), schedule_time in zip(
            self._tasks, schedule_times
        ):
            if isinstance(task_func, str):
                scheduler = docket.add(task_func, when=schedule_time)
            else:
                scheduler = docket.add(task_func, when=schedule_time)
            execution = await scheduler(*args, **kwargs)
            executions.append(execution)

        return executions
