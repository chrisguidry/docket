"""Timeout dependency for tasks."""

from __future__ import annotations

import time
from datetime import timedelta

from ._base import Dependency


class Timeout(Dependency):
    """Configures a timeout for a task.  You can specify the base timeout, and the
    task will be cancelled if it exceeds this duration.  The timeout may be extended
    within the context of a single running task.

    Example:

    ```python
    @task
    async def my_task(timeout: Timeout = Timeout(timedelta(seconds=10))) -> None:
        ...
    ```
    """

    single: bool = True

    base: timedelta
    _deadline: float

    def __init__(self, base: timedelta) -> None:
        """
        Args:
            base: The base timeout duration.
        """
        self.base = base

    async def __aenter__(self) -> Timeout:
        timeout = Timeout(base=self.base)
        timeout.start()
        return timeout

    def start(self) -> None:
        self._deadline = time.monotonic() + self.base.total_seconds()

    def expired(self) -> bool:
        return time.monotonic() >= self._deadline

    def remaining(self) -> timedelta:
        """Get the remaining time until the timeout expires."""
        return timedelta(seconds=self._deadline - time.monotonic())

    def extend(self, by: timedelta | None = None) -> None:
        """Extend the timeout by a given duration.  If no duration is provided, the
        base timeout will be used.

        Args:
            by: The duration to extend the timeout by.
        """
        if by is None:
            by = self.base
        self._deadline += by.total_seconds()
