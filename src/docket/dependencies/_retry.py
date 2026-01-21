"""Retry strategies for tasks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import NoReturn

from ._base import Dependency


class ForcedRetry(Exception):
    """Raised when a task requests a retry via `in_` or `at`"""


class Retry(Dependency):
    """Configures linear retries for a task.  You can specify the total number of
    attempts (or `None` to retry indefinitely), and the delay between attempts.

    Example:

    ```python
    @task
    async def my_task(retry: Retry = Retry(attempts=3)) -> None:
        ...
    ```
    """

    single: bool = True

    attempts: int | None
    delay: timedelta
    attempt: int

    def __init__(
        self, attempts: int | None = 1, delay: timedelta = timedelta(0)
    ) -> None:
        """
        Args:
            attempts: The total number of attempts to make.  If `None`, the task will
                be retried indefinitely.
            delay: The delay between attempts.
        """
        self.attempts = attempts
        self.delay = delay
        self.attempt = 1

    async def __aenter__(self) -> Retry:
        execution = self.execution.get()
        retry = Retry(attempts=self.attempts, delay=self.delay)
        retry.attempt = execution.attempt
        return retry

    def at(self, when: datetime) -> NoReturn:
        now = datetime.now(timezone.utc)
        diff = when - now
        diff = diff if diff.total_seconds() >= 0 else timedelta(0)

        self.in_(diff)

    def in_(self, when: timedelta) -> NoReturn:
        self.delay = when
        raise ForcedRetry()


class ExponentialRetry(Retry):
    """Configures exponential retries for a task.  You can specify the total number
    of attempts (or `None` to retry indefinitely), and the minimum and maximum delays
    between attempts.

    Example:

    ```python
    @task
    async def my_task(retry: ExponentialRetry = ExponentialRetry(attempts=3)) -> None:
        ...
    ```
    """

    def __init__(
        self,
        attempts: int | None = 1,
        minimum_delay: timedelta = timedelta(seconds=1),
        maximum_delay: timedelta = timedelta(seconds=64),
    ) -> None:
        """
        Args:
            attempts: The total number of attempts to make.  If `None`, the task will
                be retried indefinitely.
            minimum_delay: The minimum delay between attempts.
            maximum_delay: The maximum delay between attempts.
        """
        super().__init__(attempts=attempts, delay=minimum_delay)
        self.maximum_delay = maximum_delay

    async def __aenter__(self) -> ExponentialRetry:
        execution = self.execution.get()

        retry = ExponentialRetry(
            attempts=self.attempts,
            minimum_delay=self.delay,
            maximum_delay=self.maximum_delay,
        )
        retry.attempt = execution.attempt

        if execution.attempt > 1:
            backoff_factor = 2 ** (execution.attempt - 1)
            calculated_delay = self.delay * backoff_factor

            if calculated_delay > self.maximum_delay:
                retry.delay = self.maximum_delay
            else:
                retry.delay = calculated_delay

        return retry
