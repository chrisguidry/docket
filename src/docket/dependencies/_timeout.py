"""Timeout dependency for tasks."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Coroutine, cast

if TYPE_CHECKING:  # pragma: no cover
    from ..execution import Execution

from .._cancellation import CANCEL_MSG_TIMEOUT
from ._base import Runtime


class Timeout(Runtime):
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
        return Timeout(base=self.base)

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

    async def run(
        self,
        execution: Execution,
        function: Callable[..., Awaitable[Any]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        """Execute the function with timeout enforcement."""
        self.start()

        docket = self.docket.get()
        task_coro = cast(
            Coroutine[None, None, Any],
            function(*args, **kwargs),
        )
        task = asyncio.create_task(
            task_coro,
            name=f"{docket.name} - task:{execution.key}",
        )

        # Track whether WE cancelled for timeout (vs external cancellation)
        # We use a flag because Python 3.10 doesn't propagate cancel messages
        # to the awaiter, only Python 3.11+ does
        cancelled_for_timeout = False
        try:
            while not task.done():  # pragma: no branch
                remaining = self.remaining().total_seconds()
                if self.expired():
                    cancelled_for_timeout = True
                    task.cancel(CANCEL_MSG_TIMEOUT)
                    break

                try:
                    return await asyncio.wait_for(
                        asyncio.shield(task), timeout=remaining
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            if not task.done():  # pragma: no branch
                cancelled_for_timeout = True
                task.cancel(CANCEL_MSG_TIMEOUT)

        try:
            return await task
        except asyncio.CancelledError:
            if cancelled_for_timeout:
                raise asyncio.TimeoutError
            raise  # pragma: no cover - External cancellation propagation
