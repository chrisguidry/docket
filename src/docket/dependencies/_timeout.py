"""Timeout dependency for tasks."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Awaitable, Callable

if TYPE_CHECKING:  # pragma: no cover
    from ..execution import Execution

from .._cancellation import cancel_task
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
        task = asyncio.create_task(
            function(*args, **kwargs),  # type: ignore[arg-type]
            name=f"{docket.name} - task:{execution.key}",
        )

        timed_out = False
        try:
            while not task.done():  # pragma: no branch
                if self.expired():
                    timed_out = True
                    break

                try:
                    return await asyncio.wait_for(
                        asyncio.shield(task), timeout=self.remaining().total_seconds()
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            if not task.done():
                timeout_reason = (
                    f"Docket task {execution.key} exceeded "
                    f"timeout of {self.base.total_seconds()}s"
                )
                await cancel_task(task, timeout_reason)
                if timed_out:  # pragma: no branch
                    raise asyncio.TimeoutError(timeout_reason)
                # Otherwise let the original exception (e.g. CancelledError) propagate
