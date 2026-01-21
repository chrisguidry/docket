"""Progress reporting dependency."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._base import Dependency

if TYPE_CHECKING:  # pragma: no cover
    from ..execution import ExecutionProgress


class Progress(Dependency):
    """A dependency to report progress updates for the currently executing task.

    Tasks can use this to report their current progress (current/total values) and
    status messages to external observers.

    Example:

    ```python
    @task
    async def process_records(records: list, progress: Progress = Progress()) -> None:
        await progress.set_total(len(records))
        for i, record in enumerate(records):
            await process(record)
            await progress.increment()
            await progress.set_message(f"Processed {record.id}")
    ```
    """

    def __init__(self) -> None:
        self._progress: ExecutionProgress | None = None

    async def __aenter__(self) -> Progress:
        execution = self.execution.get()
        self._progress = execution.progress
        return self

    @property
    def current(self) -> int | None:
        """Current progress value."""
        assert self._progress is not None, "Progress must be used as a dependency"
        return self._progress.current

    @property
    def total(self) -> int:
        """Total/target value for progress tracking."""
        assert self._progress is not None, "Progress must be used as a dependency"
        return self._progress.total

    @property
    def message(self) -> str | None:
        """User-provided status message."""
        assert self._progress is not None, "Progress must be used as a dependency"
        return self._progress.message

    async def set_total(self, total: int) -> None:
        """Set the total/target value for progress tracking."""
        assert self._progress is not None, "Progress must be used as a dependency"
        await self._progress.set_total(total)

    async def increment(self, amount: int = 1) -> None:
        """Atomically increment the current progress value."""
        assert self._progress is not None, "Progress must be used as a dependency"
        await self._progress.increment(amount)

    async def set_message(self, message: str | None) -> None:
        """Update the progress status message."""
        assert self._progress is not None, "Progress must be used as a dependency"
        await self._progress.set_message(message)
