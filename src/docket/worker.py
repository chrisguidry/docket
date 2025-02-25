from datetime import datetime, timezone
from types import TracebackType
from typing import Self

from .docket import Docket


class Worker:
    def __init__(self, docket: Docket) -> None:
        self.docket = docket

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass

    async def run_until_current(self) -> None:
        while self.docket.executions:
            for task in self.docket.executions:
                if task.when <= datetime.now(timezone.utc):
                    await task.function(*task.args, **task.kwargs)
                    self.docket.executions.remove(task)
