"""A task that fails every attempt, with exponential backoff between them."""

from datetime import timedelta
from typing import Any

from docket import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    Docket,
    ExponentialRetry,
    Worker,
)
from docket.execution import Execution
from events import record

SCENARIO = "backoff"
WORKER: dict[str, Any] = {}


async def flaky(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
    retry: ExponentialRetry = ExponentialRetry(
        attempts=4, minimum_delay=timedelta(milliseconds=500)
    ),
) -> None:
    await record(
        docket,
        SCENARIO,
        "attempt",
        task="flaky",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )
    raise RuntimeError("flaky fails every attempt")


tasks = [flaky]


async def produce(docket: Docket) -> None:
    await docket.add(flaky, key="flaky")()
