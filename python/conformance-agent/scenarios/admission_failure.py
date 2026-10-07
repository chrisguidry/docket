"""A task whose concurrency limit counts by an argument the task doesn't have."""

from datetime import timedelta
from typing import Any

from docket import (
    ConcurrencyLimit,
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    Docket,
    Retry,
    Worker,
)
from docket.execution import Execution
from events import record

SCENARIO = "admission-failure"
WORKER: dict[str, Any] = {}


async def unlimited(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
    retry: Retry = Retry(attempts=3, delay=timedelta(milliseconds=100)),
    concurrency: ConcurrencyLimit = ConcurrencyLimit("customer", max_concurrent=1),
) -> None:
    await record(
        docket,
        SCENARIO,
        "ran",
        task="unlimited",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )


tasks = [unlimited]


async def produce(docket: Docket) -> None:
    await docket.add(unlimited, key="unlimited")()
