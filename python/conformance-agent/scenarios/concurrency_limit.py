"""Tasks that share a limit of two at a time, across every worker."""

import asyncio
from typing import Any

from docket import CurrentDocket, CurrentExecution, CurrentWorker, Docket, Worker
from docket.dependencies import ConcurrencyLimit
from docket.execution import Execution
from events import record

SCENARIO = "concurrency-limit"
WORKER: dict[str, Any] = {"concurrency": 4}

TASKS = 12
TASK_SECONDS = 0.5


async def limited(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
    concurrency: ConcurrencyLimit = ConcurrencyLimit(max_concurrent=2),
) -> None:
    await record(
        docket,
        SCENARIO,
        "started",
        task="limited",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )
    await asyncio.sleep(TASK_SECONDS)
    await record(
        docket,
        SCENARIO,
        "finished",
        task="limited",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )


tasks = [limited]


async def produce(docket: Docket) -> None:
    for number in range(TASKS):
        await docket.add(limited, key=f"limited-{number}")()
