"""Slow tasks that a worker must finish after SIGTERM or SIGINT."""

import asyncio
from typing import Any
from uuid import uuid4

from docket import CurrentDocket, CurrentExecution, CurrentWorker, Docket, Worker
from docket.execution import Execution
from events import record

SCENARIO = "graceful-drain"
WORKER: dict[str, Any] = {"concurrency": 2}

TASKS_PER_PRODUCE = 4
TASK_SECONDS = 3


async def slow(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
) -> None:
    await record(
        docket,
        SCENARIO,
        "started",
        task="slow",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )
    await asyncio.sleep(TASK_SECONDS)
    await record(
        docket,
        SCENARIO,
        "finished",
        task="slow",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )


tasks = [slow]


async def produce(docket: Docket) -> None:
    for _ in range(TASKS_PER_PRODUCE):
        await docket.add(slow, key=f"slow-{uuid4()}")()
