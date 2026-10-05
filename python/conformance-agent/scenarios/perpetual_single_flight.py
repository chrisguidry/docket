"""An automatic perpetual task that several workers share."""

import asyncio
from datetime import timedelta
from typing import Any

from docket import CurrentDocket, CurrentExecution, CurrentWorker, Docket, Worker
from docket.dependencies import Perpetual
from docket.execution import Execution
from events import record

SCENARIO = "perpetual-single-flight"
# The driver kills a worker while it runs the task, and another worker takes
# the task over once the dead worker's lease runs out.
WORKER: dict[str, Any] = {"redelivery_timeout": timedelta(seconds=2)}

TASK_SECONDS = 0.2


async def beat(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
    perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=500), automatic=True),
) -> None:
    await record(
        docket,
        SCENARIO,
        "started",
        task="beat",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )
    await asyncio.sleep(TASK_SECONDS)
    await record(
        docket,
        SCENARIO,
        "finished",
        task="beat",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )


tasks = [beat]


async def produce(docket: Docket) -> None:
    """Nothing to schedule: each worker schedules ``beat`` when it starts."""
