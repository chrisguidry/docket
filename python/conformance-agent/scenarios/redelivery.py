"""A slow task whose worker the driver kills while the task runs."""

import asyncio
from datetime import timedelta
from typing import Any

from docket import CurrentDocket, CurrentExecution, CurrentWorker, Docket, Worker
from docket.execution import Execution
from events import record

SCENARIO = "redelivery"
# Another worker takes the task over once the dead worker's lease runs out.
WORKER: dict[str, Any] = {"redelivery_timeout": timedelta(seconds=2)}

TASK_SECONDS = 3


async def interrupted(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
) -> None:
    await record(
        docket,
        SCENARIO,
        "started",
        task="interrupted",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )
    await asyncio.sleep(TASK_SECONDS)
    await record(
        docket,
        SCENARIO,
        "finished",
        task="interrupted",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )


tasks = [interrupted]


async def produce(docket: Docket) -> None:
    await docket.add(interrupted, key="interrupted")()
