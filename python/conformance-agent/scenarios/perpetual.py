"""An automatic perpetual task, which the worker schedules when it starts."""

from datetime import timedelta
from typing import Any

from docket import CurrentDocket, CurrentExecution, CurrentWorker, Docket, Worker
from docket.dependencies import Perpetual
from docket.execution import Execution
from events import record

SCENARIO = "perpetual"
# The driver can kill the worker in the middle of a run, and the new worker
# takes the task over only when the dead worker's lease runs out.
WORKER: dict[str, Any] = {"redelivery_timeout": timedelta(seconds=2)}


async def tick(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
    perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=500), automatic=True),
) -> None:
    await record(
        docket,
        SCENARIO,
        "ran",
        task="tick",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )


tasks = [tick]


async def produce(docket: Docket) -> None:
    """Nothing to schedule: the worker schedules ``tick`` when it starts."""
