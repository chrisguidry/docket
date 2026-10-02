"""A future task cancelled before it is due, then a task due after it.

When the driver sees the sentinel run, the doomed task's time has passed, so a
cancel that did not work shows up as a run of the doomed task.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from docket import CurrentDocket, CurrentExecution, CurrentWorker, Docket, Worker
from docket.execution import Execution
from events import record

SCENARIO = "cancel-before-start"
WORKER: dict[str, Any] = {}


async def doomed(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
) -> None:
    await record(
        docket,
        SCENARIO,
        "ran",
        task="doomed",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )


async def sentinel(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
) -> None:
    await record(
        docket,
        SCENARIO,
        "ran",
        task="sentinel",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )


tasks = [doomed, sentinel]


async def produce(docket: Docket) -> None:
    now = datetime.now(timezone.utc)
    await docket.add(doomed, when=now + timedelta(seconds=1.5), key="doomed")()
    await docket.add(sentinel, when=now + timedelta(seconds=2.5), key="sentinel")()
    await docket.cancel("doomed")
    await record(docket, SCENARIO, "cancelled", task="doomed", key="doomed")
