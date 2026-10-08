"""A perpetual task whose first run replaces its own key, then stops itself.

The replacement stops the task too, so the key runs exactly twice.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from docket import CurrentDocket, CurrentExecution, CurrentWorker, Docket, Worker
from docket.dependencies import Perpetual
from docket.execution import Execution
from events import record

SCENARIO = "stop-after-replace"
WORKER: dict[str, Any] = {}


async def stopping(
    round: str,
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
    perpetual: Perpetual = Perpetual(every=timedelta(hours=1)),
) -> None:
    await record(
        docket,
        SCENARIO,
        round,
        task="stopping",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )
    if round == "first":
        later = datetime.now(timezone.utc) + timedelta(seconds=1)
        await docket.replace(stopping, later, execution.key)("replacement")
        await record(docket, SCENARIO, "replaced", task="stopping", key=execution.key)
    perpetual.cancel()


tasks = [stopping]


async def produce(docket: Docket) -> None:
    await docket.add(stopping, key="stopping")("first")
