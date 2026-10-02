"""Producers that add and then replace the same key at about the same time.

Each producer adds ``shared`` 2 s out, which does nothing when the key is
already scheduled, then replaces it 4 s out.  The last replace wins.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from docket import CurrentDocket, CurrentExecution, CurrentWorker, Docket, Worker
from docket.execution import Execution
from events import record

SCENARIO = "same-key"
WORKER: dict[str, Any] = {}


async def shared(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
) -> None:
    await record(
        docket,
        SCENARIO,
        "ran",
        task="shared",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )


tasks = [shared]


async def produce(docket: Docket) -> None:
    now = datetime.now(timezone.utc)
    await docket.add(shared, when=now + timedelta(seconds=2), key="shared")()
    await docket.replace(shared, when=now + timedelta(seconds=4), key="shared")()
    await record(docket, SCENARIO, "replaced", task="shared", key="shared")
