"""A retried task whose first run replaces its own key, then fails.

Only the first attempt replaces the key, so a retry that took the key back
would run the first run again instead of the replacement.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from docket import CurrentDocket, CurrentExecution, CurrentWorker, Docket, Retry, Worker
from docket.execution import Execution
from events import record

SCENARIO = "retry-after-replace"
WORKER: dict[str, Any] = {}


async def flaky(
    round: str,
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
    retry: Retry = Retry(attempts=2),
) -> None:
    await record(
        docket,
        SCENARIO,
        round,
        task="flaky",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )
    if round == "first":
        if execution.attempt == 1:
            later = datetime.now(timezone.utc) + timedelta(seconds=1)
            await docket.replace(flaky, later, execution.key)("replacement")
            await record(docket, SCENARIO, "replaced", task="flaky", key=execution.key)
        raise RuntimeError("the first run fails")


tasks = [flaky]


async def produce(docket: Docket) -> None:
    await docket.add(flaky, key="flaky")("first")
