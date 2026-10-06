"""A small workload that touches every metric and span the driver checks.

The driver runs this workload once for each phase, and ``CONFORMANCE_PHASE``
names the phase.  Every key starts with the phase and a colon, such as
``otlp:succeed``, so the phases never share a key.  With ``P`` for that
prefix, the producer does these steps, in this order:

1. It adds ``succeed``, ``fail``, ``flaky``, and ``perpetual`` to run now,
   with the keys ``P:succeed``, ``P:fail``, ``P:flaky``, and ``P:perpetual``.
2. It adds ``doomed`` to run in one hour with the key ``P:doomed``, then
   cancels ``P:doomed``.
3. It adds ``later`` to run in one hour with the key ``P:later``, then
   replaces ``P:later`` to run now.
4. It strikes the whole ``struck`` task, adds ``struck`` with the key
   ``P:struck``, which the strike blocks, then restores ``struck``.

The tasks:

- ``succeed`` returns.
- ``fail`` raises an error with the message ``boom``.
- ``flaky`` allows two attempts with no delay.  It raises ``boom`` on attempt
  1 and returns on attempt 2.
- ``perpetual`` runs every 100 ms, and is not automatic.  It counts its runs
  with HINCRBY on the hash ``conformance:telemetry:{docket}:runs``, with the
  task's key as the field.  On its third run it cancels itself.
- ``later``, ``doomed``, and ``struck`` return.  ``doomed`` and ``struck``
  never run.

Every run records a ``ran`` event: ``succeed`` once, ``fail`` once, ``flaky``
twice, ``perpetual`` three times, and ``later`` once.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from docket import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    Docket,
    Perpetual,
    Retry,
    Worker,
)
from docket.execution import Execution
from events import record

SCENARIO = "telemetry"
WORKER: dict[str, Any] = {}
PERPETUAL_RUNS = 3


async def ran(docket: Docket, execution: Execution, worker: Worker) -> None:
    await record(
        docket,
        SCENARIO,
        "ran",
        task=execution.function_name,
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )


async def succeed(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
) -> None:
    await ran(docket, execution, worker)


async def fail(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
) -> None:
    await ran(docket, execution, worker)
    raise RuntimeError("boom")


async def flaky(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
    retry: Retry = Retry(attempts=2),
) -> None:
    await ran(docket, execution, worker)
    if execution.attempt == 1:
        raise RuntimeError("boom")


async def perpetual(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
    perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=100)),
) -> None:
    await ran(docket, execution, worker)
    # Each run is a new execution, so only Redis can count the runs.  The
    # driver gives each implementation its own docket and the same keys, so
    # the counter belongs to the docket.
    async with docket.redis() as redis:
        runs = await redis.hincrby(
            f"conformance:{SCENARIO}:{docket.name}:runs", execution.key
        )
    if runs >= PERPETUAL_RUNS:
        perpetual.cancel()


async def later(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
) -> None:
    await ran(docket, execution, worker)


async def doomed(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
) -> None:
    await ran(docket, execution, worker)


async def struck(
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
) -> None:
    await ran(docket, execution, worker)


tasks = [succeed, fail, flaky, perpetual, later, doomed, struck]


async def produce(docket: Docket) -> None:
    prefix = os.environ["CONFORMANCE_PHASE"]
    now = datetime.now(timezone.utc)
    an_hour_from_now = now + timedelta(hours=1)

    for task in (succeed, fail, flaky, perpetual):
        await docket.add(task, key=f"{prefix}:{task.__name__}")()

    await docket.add(doomed, when=an_hour_from_now, key=f"{prefix}:doomed")()
    await docket.cancel(f"{prefix}:doomed")

    await docket.add(later, when=an_hour_from_now, key=f"{prefix}:later")()
    await docket.replace(later, when=now, key=f"{prefix}:later")()

    await docket.strike(struck)
    await docket.add(struck, key=f"{prefix}:struck")()
    await docket.restore(struck)
