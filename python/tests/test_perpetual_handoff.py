"""Tests proving a fresh worker recovers a Perpetual chain another worker left
pending.

``test_perpetual_reliability.py`` shows the worker that hit a Redis failure
recovering its own chain after it reconnects.  These tests stop that worker
instead, the way a deploy would, and start a second worker with no
in-memory state from the first.  The chain continues because everything it
needs is in Redis: the message stays in the consumer-group pending list until
the second worker's redelivery sweep reclaims it via XAUTOCLAIM, the body runs
again, and ``on_complete`` schedules the next run.

Each test runs the first worker inside its failure patch and stops it by
leaving the worker's context, the way a shutdown would.  The patch stays in
force through the worker's drain, so no acknowledgement can land on the way
out.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Awaitable, Callable
from unittest.mock import patch

from redis.exceptions import ConnectionError

from docket import Docket, Perpetual, Worker
from docket.dependencies import TaskOutcome
from docket.execution import Execution, TaskFunction
from tests.conftest import wait_until


async def crashing_on_complete(
    self: Perpetual, execution: Execution, outcome: TaskOutcome
) -> bool:
    raise ConnectionError("simulated Redis outage during on_complete")


def crashing_replace(
    self: Docket,
    function: TaskFunction | str,
    when: datetime,
    key: str,
    expected_generation: int = 0,
) -> Callable[..., Awaitable[Execution]]:
    raise ConnectionError("simulated Redis outage during docket.replace")


def first_worker(docket: Docket) -> Worker:
    """A worker whose own redelivery sweep cannot reclaim the message it failed
    to acknowledge, so only a second worker can recover the chain."""
    return Worker(
        docket,
        redelivery_timeout=timedelta(minutes=1),
        reconnection_delay=timedelta(milliseconds=50),
    )


async def run_until_first_execution(
    worker: Worker, executions: list[int]
) -> asyncio.Task[None]:
    """Start ``worker`` and return once its task body has run one time.

    The caller then leaves the worker's context, which stops the worker the
    way a shutdown would.  The acknowledgement fails under the caller's patch,
    so the message stays pending in Redis for another worker."""
    run = asyncio.create_task(worker.run_forever())
    await wait_until(
        lambda: len(executions) >= 1, description="the task body to run once"
    )
    return run


async def test_fresh_worker_recovers_after_on_complete_failure(
    docket: Docket,
):
    """``Perpetual.on_complete`` fails after a successful body for as long as
    the first worker runs, so its message is never acknowledged.  A second
    worker reclaims it, the body runs again, ``on_complete`` succeeds, and the
    chain continues."""
    executions: list[int] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=20)),
    ):
        executions.append(1)

    await docket.add(perpetual_task, key="perpetual")()

    with patch.object(Perpetual, "on_complete", crashing_on_complete):
        async with first_worker(docket) as worker_a:
            run = await run_until_first_execution(worker_a, executions)
        await run
    assert len(executions) == 1  # the body ran once and its ack never landed

    await asyncio.sleep(0.25)  # past worker_b's redelivery_timeout

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_b:
        await worker_b.run_at_most({"perpetual": 3})
    assert len(executions) >= 3


async def test_fresh_worker_recovers_after_on_complete_failure_in_failure_path(
    docket: Docket,
):
    """The body raises (no ``Retry``) and ``on_complete`` fails from the
    failure-handling branch for as long as the first worker runs.  A second
    worker reclaims the message, the body raises again, ``on_complete`` now
    schedules the next run, and the chain continues."""
    executions: list[int] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=20)),
    ):
        executions.append(1)
        raise ValueError("simulated body failure")

    await docket.add(perpetual_task, key="perpetual")()

    with patch.object(Perpetual, "on_complete", crashing_on_complete):
        async with first_worker(docket) as worker_a:
            run = await run_until_first_execution(worker_a, executions)
        await run
    assert len(executions) == 1

    await asyncio.sleep(0.25)

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_b:
        await worker_b.run_at_most({"perpetual": 3})
    assert len(executions) >= 3


async def test_fresh_worker_recovers_after_replace_failure(
    docket: Docket,
):
    """The ``Docket._replace`` call that ``on_complete`` makes to schedule the
    next run fails for as long as the first worker runs.  A second worker
    reclaims the message, ``replace`` works again, and the chain continues."""
    executions: list[int] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=20)),
    ):
        executions.append(1)

    await docket.add(perpetual_task, key="perpetual")()

    with patch.object(Docket, "_replace", crashing_replace):
        async with first_worker(docket) as worker_a:
            run = await run_until_first_execution(worker_a, executions)
        await run
    assert len(executions) == 1

    await asyncio.sleep(0.25)

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_b:
        await worker_b.run_at_most({"perpetual": 3})
    assert len(executions) >= 3
