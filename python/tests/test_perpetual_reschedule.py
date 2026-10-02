"""Whether a finished Perpetual schedules its own next run.

``Perpetual.on_complete`` reschedules under the generation of the attempt
that just ran, so one script both checks who holds the key and, when nobody
else has taken it, schedules the successor.
"""

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, call

import pytest
from opentelemetry.metrics import Counter

from docket import Docket, Perpetual, Worker
from tests.conftest import wait_until


async def test_a_replaced_perpetual_does_not_reschedule_itself(
    docket: Docket, worker: Worker, monkeypatch: pytest.MonkeyPatch
):
    """A Perpetual replaced while it runs leaves the replacement's time alone."""
    superseded = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_SUPERSEDED.add", superseded)

    running = asyncio.Event()
    finish = asyncio.Event()

    async def slow_perpetual(
        perpetual: Perpetual = Perpetual(every=timedelta(hours=1)),
    ):
        running.set()
        await asyncio.wait_for(finish.wait(), timeout=10)

    key = "replaced-mid-run"
    await docket.add(slow_perpetual, key=key)()

    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(running.wait(), timeout=10)

    replacement = datetime.now(timezone.utc) + timedelta(hours=3)
    await docket.replace(slow_perpetual, replacement, key)()
    finish.set()

    await wait_until(lambda: superseded.called, description="the superseded branch")
    worker_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await worker_task

    snapshot = await docket.snapshot()
    assert [(e.key, e.when) for e in snapshot.future] == [(key, replacement)]
    assert superseded.call_args == call(
        1,
        {
            "docket.name": docket.name,
            "docket.worker": worker.name,
            "docket.task": "slow_perpetual",
            "docket.where": "on_complete",
        },
    )


async def test_an_untouched_perpetual_reschedules_itself(
    docket: Docket, worker: Worker
):
    """A Perpetual nobody replaced schedules its own next run."""
    finished = asyncio.Event()

    async def hourly_perpetual(
        perpetual: Perpetual = Perpetual(every=timedelta(hours=1)),
    ):
        finished.set()

    key = "untouched"
    before = datetime.now(timezone.utc)
    await docket.add(hourly_perpetual, key=key)()

    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(finished.wait(), timeout=10)

    async def successor_is_queued() -> bool:
        return bool((await docket.snapshot()).future)

    await wait_until(successor_is_queued, description="the successor's schedule")
    worker_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await worker_task

    snapshot = await docket.snapshot()
    (successor,) = snapshot.future
    assert successor.key == key
    assert (
        before + timedelta(minutes=59)
        < successor.when
        < before + timedelta(hours=1, minutes=1)
    )
