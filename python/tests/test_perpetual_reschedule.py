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

from docket import CurrentDocket, Docket, ExecutionState, Perpetual, Worker
from tests.conftest import wait_until

TERMINAL_STATES = (
    ExecutionState.COMPLETED,
    ExecutionState.FAILED,
    ExecutionState.CANCELLED,
)


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


async def test_a_perpetual_that_stops_itself_keeps_a_replacement(
    docket: Docket, worker: Worker, monkeypatch: pytest.MonkeyPatch
):
    """A Perpetual that cancels itself leaves alone a replace made while it ran."""
    superseded = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_SUPERSEDED.add", superseded)

    runs: list[str] = []

    async def stopping_perpetual(
        round: str,
        perpetual: Perpetual = Perpetual(every=timedelta(hours=1)),
        docket: Docket = CurrentDocket(),
    ):
        runs.append(round)
        if round == "first":
            soon = datetime.now(timezone.utc) + timedelta(milliseconds=200)
            await docket.replace(stopping_perpetual, soon, "stopping")("replacement")
        perpetual.cancel()

    await docket.add(stopping_perpetual, key="stopping")("first")
    await worker.run_until_finished()

    assert runs == ["first", "replacement"]
    assert superseded.call_args_list == [
        call(
            1,
            {
                "docket.name": docket.name,
                "docket.worker": worker.name,
                "docket.task": "stopping_perpetual",
                "docket.where": "on_complete",
            },
        )
    ]


@pytest.mark.parametrize("stops", [True, False], ids=["stops", "reschedules"])
@pytest.mark.parametrize("fails", [False, True], ids=["succeeds", "fails"])
async def test_a_perpetual_that_gives_way_publishes_no_ending(
    docket: Docket, worker: Worker, stops: bool, fails: bool
):
    """A run that a replace superseded ends without a state event, so a
    caller waiting on the key waits for the replacement, not the old run."""
    runs: list[str] = []

    async def giving_way(
        round: str,
        perpetual: Perpetual = Perpetual(every=timedelta(hours=1)),
        docket: Docket = CurrentDocket(),
    ):
        runs.append(round)
        if round == "replacement":
            perpetual.cancel()
            return
        soon = datetime.now(timezone.utc) + timedelta(milliseconds=200)
        await docket.replace(giving_way, soon, "giving-way")("replacement")
        if stops:
            perpetual.cancel()
        if fails:
            raise ValueError("the first run fails")

    execution = await docket.add(giving_way, key="giving-way")("first")
    subscribed = asyncio.Event()

    async def runs_at_first_ending() -> list[str]:
        async for event in execution.subscribe(ready=subscribed):
            if (
                event["type"] == "state"
                and ExecutionState(event["state"]) in TERMINAL_STATES
            ):
                return list(runs)
        raise AssertionError("the events ended")  # pragma: no cover

    ending = asyncio.create_task(runs_at_first_ending())
    await asyncio.wait_for(subscribed.wait(), timeout=10)
    await asyncio.wait_for(worker.run_until_finished(), timeout=30)

    assert await asyncio.wait_for(ending, timeout=10) == ["first", "replacement"]


async def test_a_perpetual_goes_on_when_its_runs_hash_disappears(
    docket: Docket, worker: Worker
):
    """A Perpetual whose runs hash is gone when it ends still schedules its
    next run, as in 0.26.2.  An eviction, or a 0.26.2 ``clear()`` during a
    rolling upgrade, can remove the hash of a task that is running."""
    runs: list[int] = []

    async def evicted_perpetual(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=10)),
        docket: Docket = CurrentDocket(),
    ):
        runs.append(len(runs) + 1)
        if len(runs) == 1:
            async with docket.redis() as redis:
                await redis.delete(docket.runs_key("evicted"))
        else:
            perpetual.cancel()

    await docket.add(evicted_perpetual, key="evicted")()
    await asyncio.wait_for(worker.run_until_finished(), timeout=10)

    assert runs == [1, 2]
