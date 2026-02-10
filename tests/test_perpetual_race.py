"""Tests for the Perpetual rescheduling race condition documented in RACE.md.

When a Perpetual task is running and an external caller uses docket.replace() to
force immediate re-execution of the same key, two executions run concurrently.
Both call Perpetual.on_complete() → docket.replace() on completion, and the last
one to finish wins — potentially with stale timing data.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from docket import CurrentExecution, Docket, Perpetual, Worker
from docket.execution import Execution

TASK_KEY = "perpetual-race-test"

STALE_INTERVAL = timedelta(seconds=2)
CORRECT_INTERVAL = timedelta(milliseconds=500)


@pytest.mark.xfail(
    reason="Perpetual.on_complete has no supersession check; "
    "stale execution overwrites correct successor (see RACE.md)",
    strict=True,
)
async def test_stale_perpetual_on_complete_overwrites_correct_successor(
    docket: Docket, worker: Worker
):
    """When a running Perpetual task is externally replaced and finishes after
    the replacement, its on_complete overwrites the correctly-timed successor."""

    config: dict[str, timedelta] = {"interval": STALE_INTERVAL}

    task_a_started = asyncio.Event()
    let_a_finish = asyncio.Event()
    task_b_started = asyncio.Event()
    let_b_finish = asyncio.Event()

    executions: list[Execution] = []

    async def racing_task(
        perpetual: Perpetual = Perpetual(),
        execution: Execution = CurrentExecution(),
    ):
        my_interval = config["interval"]
        call_number = len(executions) + 1
        executions.append(execution)

        if call_number == 1:
            # Task A: signal start, block until released
            task_a_started.set()
            await asyncio.wait_for(let_a_finish.wait(), timeout=10)
        elif call_number == 2:
            # Task B: signal start, block until released
            task_b_started.set()
            await asyncio.wait_for(let_b_finish.wait(), timeout=10)
        # Task C (call 3): the successor — just runs

        perpetual.after(my_interval)

    # Schedule the initial task (A)
    await docket.add(racing_task, key=TASK_KEY)()

    # Run the worker in the background (allow 3 executions of this key)
    worker_task = asyncio.create_task(worker.run_at_most({TASK_KEY: 3}))

    # Wait for task A to start executing
    await asyncio.wait_for(task_a_started.wait(), timeout=10)

    # Simulate user changing config to a shorter interval
    config["interval"] = CORRECT_INTERVAL

    # External replace: force immediate re-execution (creates task B)
    replace_time = datetime.now(timezone.utc)
    await docket.replace(racing_task, replace_time, TASK_KEY)()

    # Wait for task B to start
    await asyncio.wait_for(task_b_started.wait(), timeout=10)

    # Let B finish first — B's on_complete schedules successor at B_time + 500ms
    let_b_finish.set()
    # Give B's on_complete time to complete
    await asyncio.sleep(0.05)

    # Now let A finish — A's on_complete overwrites B's successor with
    # A_start + 2s (stale), pushing the successor much further out
    let_a_finish.set()

    # Wait for all 3 executions to complete
    await asyncio.wait_for(worker_task, timeout=15)

    assert len(executions) == 3

    # The third execution's `when` tells us which on_complete won the race.
    #
    # If B's on_complete won (correct): when ≈ B_completion + 500ms  (< 1s from replace)
    # If A's on_complete won (stale):   when ≈ A_completion + 2s     (> 1s from replace)
    successor = executions[2]
    gap = successor.when - replace_time

    assert gap < timedelta(seconds=1), (
        f"Stale execution won the race: successor scheduled "
        f"{gap.total_seconds():.2f}s after the correct replacement, "
        f"expected < 1s (correct interval is {CORRECT_INTERVAL})"
    )
