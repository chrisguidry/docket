"""Whether a retry gives way to a replace made while its run ran.

``Retry.handle_failure`` reschedules under the generation of the attempt
that failed, so a newer replace of the key keeps its own time and arguments.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, call

import pytest
from opentelemetry.metrics import Counter

from docket import (
    CurrentDocket,
    CurrentExecution,
    Docket,
    ExecutionState,
    Retry,
    Worker,
)
from docket.execution import Execution
from tests.conftest import wait_until

TERMINAL_STATES = (
    ExecutionState.COMPLETED,
    ExecutionState.FAILED,
    ExecutionState.CANCELLED,
)


async def test_a_retry_gives_way_to_a_replacement(
    docket: Docket, worker: Worker, monkeypatch: pytest.MonkeyPatch
):
    """A run replaced while it ran does not retry over its replacement."""
    superseded = Mock(spec=Counter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_SUPERSEDED.add", superseded)

    runs: list[tuple[str, int, datetime]] = []
    replacement_due = datetime.now(timezone.utc) + timedelta(milliseconds=500)

    async def flaky(
        round: str,
        retry: Retry = Retry(attempts=2),
        docket: Docket = CurrentDocket(),
        execution: Execution = CurrentExecution(),
    ):
        runs.append((round, execution.attempt, datetime.now(timezone.utc)))
        if round == "first":
            # A second attempt of the first run comes only from a retry that
            # took the key back, and it must not replace the key again.
            if execution.attempt == 1:  # pragma: no branch
                await docket.replace(flaky, replacement_due, "flaky")("replacement")
            raise ValueError("the first run fails")

    await docket.add(flaky, key="flaky")("first")
    await worker.run_until_finished()

    assert [(round, attempt) for round, attempt, _ in runs] == [
        ("first", 1),
        ("replacement", 1),
    ]
    assert runs[1][2] >= replacement_due
    assert superseded.call_args_list == [
        call(
            1,
            {
                "docket.name": docket.name,
                "docket.worker": worker.name,
                "docket.task": "flaky",
                "docket.where": "retry",
            },
        )
    ]


async def test_a_retry_runs_again_after_a_replacement_that_already_finished(
    zero_ttl_docket: Docket,
):
    """With no execution_ttl, a replacement that finishes first deletes the
    key's runs hash, and the failed run then retries.  Redis cannot tell this
    hash apart from one that an eviction or a 0.26.2 ``clear()`` removed,
    and those runs retried in 0.26.2, so a missing hash lets the retry go on."""
    docket = zero_ttl_docket
    runs: list[tuple[str, int]] = []

    async def replacement_finished() -> bool:
        return await docket.get_execution("flaky") is None

    async def flaky(
        round: str,
        retry: Retry = Retry(attempts=2),
        docket: Docket = CurrentDocket(),
        execution: Execution = CurrentExecution(),
    ):
        runs.append((round, execution.attempt))
        if round == "first":
            if execution.attempt == 1:
                now = datetime.now(timezone.utc)
                await docket.replace(flaky, now, "flaky")("replacement")
                await wait_until(replacement_finished, description="replacement ran")
            raise ValueError("the first run fails")

    await docket.add(flaky, key="flaky")("first")
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await asyncio.wait_for(worker.run_until_finished(), timeout=30)

    assert runs == [("first", 1), ("replacement", 1), ("first", 2)]


async def test_a_retry_that_gives_way_publishes_no_ending(
    docket: Docket, worker: Worker
):
    """A run that a replace superseded ends without a state event, so a
    caller waiting on the key waits for the replacement, not the failure."""

    async def flaky(
        round: str,
        retry: Retry = Retry(attempts=2),
        docket: Docket = CurrentDocket(),
    ):
        if round == "first":
            soon = datetime.now(timezone.utc) + timedelta(milliseconds=200)
            await docket.replace(flaky, soon, "flaky")("replacement")
            raise ValueError("the first run fails")

    execution = await docket.add(flaky, key="flaky")("first")
    subscribed = asyncio.Event()

    async def first_ending() -> ExecutionState:
        async for event in execution.subscribe(ready=subscribed):
            if event["type"] == "state":
                state = ExecutionState(event["state"])
                if state in TERMINAL_STATES:
                    return state
        raise AssertionError("the events ended")  # pragma: no cover

    ending = asyncio.create_task(first_ending())
    await asyncio.wait_for(subscribed.wait(), timeout=10)
    await worker.run_until_finished()

    assert await asyncio.wait_for(ending, timeout=10) == ExecutionState.COMPLETED
