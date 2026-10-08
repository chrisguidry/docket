"""Whether a retry gives way to a replace made while its run ran.

``Retry.handle_failure`` reschedules under the generation of the attempt
that failed, so a newer replace of the key keeps its own time and arguments.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, call

import pytest
from opentelemetry.metrics import Counter

from docket import CurrentDocket, CurrentExecution, Docket, Retry, Worker
from docket.execution import Execution
from tests.conftest import wait_until


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


async def test_a_retry_gives_way_to_a_replacement_that_already_finished(
    zero_ttl_docket: Docket,
):
    """With no execution_ttl, a replacement that finishes first deletes the
    key's runs hash, and the failed run still does not retry over it."""
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

    assert runs == [("first", 1), ("replacement", 1)]
