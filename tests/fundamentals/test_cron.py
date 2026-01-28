"""Tests for Cron dependency (cron-style scheduled tasks)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from docket import Docket, Worker
from docket.dependencies import Cron


async def test_cron_task_reschedules_itself(docket: Docket, worker: Worker):
    """Cron tasks automatically reschedule after each execution."""
    runs = 0

    async def my_cron_task(cron: Cron = Cron("0 9 * * *", automatic=False)):
        nonlocal runs
        runs += 1

    # Patch get_next to return a time 10ms in the future (instead of waiting for 9 AM)
    with patch.object(
        Cron,
        "get_next",
        return_value=datetime.now(timezone.utc) + timedelta(milliseconds=10),
    ):
        execution = await docket.add(my_cron_task)()
        await worker.run_at_most({execution.key: 3})

    assert runs == 3


async def test_cron_tasks_are_automatically_scheduled(docket: Docket, worker: Worker):
    """Cron tasks with automatic=True are scheduled at worker startup."""
    calls = 0

    async def my_automatic_cron(
        cron: Cron = Cron("0 0 * * *"),
    ):  # automatic=True is default
        nonlocal calls
        calls += 1

    docket.register(my_automatic_cron)

    with patch.object(
        Cron,
        "get_next",
        return_value=datetime.now(timezone.utc) + timedelta(milliseconds=10),
    ):
        await worker.run_at_most({"my_automatic_cron": 2})

    assert calls == 2


async def test_cron_tasks_continue_after_errors(docket: Docket, worker: Worker):
    """Cron tasks keep rescheduling even when they raise exceptions."""
    calls = 0

    async def flaky_cron_task(cron: Cron = Cron("0 * * * *", automatic=False)):
        nonlocal calls
        calls += 1
        raise ValueError("Task failed!")

    with patch.object(
        Cron,
        "get_next",
        return_value=datetime.now(timezone.utc) + timedelta(milliseconds=10),
    ):
        execution = await docket.add(flaky_cron_task)()
        await worker.run_at_most({execution.key: 3})

    assert calls == 3


async def test_cron_tasks_can_cancel_themselves(docket: Docket, worker: Worker):
    """A cron task can stop rescheduling by calling cron.cancel()."""
    calls = 0

    async def limited_cron_task(cron: Cron = Cron("0 * * * *", automatic=False)):
        nonlocal calls
        calls += 1
        if calls >= 3:
            cron.cancel()

    with patch.object(
        Cron,
        "get_next",
        return_value=datetime.now(timezone.utc) + timedelta(milliseconds=10),
    ):
        await docket.add(limited_cron_task)()
        await worker.run_until_finished()

    assert calls == 3


async def test_cron_supports_vixie_keywords(docket: Docket, worker: Worker):
    """Cron supports Vixie cron keywords like @daily, @weekly, @hourly."""
    runs = 0

    # @daily is equivalent to "0 0 * * *" (midnight every day)
    async def daily_task(cron: Cron = Cron("@daily", automatic=False)):
        nonlocal runs
        runs += 1

    with patch.object(
        Cron,
        "get_next",
        return_value=datetime.now(timezone.utc) + timedelta(milliseconds=10),
    ):
        execution = await docket.add(daily_task)()
        await worker.run_at_most({execution.key: 1})

    assert runs == 1


def test_cron_get_next_returns_future_time():
    """Cron.get_next() returns a datetime in the future via croniter."""
    cron = Cron("* * * * *", automatic=False)  # Every minute
    next_time = cron.get_next()

    assert isinstance(next_time, datetime)
    assert next_time > datetime.now(timezone.utc)
