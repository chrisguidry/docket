"""Tests for Cron dependency (cron-style scheduled tasks)."""

import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from docket import Docket, Worker
from docket.dependencies import Cron
from docket.dependencies._cron import VIXIE_KEYWORDS


def _soon() -> datetime:
    return datetime.now(timezone.utc) + timedelta(milliseconds=10)


async def test_cron_task_reschedules_itself(docket: Docket, worker: Worker):
    """Cron tasks automatically reschedule after each execution."""
    runs = 0

    async def my_cron_task(cron: Cron = Cron("0 9 * * *", automatic=False)):
        nonlocal runs
        runs += 1

    with patch.object(Cron, "next_time", side_effect=_soon):
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

    with patch.object(Cron, "next_time", side_effect=_soon):
        await worker.run_at_most({"my_automatic_cron": 2})

    assert calls == 2


async def test_cron_tasks_continue_after_errors(docket: Docket, worker: Worker):
    """Cron tasks keep rescheduling even when they raise exceptions."""
    calls = 0

    async def flaky_cron_task(cron: Cron = Cron("0 * * * *", automatic=False)):
        nonlocal calls
        calls += 1
        raise ValueError("Task failed!")

    with patch.object(Cron, "next_time", side_effect=_soon):
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

    with patch.object(Cron, "next_time", side_effect=_soon):
        await docket.add(limited_cron_task)()
        await worker.run_until_finished()

    assert calls == 3


@pytest.mark.parametrize("keyword,expected", list(VIXIE_KEYWORDS.items()))
def test_vixie_keywords_expand_to_valid_expressions(keyword: str, expected: str):
    """Vixie cron keywords are expanded and produce valid next times via cronsim."""
    now = datetime.now(timezone.utc)
    cron = Cron(keyword, automatic=False)
    assert cron.expression == expected
    assert cron.next_time() > now


async def test_cron_supports_vixie_keywords(docket: Docket, worker: Worker):
    """Cron supports Vixie cron keywords like @daily, @weekly, @hourly."""
    runs = 0

    async def daily_task(cron: Cron = Cron("@daily", automatic=False)):
        nonlocal runs
        runs += 1

    with patch.object(Cron, "next_time", side_effect=_soon):
        execution = await docket.add(daily_task)()
        await worker.run_at_most({execution.key: 1})

    assert runs == 1


async def test_automatic_cron_waits_for_scheduled_time(docket: Docket, worker: Worker):
    """Automatic cron tasks wait for their next scheduled time instead of running immediately.

    Unlike Perpetual tasks which run immediately at worker startup, Cron tasks
    schedule themselves for the next matching cron time. This ensures a Monday 9 AM
    cron doesn't accidentally run on a Wednesday startup.
    """
    calls: list[datetime] = []

    async def scheduled_task(cron: Cron = Cron("0 9 * * 1")):  # Mondays at 9 AM
        calls.append(datetime.now(timezone.utc))

    docket.register(scheduled_task)

    future_time = datetime.now(timezone.utc) + timedelta(milliseconds=100)
    with patch.object(Cron, "next_time", return_value=future_time):
        await worker.run_at_most({"scheduled_task": 1})

    assert len(calls) == 1
    assert calls[0] >= future_time - timedelta(milliseconds=50)


@pytest.mark.skipif(
    sys.platform == "win32", reason="Timing-sensitive: unreliable on Windows"
)
async def test_cron_with_timezone(docket: Docket, worker: Worker):
    """Cron tasks can be scheduled in a specific timezone."""
    runs = 0

    pacific = ZoneInfo("America/Los_Angeles")

    async def pacific_task(cron: Cron = Cron("0 9 * * *", tz=pacific, automatic=False)):
        nonlocal runs
        runs += 1

    with patch.object(Cron, "next_time", side_effect=_soon):
        execution = await docket.add(pacific_task)()
        await worker.run_at_most({execution.key: 2})

    assert runs == 2


def test_cron_next_time_returns_sequential_future_datetimes():
    """next_time produces ascending aware datetimes via the real cronsim iterator."""
    now = datetime.now(timezone.utc)
    cron = Cron("0 9 * * *", automatic=False)
    first = cron.next_time()
    second = cron.next_time()
    assert first > now
    assert second > first
    assert first.tzinfo is not None


def test_cron_next_time_respects_timezone():
    """next_time produces times matching the configured timezone."""
    tokyo = ZoneInfo("Asia/Tokyo")
    cron = Cron("0 9 * * *", automatic=False, tz=tokyo)
    next_time = cron.next_time()
    assert next_time.astimezone(tokyo).hour == 9


def test_standard_expression_not_modified():
    """Standard 5-field expressions pass through without modification."""
    cron = Cron("30 2 15 * *", automatic=False)
    assert cron.expression == "30 2 15 * *"
