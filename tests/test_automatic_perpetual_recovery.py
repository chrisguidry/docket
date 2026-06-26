"""Recovery of automatic perpetuals whose self-rescheduling chain is severed.

A rolling deploy that introduces a new ``Perpetual(automatic=True)`` task can
have its chain silently and permanently broken: an old worker that doesn't yet
have the task registered consumes one of its executions through the unknown-task
fallback, which carries no ``Perpetual`` completion handler and so never
reschedules.  The streaming Lua has already removed the schedule entry, so the
chain is gone until the whole fleet restarts (see issue #437).

A still-running worker re-sows automatic perpetuals on an interval, which heals
a severed chain without a restart.  ``docket.add(task, key=name)`` dedups against
a live perpetual (scheduled, queued, or running), so re-seeding is a no-op while
the chain is healthy and only takes effect once it has actually been lost.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from docket import Cron, Docket, Perpetual, Worker
from tests.conftest import wait_until


async def test_running_worker_resows_severed_automatic_perpetual(
    docket: Docket,
    worker: Worker,
    monkeypatch: pytest.MonkeyPatch,
):
    """A long-running worker re-sows an automatic perpetual after its chain is
    severed, without needing a restart."""
    # Re-seed often so the test doesn't wait the production minute.
    monkeypatch.setattr(
        "docket.worker.AUTOMATIC_PERPETUAL_RESEED_INTERVAL_SECONDS", 0.05
    )

    calls = 0

    async def perpetual_task(
        # A long interval so the chain won't re-run on its own within the test
        # window -- only a re-seed produces the second execution.
        perpetual: Perpetual = Perpetual(every=timedelta(seconds=30), automatic=True),
    ):
        nonlocal calls
        calls += 1

    docket.register(perpetual_task)
    runs_key = docket.runs_key("perpetual_task")

    async def next_run_is_parked() -> bool:
        async with docket.redis() as redis:
            return await redis.hget(runs_key, "state") == b"scheduled"

    run = asyncio.create_task(worker.run_forever())
    await wait_until(lambda: calls == 1, description="startup seeding ran the task")
    # Let the perpetual finish parking its own next run before severing, so the
    # cancel can't race the self-reschedule and leave the chain intact.
    await wait_until(next_run_is_parked, description="next run parked")

    # Sever the chain the same way the fallback drop does: no scheduled entry,
    # nothing in flight.
    await docket.cancel("perpetual_task")

    await wait_until(lambda: calls == 2, description="re-seeding re-ran the perpetual")

    run.cancel()
    await asyncio.gather(run, return_exceptions=True)

    # Leave nothing parked 30s out for the leak checker.
    await docket.cancel("perpetual_task")


async def test_fallback_drop_then_reseed_recovers_chain(
    docket: Docket,
    caplog: pytest.LogCaptureFixture,
):
    """An old worker dropping a new automatic perpetual via the unknown-task
    fallback severs its chain, and re-seeding restores it."""
    calls = 0

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(seconds=30), automatic=True),
    ):
        nonlocal calls
        calls += 1

    docket.register(perpetual_task)

    # The new worker has seeded the perpetual; it becomes due and is streamed.
    await docket.add(perpetual_task, key="perpetual_task")()

    # An old worker, still draining, doesn't have the task registered and drops
    # it through the fallback.
    docket.tasks.pop("perpetual_task")
    async with Worker(
        docket,
        schedule_automatic_tasks=False,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as old_worker:
        with caplog.at_level(logging.WARNING):
            await old_worker.run_until_finished()

    assert "Unknown task 'perpetual_task' received - dropping" in caplog.text
    assert calls == 0

    # The chain is severed: no live schedule entry remains for the perpetual.
    runs_key = docket.runs_key("perpetual_task")
    async with docket.redis() as redis:
        assert await redis.hget(runs_key, "known") is None

    # The surviving new worker (which has the task) re-sows it on its next tick.
    docket.register(perpetual_task)
    new_worker = Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    )
    await new_worker._schedule_all_automatic_perpetual_tasks()  # type: ignore[protected-access]

    async with docket.redis() as redis:
        assert await redis.hget(runs_key, "known") is not None

    async with new_worker:
        await new_worker.run_at_most({"perpetual_task": 1})
    assert calls == 1

    await docket.cancel("perpetual_task")


async def test_reseed_is_idempotent_for_live_perpetual(
    docket: Docket,
    worker: Worker,
):
    """Re-seeding a healthy automatic perpetual dedups against the live entry
    and never injects a duplicate execution."""

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(seconds=30), automatic=True),
    ):
        pass  # pragma: no cover

    docket.register(perpetual_task)

    # First seed establishes a live (queued) entry for the perpetual.
    await worker._schedule_all_automatic_perpetual_tasks()  # type: ignore[protected-access]

    runs_key = docket.runs_key("perpetual_task")
    async with docket.redis() as redis:
        assert await redis.hget(runs_key, "known") is not None
        generation_before = await redis.hget(runs_key, "generation")

    # A second seed of the same live perpetual must dedup, not re-schedule -- the
    # generation counter only advances when a task is actually (re)scheduled.
    await worker._schedule_all_automatic_perpetual_tasks()  # type: ignore[protected-access]

    async with docket.redis() as redis:
        generation_after = await redis.hget(runs_key, "generation")

    assert generation_before == generation_after

    await docket.cancel("perpetual_task")


async def test_reseed_does_not_advance_cron_iterator_while_live(
    docket: Docket,
    worker: Worker,
):
    """Reseeding a healthy automatic cron must not consume its schedule iterator.

    ``Cron.initial_when`` advances the iterator each time it's read, so reseeding
    a cron that's already scheduled would march its next run further into the
    future on every tick. Live tasks should be left untouched.
    """
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    next_time_calls = 0

    def advancing() -> datetime:
        nonlocal next_time_calls
        next_time_calls += 1
        return base + timedelta(hours=next_time_calls)

    async def cron_task(cron: Cron = Cron("0 * * * *", automatic=True)):
        pass  # pragma: no cover

    docket.register(cron_task)

    with patch.object(Cron, "next_time", side_effect=advancing):
        # The first seed schedules the cron, consuming one occurrence.
        await worker._schedule_all_automatic_perpetual_tasks()  # type: ignore[protected-access]
        assert next_time_calls == 1

        # The cron is now live; further reseeds must leave the iterator alone.
        await worker._schedule_all_automatic_perpetual_tasks()  # type: ignore[protected-access]
        await worker._schedule_all_automatic_perpetual_tasks()  # type: ignore[protected-access]
        assert next_time_calls == 1

    await docket.cancel("cron_task")


async def test_restoring_struck_automatic_perpetual_resumes_without_restart(
    docket: Docket,
    worker: Worker,
    monkeypatch: pytest.MonkeyPatch,
):
    """A struck automatic perpetual resumes scheduling after it is restored,
    without needing a worker restart (issue #377).

    Striking blocks rescheduling and cancels the due execution, which severs a
    perpetual's self-rescheduling chain.  Before re-seeding existed, restore left
    the task inert until the worker was restarted; now a running worker re-sows
    it on its next tick.
    """
    monkeypatch.setattr(
        "docket.worker.AUTOMATIC_PERPETUAL_RESEED_INTERVAL_SECONDS", 0.05
    )

    calls = 0

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(
            every=timedelta(milliseconds=50), automatic=True
        ),
    ):
        nonlocal calls
        calls += 1

    docket.register(perpetual_task)
    runs_key = docket.runs_key("perpetual_task")

    async def chain_is_dead() -> bool:
        async with docket.redis() as redis:
            known = await redis.hget(runs_key, "known")
            state = await redis.hget(runs_key, "state")
            return known is None and state != b"running"

    run = asyncio.create_task(worker.run_forever())
    await wait_until(lambda: calls >= 1, description="perpetual running normally")

    # Suspend it: striking cancels the due execution and blocks rescheduling, so
    # the chain dies and re-seeding stays a no-op while struck.
    await docket.strike("perpetual_task")
    await wait_until(chain_is_dead, description="struck chain severed")
    calls_while_struck = calls

    # Restoring must resume scheduling on the still-running worker.
    await docket.restore("perpetual_task")
    await wait_until(
        lambda: calls > calls_while_struck,
        description="restored perpetual resumed without restart",
    )

    run.cancel()
    await asyncio.gather(run, return_exceptions=True)
    await docket.cancel("perpetual_task")

    await docket.cancel("perpetual_task")
