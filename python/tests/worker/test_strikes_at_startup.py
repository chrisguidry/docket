"""A worker reads the strikes that exist when it starts before it runs
anything, so a strike written earlier is never missed."""

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest

from docket import Docket, Worker
from docket.execution import ExecutionState
from docket.strikelist import StrikeList


def block_strike_reads(monkeypatch: pytest.MonkeyPatch) -> asyncio.Event:
    """Holds every strike monitor's reads until the returned event is set."""
    loading = asyncio.Event()
    read_strikes = StrikeList._read_strikes  # pyright: ignore[reportPrivateUsage]

    async def slow_read_strikes(self: StrikeList, *args: Any) -> Any:
        await loading.wait()
        return await read_strikes(self, *args)

    monkeypatch.setattr(StrikeList, "_read_strikes", slow_read_strikes)
    return loading


async def test_a_worker_waits_for_the_strikes_before_it_runs_anything(
    docket: Docket, the_task: AsyncMock, monkeypatch: pytest.MonkeyPatch
):
    docket.register(the_task)
    execution = await docket.add(the_task)()
    await docket.strike(the_task)

    # A second connection to the docket, whose strike monitor cannot read
    # the stream until the test lets it, the way it can be slow after a
    # restart.
    loading = block_strike_reads(monkeypatch)

    async with Docket(name=docket.name, url=docket.url) as second:
        second.register(the_task)
        async with Worker(
            second,
            schedule_automatic_tasks=False,
            minimum_check_interval=timedelta(milliseconds=5),
        ) as worker:
            running = asyncio.create_task(worker.run_until_finished())
            await asyncio.sleep(0.5)
            loading.set()
            await running

    the_task.assert_not_awaited()
    await execution.sync()
    assert execution.state == ExecutionState.CANCELLED


async def test_a_worker_stops_while_it_waits_for_the_strikes(
    docket: Docket, monkeypatch: pytest.MonkeyPatch
):
    block_strike_reads(monkeypatch)

    async with Docket(name=docket.name, url=docket.url) as second:
        async with Worker(second, schedule_automatic_tasks=False) as worker:
            running = asyncio.create_task(worker.run_forever())
            await asyncio.sleep(0.2)
        # Leaving the worker stops it, though the strikes never loaded.
        await asyncio.wait_for(running, timeout=5)
