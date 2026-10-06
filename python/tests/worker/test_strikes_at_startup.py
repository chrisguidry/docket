"""A worker reads the strikes that exist when it starts before it runs
anything, so a strike written earlier is never missed."""

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from docket import Docket, Worker
from docket.execution import ExecutionState
from docket.strikelist import StrikeList


async def test_a_worker_waits_for_the_strikes_before_it_runs_anything(
    docket: Docket, monkeypatch: pytest.MonkeyPatch
):
    runs: list[str] = []

    async def struck() -> None:
        runs.append("ran")

    docket.register(struck)
    execution = await docket.add(struck)()
    await docket.strike(struck)

    # A second connection to the docket, whose strike monitor cannot read
    # the stream until the test lets it, the way it can be slow after a
    # restart.
    loading = asyncio.Event()
    read_strikes = StrikeList._read_strikes  # pyright: ignore[reportPrivateUsage]

    async def slow_read_strikes(self: StrikeList, *args: Any) -> Any:
        await loading.wait()
        return await read_strikes(self, *args)

    monkeypatch.setattr(StrikeList, "_read_strikes", slow_read_strikes)

    async with Docket(name=docket.name, url=docket.url) as second:
        second.register(struck)
        async with Worker(
            second,
            schedule_automatic_tasks=False,
            minimum_check_interval=timedelta(milliseconds=5),
        ) as worker:
            running = asyncio.create_task(worker.run_until_finished())
            await asyncio.sleep(0.5)
            loading.set()
            await running

    assert runs == []
    await execution.sync()
    assert execution.state == ExecutionState.CANCELLED
