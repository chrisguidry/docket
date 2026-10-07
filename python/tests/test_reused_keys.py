"""A key used again, or replaced while it runs, shows only its current run:
its result, its error, and its ending."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Callable

from docket import Docket, Worker
from docket.execution import ExecutionState


async def test_a_replaced_run_that_finishes_last_keeps_its_successors_result(
    docket: Docket, worker: Worker
):
    started = asyncio.Event()
    release = asyncio.Event()

    async def echo(text: str) -> str:
        if text == "old":
            started.set()
            await release.wait()
        return text

    docket.register(echo)
    await docket.add(echo, key="swapped")("old")
    running = asyncio.create_task(worker.run_until_finished())
    await started.wait()

    replacement = await docket.replace(
        echo, when=datetime.now(timezone.utc), key="swapped"
    )("new")
    while replacement.state != ExecutionState.COMPLETED:
        await asyncio.sleep(0.02)
        await replacement.sync()
    release.set()
    await running

    assert await replacement.get_result() == "new"


async def test_a_reused_key_does_not_return_the_previous_runs_result(
    docket: Docket, worker: Worker
):
    async def maybe(text: str | None) -> str | None:
        return text

    docket.register(maybe)
    first = await docket.add(maybe, key="reused")("first")
    await worker.run_until_finished()
    assert await first.get_result() == "first"

    second = await docket.add(maybe, key="reused")(None)
    await worker.run_until_finished()

    assert await second.get_result() is None


async def test_a_reused_key_does_not_show_the_previous_runs_ending(
    docket: Docket, worker: Worker
):
    async def echo(text: str) -> str:
        if text == "fail":
            raise ValueError("the first run failed")
        return text

    docket.register(echo)
    await docket.add(echo, key="again")("fail")
    await worker.run_until_finished()

    later = datetime.now(timezone.utc) + timedelta(minutes=1)
    second = await docket.add(echo, when=later, key="again")("fine")
    await second.sync()
    assert (second.state, second.error, second.completed_at, second.worker) == (
        ExecutionState.SCHEDULED,
        None,
        None,
        None,
    )

    await docket.replace(echo, when=datetime.now(timezone.utc), key="again")("fine")
    await worker.run_until_finished()
    await second.sync()
    assert (second.state, second.error) == (ExecutionState.COMPLETED, None)


async def test_a_key_used_again_soon_after_its_run_ended_keeps_its_record(
    redis_url: str, make_docket_name: Callable[[], str]
):
    """An ended run's record expires execution_ttl after the ending.  A run added
    under the same key before then has a record of its own: it waits to be claimed
    and runs, however long it waits."""
    ran: list[int] = []

    async def count(n: int) -> None:
        ran.append(n)

    async with Docket(
        name=make_docket_name(), url=redis_url, execution_ttl=timedelta(seconds=1)
    ) as docket:
        docket.register(count)
        await docket.add(count, key="again")(1)
        async with Worker(docket) as worker:
            await worker.run_until_finished()

        second = await docket.add(count, key="again")(2)
        await asyncio.sleep(1.5)  # the ended run's record would have expired by now
        await second.sync()
        assert second.state == ExecutionState.QUEUED

        async with Worker(docket) as worker:
            await worker.run_until_finished()

    assert ran == [1, 2]
