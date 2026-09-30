"""Tests for the safeguard task that a parked concurrency-limit waiter schedules."""

import asyncio
from datetime import datetime
from typing import Annotated, Any, Awaitable, Callable

import pytest

from docket import ConcurrencyLimit, Docket, Execution, ExecutionState, Worker
from tests.conftest import wait_until

SAFEGUARD_PREFIX = "__safeguard__:"


async def test_waiter_woken_before_its_safeguard_exists_cancels_the_safeguard(
    docket: Docket, monkeypatch: pytest.MonkeyPatch
):
    """A waiter that is woken before it schedules its safeguard cancels it.

    Parking a task and scheduling its safeguard are two round trips.  When the
    slot holder releases between them, the wake finds no safeguard to remove,
    so the parked task cancels the safeguard itself.  Without that, the
    safeguard would wait in the queue for the whole redelivery timeout, and
    run_until_finished would not return until it ran.

    Either task can take the slot first, so the test holds open the gap for
    whichever one parks.
    """
    slot_holder_may_finish = asyncio.Event()
    parked: list[str] = []
    finished: list[str] = []

    async def the_task(
        name: str, customer_id: Annotated[int, ConcurrencyLimit(1)]
    ) -> None:
        await slot_holder_may_finish.wait()
        finished.append(name)

    async def is_woken(task_key: str) -> bool:
        async with docket.redis() as redis:
            state = await redis.hget(docket.runs_key(task_key), "state")
        return state == b"queued"

    add = docket.add

    def add_after_the_wake(
        function: Callable[..., Awaitable[Any]],
        when: datetime | None = None,
        key: str | None = None,
    ) -> Callable[..., Awaitable[Execution]]:
        schedule = add(function, when, key)

        async def scheduler(*args: Any, **kwargs: Any) -> Execution:
            if key is not None and key.startswith(SAFEGUARD_PREFIX):
                parked_key = key.removeprefix(SAFEGUARD_PREFIX)
                parked.append(parked_key)
                slot_holder_may_finish.set()
                await wait_until(
                    lambda: is_woken(parked_key), description="the waiter's wake"
                )
            return await schedule(*args, **kwargs)

        return scheduler

    monkeypatch.setattr(docket, "add", add_after_the_wake)

    await docket.add(the_task, key="first")("first", customer_id=1)
    await docket.add(the_task, key="second")("second", customer_id=1)

    async with Worker(docket, concurrency=2) as worker:
        await worker.run_until_finished()

    assert sorted(finished) == ["first", "second"]
    assert len(parked) == 1
    safeguard = await docket.get_execution(SAFEGUARD_PREFIX + parked[0])
    assert safeguard is not None
    assert safeguard.state == ExecutionState.CANCELLED
