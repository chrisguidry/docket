"""Tests for the generation that each state event carries.

A replace() or a Perpetual reschedule starts a new run under a key with a
higher generation.  A retry or a concurrency limit moves the same run to a
higher generation.  Each state event carries the generation of the run that
published it, so subscribe() and get_result() can ignore the finish of a run
that a newer one superseded.
"""

import asyncio
import contextlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Awaitable, Callable

import pytest

from docket import ConcurrencyLimit, Docket, Execution, ExecutionState, Retry, Worker
from docket._redis import PubSubClient, confirm_subscriptions
from tests.conftest import wait_for_event

KEY = "tracked"


@pytest.fixture
async def published(docket: Docket) -> AsyncGenerator[list[dict[str, Any]], None]:
    """Every state event published for ``KEY``, parsed from its JSON payload."""
    events: list[dict[str, Any]] = []
    ready = asyncio.Event()

    async def collect() -> None:
        async with docket._pubsub() as pubsub:  # pyright: ignore[reportPrivateUsage]
            await pubsub.subscribe(docket.key(f"state:{KEY}"))
            await confirm_subscriptions(pubsub, 1)
            ready.set()
            async for message in pubsub.listen():  # pragma: no branch
                if message["type"] != "message":
                    continue  # pragma: no cover
                events.append(json.loads(message["data"]))

    collector = asyncio.create_task(collect())
    await asyncio.wait_for(ready.wait(), timeout=5)
    yield events
    collector.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await collector


def states_and_generations(events: list[dict[str, Any]]) -> list[tuple[str, int]]:
    return [(event["state"], event["generation"]) for event in events]


async def succeeds_on_retry(retry: Retry = Retry(attempts=2)) -> str:
    if retry.attempt == 1:
        raise ValueError("the first attempt fails")
    return "retried"


async def test_each_state_event_carries_the_generation_of_its_attempt(
    docket: Docket,
    worker: Worker,
    published: list[dict[str, Any]],
    now: Callable[[], datetime],
):
    """A retry moves a run to a new generation, and every state event carries
    the generation of the attempt that published it."""
    await docket.add(
        succeeds_on_retry, when=now() + timedelta(milliseconds=50), key=KEY
    )()

    await worker.run_until_finished()
    await wait_for_event(
        published, lambda event: event["state"] == "completed", description="finish"
    )

    assert states_and_generations(published) == [
        ("scheduled", 1),
        ("queued", 1),
        ("running", 1),
        ("queued", 2),
        ("running", 2),
        ("completed", 2),
    ]


async def unstarted() -> None: ...


async def test_a_cancel_event_carries_the_generation_it_cancelled(
    docket: Docket, published: list[dict[str, Any]], now: Callable[[], datetime]
):
    """A cancel publishes the generation of the run it cancelled."""
    later = now() + timedelta(hours=1)
    await docket.add(unstarted, when=later, key=KEY)()
    await docket.replace(unstarted, later, KEY)()

    await docket.cancel(KEY)
    await wait_for_event(
        published, lambda event: event["state"] == "cancelled", description="cancel"
    )

    assert states_and_generations(published) == [
        ("scheduled", 1),
        ("scheduled", 2),
        ("cancelled", 2),
    ]


async def test_parking_and_waking_move_a_run_to_new_generations(
    docket: Docket, worker: Worker, published: list[dict[str, Any]]
):
    """A concurrency limit moves a run to a new generation when it parks the
    run, and again when it wakes it.  The run's events carry each one."""
    holder_started = asyncio.Event()
    release_holder = asyncio.Event()

    async def limited(
        role: str, limit: ConcurrencyLimit = ConcurrencyLimit(max_concurrent=1)
    ) -> None:
        if role == "holder":
            holder_started.set()
            await release_holder.wait()

    await docket.add(limited, key="holder")("holder")
    run = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(holder_started.wait(), timeout=5)

    await docket.add(limited, key=KEY)("contender")
    await wait_for_event(
        published, lambda event: event["state"] == "scheduled", description="park"
    )
    release_holder.set()
    await asyncio.wait_for(run, timeout=10)
    await wait_for_event(
        published, lambda event: event["state"] == "completed", description="finish"
    )

    assert states_and_generations(published) == [
        ("queued", 1),
        ("running", 1),
        ("scheduled", 2),
        ("queued", 3),
        ("running", 3),
        ("completed", 3),
    ]


async def replaceable() -> str:
    return "replacement"


@pytest.fixture
async def running_predecessor(docket: Docket) -> Execution:
    """A run of ``replaceable`` under ``KEY`` that a worker has claimed, so a
    replace() leaves it running while its successor waits."""
    await docket.add(replaceable, key=KEY)()
    async with docket.redis() as redis:
        messages = await redis.xrange(docket.stream_key)
    message_id, message = messages[0]
    predecessor = await Execution.from_message(docket, message, message_id=message_id)
    await predecessor.claim("worker-1")
    return predecessor


async def returned_by_replace(docket: Docket) -> Execution:
    return await docket.replace(replaceable, datetime.now(timezone.utc), KEY)()


async def looked_up_after_replace(docket: Docket) -> Execution:
    await docket.replace(replaceable, datetime.now(timezone.utc), KEY)()
    execution = await docket.get_execution(KEY)
    assert execution is not None
    return execution


@pytest.fixture(params=[returned_by_replace, looked_up_after_replace])
async def replacement(
    request: pytest.FixtureRequest, docket: Docket, running_predecessor: Execution
) -> Execution:
    """The run that replaced ``running_predecessor``, held the way replace()
    returns it, or the way get_execution() finds a Perpetual task's next run."""
    hold: Callable[[Docket], Awaitable[Execution]] = request.param
    return await hold(docket)


@pytest.fixture
def predecessor_finishes_during_setup(
    running_predecessor: Execution, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Finish ``running_predecessor`` once a subscribe() call is listening, but
    before it reads the task's current state."""

    async def confirm_then_finish(pubsub: PubSubClient, count: int) -> None:
        await confirm_subscriptions(pubsub, count)
        await running_predecessor.mark_as_completed()

    monkeypatch.setattr("docket.execution.confirm_subscriptions", confirm_then_finish)


@pytest.mark.usefixtures("predecessor_finishes_during_setup")
async def test_subscribe_leaves_out_a_superseded_finish_during_setup(
    worker: Worker, replacement: Execution
):
    """A replacement's subscribe() does not report the finish of the run it
    replaced, even when that finish lands while the subscription sets up."""
    ready = asyncio.Event()
    states: list[ExecutionState] = []

    async def collect_states() -> None:
        async for event in replacement.subscribe(ready=ready):  # pragma: no branch
            if event["type"] == "state":
                states.append(ExecutionState(event["state"]))
                if event["state"] == ExecutionState.COMPLETED:
                    break

    collector = asyncio.create_task(collect_states())
    await asyncio.wait_for(ready.wait(), timeout=5)
    await worker.run_until_finished()
    await asyncio.wait_for(collector, timeout=5)

    assert states == [
        ExecutionState.QUEUED,
        ExecutionState.RUNNING,
        ExecutionState.COMPLETED,
    ]


@pytest.mark.usefixtures("predecessor_finishes_during_setup")
async def test_get_result_on_a_replacement_ignores_its_predecessor_finishing_during_setup(
    worker: Worker, replacement: Execution, subscribed: asyncio.Event
):
    """get_result() on a replacement waits for its own run when the run it
    replaced finishes while the waiter subscribes."""
    waiter = asyncio.create_task(replacement.get_result(timeout=timedelta(seconds=5)))

    await asyncio.wait_for(subscribed.wait(), timeout=5)
    await worker.run_until_finished()

    assert await waiter == "replacement"


async def test_get_result_on_a_replacement_ignores_its_predecessor_finishing_later(
    worker: Worker,
    running_predecessor: Execution,
    replacement: Execution,
    subscribed: asyncio.Event,
):
    """get_result() on a replacement waits for its own run when the run it
    replaced finishes after the waiter subscribes."""
    waiter = asyncio.create_task(replacement.get_result(timeout=timedelta(seconds=5)))

    await asyncio.wait_for(subscribed.wait(), timeout=5)
    await running_predecessor.mark_as_completed()
    await worker.run_until_finished()

    assert await waiter == "replacement"


async def test_get_result_waits_through_a_retry(
    docket: Docket, worker: Worker, subscribed: asyncio.Event
):
    """get_result() returns the result of the attempt that succeeded, after the
    retry moved the run to a new generation."""
    execution = await docket.add(succeeds_on_retry)()
    waiter = asyncio.create_task(execution.get_result(timeout=timedelta(seconds=5)))

    await asyncio.wait_for(subscribed.wait(), timeout=5)
    await worker.run_until_finished()

    assert await waiter == "retried"
