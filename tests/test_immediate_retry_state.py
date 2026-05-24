"""Regression test: a ``Retry(delay=timedelta(0))`` reschedule lands directly
in the stream and the state event published from inside ``_schedule`` Lua's
reschedule branch reports ``state=queued`` -- not ``state=scheduled``.

Pre-fix, the reschedule branch always parked the retry into the future queue
regardless of delay, and the Python side always published a ``scheduled``
state event for any reschedule.  The new behaviour respects ``is_immediate``:
zero-delay retries land directly on the stream and publish ``queued`` instead.

Subscribers (dashboards, "wait for next attempt" logic, etc.) that watched
for ``scheduled`` on every retry will no longer see it for the zero-delay
case.  This test locks that semantics in so a regression to the old
"always park, always publish scheduled" behaviour is caught at the contract
boundary instead of at runtime in someone's pipeline.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import timedelta

import pytest
from docket import Docket, Worker
from docket.dependencies import Retry

TASK_KEY = "immediate-retry"


@pytest.fixture
async def state_messages(docket: Docket):
    """Collect every state event published on the test task's channel."""
    messages: list[dict[str, object]] = []
    ready = asyncio.Event()

    async def collector() -> None:
        async with docket._pubsub() as pubsub:  # pyright: ignore[reportPrivateUsage]
            await pubsub.subscribe(docket.key(f"state:{TASK_KEY}"))
            ready.set()
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue  # pragma: no cover
                data = message["data"]
                payload = data.decode() if isinstance(data, bytes) else data
                messages.append(json.loads(payload))

    task = asyncio.create_task(collector())
    await ready.wait()
    try:
        yield messages
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_immediate_retry_publishes_queued_state_event(
    docket: Docket,
    worker: Worker,
    state_messages: list[dict[str, object]],
) -> None:
    """A ``Retry(delay=timedelta(0))`` reschedule publishes ``state=queued``.

    The first attempt raises, ``Retry.handle_failure`` calls
    ``execution.schedule(replace=True, reschedule_message=...)`` with
    ``delay=0``, so ``_schedule``'s reschedule branch takes the
    ``is_immediate`` path (XADD straight to the stream), and the Lua-side
    PUBLISH carries the Python-built ``state=queued`` payload.
    """
    attempts: list[int] = []

    async def the_task(
        retry: Retry = Retry(attempts=2, delay=timedelta(0)),
    ) -> None:
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("first attempt fails to trigger immediate retry")

    docket.register(the_task)
    await docket.add(the_task, key=TASK_KEY)()

    await worker.run_until_finished()
    # Let the subscriber drain.
    await asyncio.sleep(0.05)

    # Sanity: the task ran twice (original + immediate retry).
    assert len(attempts) == 2

    # The reschedule emitted exactly one state event from the _schedule
    # Lua reschedule branch.  With is_immediate=True, that event's state
    # MUST be 'queued' (the new behaviour), not 'scheduled' (the old).
    reschedule_events = [
        msg
        for msg in state_messages
        if msg.get("state") in {"queued", "scheduled"} and msg.get("when") is not None
    ]
    # We expect at least the initial QUEUED (from the first add) and the
    # retry's QUEUED.  No 'scheduled' events should appear because the
    # immediate path was used both times.
    assert reschedule_events, (
        f"expected to see at least one queued/scheduled state event; "
        f"got: {state_messages!r}"
    )
    assert not any(msg.get("state") == "scheduled" for msg in state_messages), (
        f"immediate retry must not publish state=scheduled (that was the "
        f"pre-fix behaviour where the reschedule branch always parked); "
        f"got: {state_messages!r}"
    )
    assert sum(1 for msg in state_messages if msg.get("state") == "queued") >= 2, (
        f"expected at least two queued events (initial add + retry); "
        f"got: {state_messages!r}"
    )


async def test_delayed_retry_still_publishes_scheduled_state_event(
    docket: Docket,
    worker: Worker,
    state_messages: list[dict[str, object]],
) -> None:
    """A retry with a non-zero delay parks into the queue and still publishes
    ``state=scheduled``.  Counterpart to the immediate-retry test: confirms
    the new branch hasn't accidentally collapsed both delays into one.
    """
    attempts: list[int] = []

    async def the_task(
        retry: Retry = Retry(attempts=2, delay=timedelta(milliseconds=50)),
    ) -> None:
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("first attempt fails to trigger delayed retry")

    docket.register(the_task)
    await docket.add(the_task, key=TASK_KEY)()

    await worker.run_until_finished()
    await asyncio.sleep(0.05)

    assert len(attempts) == 2

    # The retry parked into the queue with delay>0, so the Lua reschedule
    # branch took the non-immediate path and published 'scheduled'.
    assert any(msg.get("state") == "scheduled" for msg in state_messages), (
        f"a delayed retry must publish state=scheduled when it parks; "
        f"got: {state_messages!r}"
    )
