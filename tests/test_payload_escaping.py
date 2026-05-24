"""Regression test: state-channel payloads must remain valid JSON when
task keys contain characters that break naive string concatenation.

The Lua scripts that build their state payload via ``..`` concatenation
will emit broken JSON when a task key contains ``"``, ``\\``, ``\n``,
``\r``, or ``\t``.  Subscribers' ``json.loads`` then raises and they
miss the state transition entirely.

Coverage:

- ``_stream_due_tasks`` (worker scheduler): future-scheduled task with a
  weird key, scheduler moves it to the stream, subscriber receives a
  ``queued`` event.
- ``_acquire_or_park`` / ``_release_and_wake`` / ``_scavenge_and_wake``
  (concurrency limiter): touched by the concurrency tests already; we
  add a direct test that uses a weird key.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import datetime, timedelta, timezone

import pytest
from docket import ConcurrencyLimit, Docket, Worker

# Weird key that exercises the five escapes the Lua side handles:
# - `"` would close the JSON string mid-value.
# - `\` plus the following char would form an invalid JSON escape.
# - the named whitespace controls `\n` / `\r` / `\t`.
# Exotic control chars (NUL, BEL, VT, FF, ESC, ...) are not supported
# in task keys; callers giving us those will see broken JSON payloads.
WEIRD_KEY = 'weird"key\nwith\\backslash\tand\r\nctrl'


@pytest.fixture
async def state_messages(docket: Docket):
    """Collect state events for WEIRD_KEY in the background.

    Payloads are run through ``json.loads`` directly -- if escape ever
    regresses and emits invalid JSON the collector raises here and the
    awaiting test sees the failure (rather than us silently dropping
    bad payloads on the floor).
    """
    messages: list[dict[str, object]] = []
    ready = asyncio.Event()

    async def collector() -> None:
        async with docket._pubsub() as pubsub:  # pyright: ignore[reportPrivateUsage]
            await pubsub.subscribe(docket.key(f"state:{WEIRD_KEY}"))
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
        with contextlib.suppress(asyncio.CancelledError, json.JSONDecodeError):
            await task


async def test_stream_due_tasks_payload_is_parseable_with_weird_key(
    docket: Docket,
    worker: Worker,
    state_messages: list[dict[str, object]],
) -> None:
    """``_stream_due_tasks`` Lua must emit JSON-parseable state events even
    when the task key contains ``"`` / ``\\n`` / control chars."""
    messages = state_messages

    async def the_task() -> None: ...

    docket.register(the_task)

    # Schedule for the very near future so the task parks in the queue
    # (not the stream) and the worker's scheduler invokes
    # ``_stream_due_tasks`` to move it.  ``add`` with ``when<=now`` would
    # short-circuit through the immediate path in ``_schedule`` and we
    # would never exercise the Lua we care about here.
    when = datetime.now(timezone.utc) + timedelta(milliseconds=50)
    await docket.add(the_task, when=when, key=WEIRD_KEY)()

    await worker.run_until_finished()

    # Drain a beat to let subscriber events flush.
    await asyncio.sleep(0.05)

    assert any(msg.get("state") == "queued" for msg in messages), (
        f"subscriber should have seen a queued state event from "
        f"_stream_due_tasks; got: {messages!r}"
    )
    # The key field of every event we received must round-trip exactly.
    for msg in messages:
        assert msg.get("key") == WEIRD_KEY, (
            f"key round-trip failed in state event: {msg!r}"
        )


async def test_concurrency_park_payload_is_parseable_with_weird_key(
    docket: Docket,
    worker: Worker,
    state_messages: list[dict[str, object]],
) -> None:
    """``_acquire_or_park`` Lua publishes a ``scheduled`` state event when
    parking a task on the waiter stream.  The payload must remain parseable
    when the task key contains awkward characters."""
    messages = state_messages

    started_first = asyncio.Event()
    let_first_finish = asyncio.Event()

    async def the_task(
        limit: ConcurrencyLimit = ConcurrencyLimit(max_concurrent=1),
    ) -> None:
        if not started_first.is_set():
            started_first.set()
            await let_first_finish.wait()

    docket.register(the_task)

    # Schedule the holder and start the worker so it grabs the slot
    # before the weird-key task is even added.  Without this ordering,
    # both tasks land in the stream and the worker may claim the
    # weird-key one first, in which case it never parks (it just gets
    # the slot) and the ``scheduled`` state event we are testing for
    # is never emitted.
    await docket.add(the_task, key="holder")()
    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(started_first.wait(), timeout=5)

    # Holder is running and holding the only slot.  Now add the
    # weird-key task -- it must park on the waiter stream.
    await docket.add(the_task, key=WEIRD_KEY)()
    # Give the parking script a moment to publish.
    await asyncio.sleep(0.1)
    let_first_finish.set()
    await asyncio.wait_for(worker_task, timeout=10)
    await asyncio.sleep(0.05)

    assert any(msg.get("state") == "scheduled" for msg in messages), (
        f"subscriber should have seen a scheduled (parked) state event "
        f"from _acquire_or_park; got: {messages!r}"
    )
    for msg in messages:
        assert msg.get("key") == WEIRD_KEY, (
            f"key round-trip failed in state event: {msg!r}"
        )
