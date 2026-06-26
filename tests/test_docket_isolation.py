"""Locks in the per-docket isolation contract.

Two ``Docket`` instances with different names sharing one Redis are
expected to operate in complete isolation: tasks, strike state, and
pub/sub channels are namespace-scoped to each docket's prefix.  This
is the foundation that lets two unrelated services co-locate on a
shared Redis without colliding on identical task keys.

The contract is implicit in ``Docket.key()`` building every Redis key
off ``self.prefix``.  A regression that ever forgot the prefix (or
flattened two prefixes into one) would silently merge state across
unrelated services.  These tests would catch that.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import AsyncGenerator, Callable

import pytest
from docket import Docket

from tests.conftest import wait_for_event


@pytest.fixture
async def other_docket(
    redis_url: str, make_docket_name: Callable[[], str]
) -> AsyncGenerator[Docket, None]:
    """A second Docket pointing at the same Redis as the primary ``docket``
    fixture, but with a distinct name so its prefix is different."""
    async with Docket(name=make_docket_name(), url=redis_url) as second:
        yield second


async def test_add_in_one_docket_invisible_to_another(
    docket: Docket, other_docket: Docket
) -> None:
    """A task added in ``docket`` is not visible to ``other_docket``,
    even though both share Redis."""
    assert docket.name != other_docket.name

    async def the_task() -> None: ...

    docket.register(the_task)
    other_docket.register(the_task)

    await docket.add(the_task, key="shared-name")()

    # The other docket must not see this key under its own prefix.
    found = await other_docket.get_execution("shared-name")
    assert found is None, (
        f"other_docket should not see a task added under a different prefix; "
        f"got: {found!r}"
    )


async def test_cancel_in_one_docket_does_not_touch_another(
    docket: Docket, other_docket: Docket
) -> None:
    """Cancelling ``shared-name`` in ``docket`` must not flip the state of
    a same-keyed task in ``other_docket``."""

    async def the_task() -> None: ...

    docket.register(the_task)
    other_docket.register(the_task)

    await docket.add(the_task, key="shared-name")()
    await other_docket.add(the_task, key="shared-name")()

    await docket.cancel("shared-name")

    # The other docket's task must still exist and not be cancelled.
    other = await other_docket.get_execution("shared-name")
    assert other is not None, "other_docket's task must still exist after docket.cancel"
    # And the original docket's task should now be tombstoned/cancelled.
    primary = await docket.get_execution("shared-name")
    if primary is not None:  # pragma: no branch
        # With non-zero execution_ttl the tombstone hangs around with
        # state=cancelled; with execution_ttl=0 it would be gone.
        assert primary.state.value == "cancelled"


async def test_state_pubsub_does_not_cross_dockets(
    docket: Docket, other_docket: Docket
) -> None:
    """A state event published on ``docket``'s channel must not be
    delivered to a subscriber on ``other_docket``'s channel for the
    same task key.  Pub/sub channels are prefix-scoped just like
    Redis keys.

    Subscribes to BOTH channels.  When the event arrives on docket's
    own channel, we know Redis has already dispatched it -- so if a
    leak to other_docket's channel was going to happen, it would
    already be in ``other_events`` too.  That makes this negative
    assertion deterministic instead of a fixed-duration wait.
    """
    task_key = "shared-name"
    same_events: list[dict[str, object]] = []
    other_events: list[dict[str, object]] = []
    same_ready = asyncio.Event()
    other_ready = asyncio.Event()

    async def collect(
        target_docket: Docket,
        bucket: list[dict[str, object]],
        ready: asyncio.Event,
    ) -> None:
        async with target_docket._pubsub() as pubsub:  # pyright: ignore[reportPrivateUsage]
            await pubsub.subscribe(target_docket.key(f"state:{task_key}"))
            ready.set()
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue  # pragma: no cover
                data = message["data"]
                payload = data.decode() if isinstance(data, bytes) else data
                bucket.append(json.loads(payload))

    same_collector = asyncio.create_task(collect(docket, same_events, same_ready))
    other_collector = asyncio.create_task(
        collect(other_docket, other_events, other_ready)
    )
    await same_ready.wait()
    await other_ready.wait()

    async def the_task() -> None: ...

    docket.register(the_task)
    await docket.add(the_task, key=task_key)()

    # Handshake: wait for the event on docket's own channel.  Once
    # we've seen it, Redis has already done its dispatch and any
    # cross-docket leak would already be in other_events.
    await wait_for_event(
        same_events,
        lambda m: m.get("key") == task_key,
        description="same-docket state event",
    )

    same_collector.cancel()
    other_collector.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await same_collector
    with contextlib.suppress(asyncio.CancelledError):
        await other_collector

    assert other_events == [], (
        f"other_docket subscribers must not receive state events from "
        f"a different docket's channel; got: {other_events!r}"
    )
