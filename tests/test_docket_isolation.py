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
    if primary is not None:
        # With non-zero execution_ttl the tombstone hangs around with
        # state=cancelled; with execution_ttl=0 it would be gone.
        assert primary.state.value == "cancelled"


async def test_state_pubsub_does_not_cross_dockets(
    docket: Docket, other_docket: Docket
) -> None:
    """A state event published on ``docket``'s channel must not be
    delivered to a subscriber on ``other_docket``'s channel for the
    same task key.  Pub/sub channels are prefix-scoped just like
    Redis keys."""
    task_key = "shared-name"
    other_events: list[dict[str, object]] = []
    ready = asyncio.Event()

    async def collector() -> None:
        async with other_docket._pubsub() as pubsub:  # pyright: ignore[reportPrivateUsage]
            await pubsub.subscribe(other_docket.key(f"state:{task_key}"))
            ready.set()
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue  # pragma: no cover
                data = message["data"]
                payload = data.decode() if isinstance(data, bytes) else data
                other_events.append(json.loads(payload))

    collector_task = asyncio.create_task(collector())
    await ready.wait()

    async def the_task() -> None: ...

    docket.register(the_task)
    # Trigger state-event publishing on docket's channel.
    await docket.add(the_task, key=task_key)()
    # Give the publish a moment to land.
    await asyncio.sleep(0.05)

    collector_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await collector_task

    assert other_events == [], (
        f"other_docket subscribers must not receive state events from "
        f"a different docket's channel; got: {other_events!r}"
    )
