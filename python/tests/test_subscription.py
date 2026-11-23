"""Moving a pub/sub subscription to a new connection after a rotation.

These count a rotation without sending AUTH, so they run on every backend.
tests/test_credential_rotation.py rotates real credentials on a real Redis.
"""

# pyright: reportPrivateUsage=false

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from types import CoroutineType
from typing import Any, AsyncGenerator, Callable, cast
from unittest.mock import patch

import pytest

from docket import Docket
from docket._subscription import (
    SUBSCRIBED,
    UNSUBSCRIBED,
    Subscription,
    not_delivered,
)
from tests.conftest import skip_memory


def rotate(docket: Docket) -> None:
    docket._redis._rotation.rotations += 1


async def read(subscription: Subscription, count: int) -> list[bytes]:
    received: list[bytes] = []
    while len(received) < count:
        message = await subscription.get_message(timeout=1)
        assert message is not None, f"only {received} arrived"
        received.append(message["data"])
    return received


async def test_a_subscription_keeps_its_connection_without_a_rotation(
    docket: Docket,
):
    channel = docket.key("state:subscription")
    async with Subscription(docket, channels=[channel]) as subscription:
        before = subscription._pubsub
        await docket._publish(channel, "one")
        assert await read(subscription, 1) == [b"one"]
        assert subscription._pubsub is before


async def test_a_rotation_moves_a_subscription_to_a_new_connection(docket: Docket):
    """Messages that the old connection holds unread come first, then the
    ones published after the move, each once."""
    channel = docket.key("state:subscription")
    async with Subscription(docket, channels=[channel]) as subscription:
        before = subscription._pubsub
        for i in range(5):
            await docket._publish(channel, f"{i}")

        rotate(docket)
        assert await read(subscription, 1) == [b"0"]
        assert subscription._pubsub is not before

        for i in range(5, 10):
            await docket._publish(channel, f"{i}")
        assert await read(subscription, 9) == [f"{i}".encode() for i in range(1, 10)]


async def test_a_rotation_moves_a_pattern_subscription(docket: Docket):
    pattern = docket.key("cancel:*")
    async with Subscription(docket, patterns=[pattern]) as subscription:
        await docket._publish(docket.key("cancel:a"), "a")
        rotate(docket)
        await docket._publish(docket.key("cancel:b"), "b")
        assert await read(subscription, 2) == [b"a", b"b"]


# The memory backend delivers each message to a subscriber's queue from a
# background thread, so a message published just before the move can reach the
# old connection after it is unsubscribed.  Redis delivers it first.
@skip_memory
async def test_a_rotation_loses_nothing_published_during_the_move(  # pragma: no cover
    docket: Docket,
):
    channel = docket.key("state:subscription")
    sent = [f"{i}".encode() for i in range(200)]
    async with Subscription(docket, channels=[channel]) as subscription:

        async def publish() -> None:
            for i, message in enumerate(sent):
                await docket._publish(channel, message.decode())
                if i == 50:
                    rotate(docket)
                await asyncio.sleep(0)

        publishing = asyncio.create_task(publish())
        received: list[bytes] = []
        while not set(sent) <= set(received):
            message = await subscription.get_message(timeout=1)
            assert message is not None
            received.append(message["data"])
        await publishing

    # A message published while both connections are subscribed arrives twice.
    assert sorted(set(received), key=int) == sent


async def test_listen_yields_messages_across_a_rotation(docket: Docket):
    channel = docket.key("state:subscription")
    async with Subscription(docket, channels=[channel]) as subscription:
        messages = subscription.listen()
        await docket._publish(channel, "before")
        assert (await anext(messages))["data"] == b"before"

        async def next_message() -> dict[str, Any]:
            return await anext(messages)

        rotate(docket)
        with patch("docket._subscription.LISTEN_READ_TIMEOUT", 0.01):
            reading = asyncio.create_task(next_message())
            await asyncio.sleep(0.05)
            await docket._publish(channel, "after")
            assert (await reading)["data"] == b"after"


async def never_confirmed(*channels: Any) -> None:
    """A SUBSCRIBE or UNSUBSCRIBE that the server never answers."""


def failing_with(error: BaseException) -> Callable[..., CoroutineType[Any, Any, None]]:
    async def fail(*channels: Any) -> None:
        raise error

    return fail


def second_pubsub_subscribes_with(
    docket: Docket,
    subscribe: Callable[..., CoroutineType[Any, Any, None]],
    opened: list[Any],
    closed: list[Any],
) -> Any:
    """Patches the docket so the second connection it opens subscribes with
    ``subscribe``."""
    original_pubsub = docket._pubsub

    @asynccontextmanager
    async def pubsub() -> AsyncGenerator[Any, None]:
        async with original_pubsub() as pubsub:
            opened.append(pubsub)
            if len(opened) == 2:
                pubsub.subscribe = subscribe
            try:
                yield pubsub
            finally:
                closed.append(pubsub)

    return patch.object(docket, "_pubsub", pubsub)


@pytest.mark.parametrize(
    "subscribe",
    [
        pytest.param(failing_with(ConnectionError("pool is full")), id="fails"),
        pytest.param(never_confirmed, id="never-confirmed"),
    ],
)
async def test_a_failed_move_keeps_the_old_connection(
    docket: Docket,
    caplog: pytest.LogCaptureFixture,
    subscribe: Callable[..., CoroutineType[Any, Any, None]],
):
    channel = docket.key("state:subscription")
    opened: list[Any] = []
    closed: list[Any] = []
    with (
        patch("docket._subscription.MOVE_TIMEOUT", 0.1),
        second_pubsub_subscribes_with(docket, subscribe, opened, closed),
    ):
        async with Subscription(docket, channels=[channel]) as subscription:
            rotate(docket)
            await docket._publish(channel, "kept")
            assert await read(subscription, 1) == [b"kept"]
            assert subscription._pubsub is opened[0]
            assert closed == [opened[1]]

    assert closed == [opened[1], opened[0]]
    assert "Could not move a subscription" in caplog.text


async def test_a_cancelled_move_closes_the_new_connection(docket: Docket):
    channel = docket.key("state:subscription")
    opened: list[Any] = []
    closed: list[Any] = []
    with second_pubsub_subscribes_with(
        docket, failing_with(asyncio.CancelledError()), opened, closed
    ):
        async with Subscription(docket, channels=[channel]) as subscription:
            rotate(docket)
            with pytest.raises(asyncio.CancelledError):
                await subscription.get_message(timeout=0)
            assert closed == [opened[1]]


@pytest.mark.parametrize(
    "unsubscribe",
    [
        pytest.param(failing_with(ConnectionError("it is gone")), id="fails"),
        pytest.param(never_confirmed, id="never-confirmed"),
    ],
)
async def test_a_broken_old_connection_still_hands_over(
    docket: Docket,
    caplog: pytest.LogCaptureFixture,
    unsubscribe: Callable[..., CoroutineType[Any, Any, None]],
):
    channel = docket.key("state:subscription")
    async with Subscription(docket, channels=[channel]) as subscription:
        old = subscription._pubsub
        # Closing the connection unsubscribes it again, which works.
        old_stack = subscription._stack
        old_stack.push_async_callback(restore, old, "unsubscribe", old.unsubscribe)
        old.unsubscribe = unsubscribe
        rotate(docket)
        # A connection that is really broken holds nothing to read, so the
        # message goes out after the move, when only the new one can get it.
        with patch("docket._subscription.MOVE_TIMEOUT", 0.1):
            assert await subscription.get_message(timeout=0) is None
        await docket._publish(channel, "after")
        assert await read(subscription, 1) == [b"after"]
        assert subscription._pubsub is not old

    assert "The old connection failed" in caplog.text


async def restore(target: Any, name: str, value: Any) -> None:
    setattr(target, name, value)


async def test_a_failure_closing_the_old_connection_still_hands_over(
    docket: Docket, caplog: pytest.LogCaptureFixture
):
    channel = docket.key("state:subscription")
    async with Subscription(docket, channels=[channel]) as subscription:
        subscription._stack.push_async_callback(
            failing_with(ConnectionError("the close failed"))
        )
        await docket._publish(channel, "before")
        rotate(docket)
        await docket._publish(channel, "after")
        assert await read(subscription, 2) == [b"before", b"after"]

    assert "Could not close a subscription's old connection" in caplog.text


async def test_a_failed_subscribe_closes_the_connection(docket: Docket):
    original_pubsub = docket._pubsub
    closed: list[Any] = []

    @asynccontextmanager
    async def failing_pubsub() -> AsyncGenerator[Any, None]:
        async with original_pubsub() as pubsub:

            async def fail(*channels: Any) -> None:
                raise ConnectionError("subscribe failed")

            pubsub.subscribe = fail
            try:
                yield pubsub
            finally:
                closed.append(pubsub)

    with patch.object(docket, "_pubsub", failing_pubsub):
        with pytest.raises(ConnectionError, match="subscribe failed"):
            async with Subscription(
                docket, channels=[docket.key("state:subscription")]
            ):
                pass  # pragma: no cover

    assert len(closed) == 1


async def test_messages_before_the_last_confirmation_are_kept(docket: Docket):
    """A message on a channel the server has confirmed can arrive before the
    confirmation of a pattern subscribed after it."""
    channel = docket.key("state:subscription")
    replies = [
        {"type": "subscribe", "channel": channel, "data": 1},
        {"type": "message", "channel": channel, "data": b"early"},
        {"type": "psubscribe", "channel": docket.key("cancel:*"), "data": 2},
    ]

    class Replies:
        async def get_message(self, **kwargs: Any) -> dict[str, Any]:
            return replies.pop(0)

    subscription = Subscription(
        docket, channels=[channel], patterns=[docket.key("cancel:*")]
    )
    received: deque[dict[str, Any]] = deque()
    await subscription._read_until(cast(Any, Replies()), SUBSCRIBED, received)
    assert [message["data"] for message in received] == [b"early"]
    assert replies == []


async def test_a_confirmation_from_a_reconnect_is_not_a_message(docket: Docket):
    """redis-py subscribes again when it reconnects, so a subscribe
    confirmation can arrive while the subscription waits to be unsubscribed."""
    pattern = docket.key("cancel:*")
    replies = [
        {"type": "psubscribe", "channel": pattern, "data": 1},
        {"type": "punsubscribe", "channel": pattern, "data": 0},
    ]

    class Replies:
        async def get_message(self, **kwargs: Any) -> dict[str, Any]:
            return replies.pop(0)

    subscription = Subscription(docket, patterns=[pattern])
    received: deque[dict[str, Any]] = deque()
    await subscription._read_until(cast(Any, Replies()), UNSUBSCRIBED, received)
    assert list(received) == []
    assert replies == []


def message(data: bytes) -> dict[str, Any]:
    return {"type": "message", "pattern": None, "channel": b"c", "data": data}


@pytest.mark.parametrize(
    ("early", "delivered", "kept"),
    [
        ([], [b"a"], []),
        ([b"a"], [b"a"], []),
        ([b"a", b"b"], [b"a"], [b"b"]),
        ([b"a", b"a"], [b"a"], [b"a"]),
    ],
)
def test_only_the_new_connections_early_messages_that_are_not_copies_are_kept(
    early: list[bytes], delivered: list[bytes], kept: list[bytes]
):
    """What the new connection receives before its confirmations also reaches
    the old one, unless the old one broke first."""
    result = not_delivered(
        [message(data) for data in early], [message(data) for data in delivered]
    )
    assert [m["data"] for m in result] == kept
