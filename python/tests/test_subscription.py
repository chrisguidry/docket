"""Moving a pub/sub subscription to a new connection after a rotation.

These count a rotation without sending AUTH, so they run on every backend.
tests/test_credential_rotation.py rotates real credentials on a real Redis.
"""

# pyright: reportPrivateUsage=false

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator
from unittest.mock import patch

import pytest

from docket import Docket
from docket._subscription import Subscription


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


async def test_a_rotation_loses_nothing_published_during_the_move(docket: Docket):
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


def second_pubsub_fails(
    docket: Docket, error: BaseException, opened: list[Any], closed: list[Any]
) -> Any:
    """Patches the docket so the second connection it opens can't subscribe."""
    original_pubsub = docket._pubsub

    @asynccontextmanager
    async def pubsub() -> AsyncGenerator[Any, None]:
        async with original_pubsub() as pubsub:
            opened.append(pubsub)
            if len(opened) == 2:

                async def fail(*channels: Any) -> None:
                    raise error

                pubsub.subscribe = fail
            try:
                yield pubsub
            finally:
                closed.append(pubsub)

    return patch.object(docket, "_pubsub", pubsub)


async def test_a_failed_move_keeps_the_old_connection(
    docket: Docket, caplog: pytest.LogCaptureFixture
):
    channel = docket.key("state:subscription")
    opened: list[Any] = []
    closed: list[Any] = []
    with second_pubsub_fails(docket, ConnectionError("pool is full"), opened, closed):
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
    with second_pubsub_fails(docket, asyncio.CancelledError(), opened, closed):
        async with Subscription(docket, channels=[channel]) as subscription:
            rotate(docket)
            with pytest.raises(asyncio.CancelledError):
                await subscription.get_message(timeout=0)
            assert closed == [opened[1]]


async def test_a_broken_old_connection_still_hands_over(
    docket: Docket, caplog: pytest.LogCaptureFixture
):
    channel = docket.key("state:subscription")
    async with Subscription(docket, channels=[channel]) as subscription:
        old = subscription._pubsub
        unsubscribe = old.unsubscribe

        async def fail(*channels: Any) -> None:
            # Closing the connection unsubscribes it again, which works.
            old.unsubscribe = unsubscribe
            raise ConnectionError("the old connection is gone")

        old.unsubscribe = fail
        rotate(docket)
        await docket._publish(channel, "after")
        assert await read(subscription, 1) == [b"after"]
        assert subscription._pubsub is not old

    assert "The old connection failed" in caplog.text


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
    original_pubsub = docket._pubsub

    @asynccontextmanager
    async def publishing_pubsub() -> AsyncGenerator[Any, None]:
        async with original_pubsub() as pubsub:
            psubscribe = pubsub.psubscribe

            async def publish_then_psubscribe(*patterns: Any) -> None:
                await docket._publish(channel, "early")
                await psubscribe(*patterns)

            pubsub.psubscribe = publish_then_psubscribe
            yield pubsub

    with patch.object(docket, "_pubsub", publishing_pubsub):
        async with Subscription(
            docket, channels=[channel], patterns=[docket.key("cancel:*")]
        ) as subscription:
            assert await read(subscription, 1) == [b"early"]
