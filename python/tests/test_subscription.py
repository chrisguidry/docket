"""A Subscription confirms before it returns and hands its reader only
published messages.

tests/test_credential_rotation.py re-authenticates subscriptions on a real
Redis.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, AsyncIterator

import pytest

from docket import Docket
from docket._redis import PubSubClient
from docket._subscription import Subscription


@pytest.mark.parametrize(
    ("channels", "patterns", "published_to"),
    [
        pytest.param(["state:a"], [], "state:a", id="channel"),
        pytest.param([], ["cancel:*"], "cancel:a", id="pattern"),
    ],
)
async def test_a_message_published_right_after_entering_arrives(
    docket: Docket, channels: list[str], patterns: list[str], published_to: str
):
    async with docket._subscription(
        channels=[docket.key(c) for c in channels],
        patterns=[docket.key(p) for p in patterns],
    ) as subscription:
        await docket._publish(docket.key(published_to), "hello")
        message = await subscription.get_message(timeout=5)

    assert message is not None
    assert message["data"] == b"hello"


async def test_get_message_returns_none_when_nothing_is_published(docket: Docket):
    async with docket._subscription(channels=[docket.key("state:a")]) as subscription:
        assert await subscription.get_message(timeout=0.01) is None


async def test_listen_yields_published_messages_in_order(docket: Docket):
    channel = docket.key("state:a")
    async with docket._subscription(channels=[channel]) as subscription:
        for i in range(3):
            await docket._publish(channel, f"{i}")
        messages = subscription.listen()
        received = [(await anext(messages))["data"] for _ in range(3)]

    assert received == [b"0", b"1", b"2"]


class ScriptedPubSub:
    """A pub/sub connection whose reads return ``replies`` in order, then None.
    Redis can't be made to send these replies at a chosen moment."""

    def __init__(self, replies: list[dict[str, Any]]) -> None:
        self.replies = deque(replies)

    async def subscribe(self, *channels: Any) -> None:
        pass

    async def psubscribe(self, *patterns: Any) -> None:
        pass

    async def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float | None = 0.0
    ) -> dict[str, Any] | None:
        return self.replies.popleft() if self.replies else None

    def listen(self) -> AsyncIterator[dict[str, Any]]:
        raise NotImplementedError  # pragma: no cover

    async def aclose(self) -> None:
        pass  # pragma: no cover


@asynccontextmanager
async def opened(pubsub: PubSubClient) -> AsyncGenerator[PubSubClient, None]:
    yield pubsub


def reply(type: str, data: Any = b"") -> dict[str, Any]:
    return {"type": type, "pattern": None, "channel": b"c", "data": data}


@pytest.mark.parametrize(
    "dropped",
    [
        # The server's "+OK" to an AUTH that a rotation wrote
        pytest.param(reply("pong", b"OK"), id="auth-reply"),
        # The confirmations redis-py reads when it reconnects and resubscribes
        pytest.param(reply("subscribe", 1), id="resubscribed"),
        pytest.param(reply("psubscribe", 1), id="presubscribed"),
    ],
)
async def test_get_message_drops_replies_that_are_not_messages(
    dropped: dict[str, Any],
):
    pubsub = ScriptedPubSub(
        [reply("subscribe", 1), dropped, reply("message", b"published")]
    )
    async with Subscription(opened(pubsub), channels=["c"]) as subscription:
        message = await subscription.get_message(timeout=0)

    assert message == reply("message", b"published")


async def test_an_auth_reply_before_the_confirmation_is_dropped():
    """A rotation can write AUTH between the SUBSCRIBE and its confirmation."""
    pubsub = ScriptedPubSub(
        [reply("pong", b"OK"), reply("subscribe", 1), reply("message", b"published")]
    )
    async with Subscription(opened(pubsub), channels=["c"]) as subscription:
        message = await subscription.get_message(timeout=0)

    assert message == reply("message", b"published")


async def test_messages_before_the_last_confirmation_are_kept():
    """A message on a channel the server has confirmed can arrive before the
    confirmation of a pattern subscribed after it."""
    pubsub = ScriptedPubSub(
        [
            reply("subscribe", 1),
            reply("message", b"early"),
            reply("psubscribe", 2),
            reply("pmessage", b"later"),
        ]
    )
    async with Subscription(
        opened(pubsub), channels=["c"], patterns=["p*"]
    ) as subscription:
        first = await subscription.get_message(timeout=0)
        second = await subscription.get_message(timeout=0)

    assert [first, second] == [reply("message", b"early"), reply("pmessage", b"later")]


async def test_a_failed_subscribe_closes_the_connection():
    closed = asyncio.Event()

    class FailingPubSub(ScriptedPubSub):
        async def subscribe(self, *channels: Any) -> None:
            raise ConnectionError("subscribe failed")

    @asynccontextmanager
    async def opening() -> AsyncGenerator[PubSubClient, None]:
        try:
            yield FailingPubSub([])
        finally:
            closed.set()

    with pytest.raises(ConnectionError, match="subscribe failed"):
        async with Subscription(opening(), channels=["c"]):
            pass  # pragma: no cover

    assert closed.is_set()
