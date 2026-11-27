"""A Subscription confirms before it returns and hands its reader only
published messages.

tests/test_credential_rotation.py re-authenticates subscriptions on a real
Redis.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import time
from collections import deque
from contextlib import asynccontextmanager, suppress
from typing import Any, AsyncGenerator, AsyncIterator, Awaitable, Callable, cast

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError

from docket import Docket
from docket._redis import PubSubClient
from docket._subscription import Message, Probe, Subscription, no_probe, ping
from tests._container import PROVIDER_ENABLED
from tests.conftest import skip_cluster, skip_memory


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


class Yielded:
    """A read that waits on the event loop once and returns None, as a read
    does that consumes a health-check pong."""


YIELDED = Yielded()

Script = list[dict[str, Any] | Yielded | None]


class ScriptedPubSub:
    """A pub/sub connection whose reads return ``replies`` in order.  A None
    in the script returns at once, as a read does that finds a health-check
    pong already buffered.  Once the script runs out, each read returns None
    without waiting, as redis-py does on a connection that TCP keepalive
    ended.  Redis can't be made to send these replies at a chosen moment."""

    def __init__(self, replies: Script) -> None:
        self.replies = deque(replies)
        self.reads = 0

    async def subscribe(self, *channels: Any) -> None:
        pass

    async def psubscribe(self, *patterns: Any) -> None:
        pass

    async def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float | None = 0.0
    ) -> dict[str, Any] | None:
        # Fails a reader that would otherwise read a dead connection forever
        # without yielding, which no timeout around the test can interrupt.
        self.reads += 1
        assert self.reads < 500, "kept reading a dead connection"
        reply = self.replies.popleft() if self.replies else None
        if isinstance(reply, Yielded):
            await asyncio.sleep(0)
            return None
        return reply

    def listen(self) -> AsyncIterator[dict[str, Any]]:
        raise NotImplementedError  # pragma: no cover

    async def aclose(self) -> None:
        pass  # pragma: no cover


@asynccontextmanager
async def opened(pubsub: PubSubClient) -> AsyncGenerator[PubSubClient, None]:
    yield pubsub


def reply(type: str, data: Any = b"") -> dict[str, Any]:
    return {"type": type, "pattern": None, "channel": b"c", "data": data}


def failing_with(error: Exception) -> Probe:
    """A probe of a dead connection, which fails with ``error``."""

    async def probe(pubsub: PubSubClient) -> None:
        raise error

    return probe


# How a PING on a dead connection fails: a redis-py error, or the error
# asyncio raises writing to a closed transport on Python 3.12 and 3.13
PROBE_ERRORS = [
    pytest.param(ConnectionError("Connection closed by server."), id="redis-error"),
    pytest.param(
        AttributeError("'NoneType' object has no attribute '_add_writer'"),
        id="closed-transport-attribute-error",
    ),
    pytest.param(
        TypeError("'NoneType' object is not callable"),
        id="closed-transport-type-error",
    ),
]

DEAD = failing_with(ConnectionError("Connection closed by server."))


@pytest.mark.parametrize(
    "dropped",
    [
        # The server's "+OK" to an AUTH that a rotation wrote
        pytest.param(reply("pong", b"OK"), id="auth-reply"),
        # The server's reply to a probe's PING
        pytest.param(reply("pong", b""), id="probe-reply"),
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
    async with Subscription(opened(pubsub), DEAD, channels=["c"]) as subscription:
        message = await subscription.get_message(timeout=0)

    assert message == reply("message", b"published")


async def test_an_auth_reply_before_the_confirmation_is_dropped():
    """A rotation can write AUTH between the SUBSCRIBE and its confirmation."""
    pubsub = ScriptedPubSub(
        [reply("pong", b"OK"), reply("subscribe", 1), reply("message", b"published")]
    )
    async with Subscription(opened(pubsub), DEAD, channels=["c"]) as subscription:
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
        opened(pubsub), DEAD, channels=["c"], patterns=["p*"]
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
        async with Subscription(opening(), DEAD, channels=["c"]):
            pass  # pragma: no cover

    assert closed.is_set()


async def next_message(subscription: Subscription, timeout: float | None) -> Message:
    """Read as the worker's cancellation listener does, skipping each None."""
    while (message := await subscription.get_message(timeout=timeout)) is None:
        pass
    return message


async def read_timed(subscription: Subscription) -> Message:
    return await next_message(subscription, 0.1)


async def read_blocking(subscription: Subscription) -> Message:
    return await next_message(subscription, None)


async def read_listening(subscription: Subscription) -> Message:
    return await anext(subscription.listen())


Reader = Callable[[Subscription], Awaitable[Message]]

PUBLISHED_MESSAGE = reply("message", b"published")

READERS = [
    pytest.param(read_timed, id="timed"),
    pytest.param(read_blocking, id="blocking"),
    pytest.param(read_listening, id="listen"),
]


@pytest.mark.parametrize("read", READERS)
@pytest.mark.parametrize("error", PROBE_ERRORS)
async def test_reading_a_dead_connection_raises(read: Reader, error: Exception):
    pubsub = ScriptedPubSub([reply("subscribe", 1)])
    probe = failing_with(error)
    async with Subscription(opened(pubsub), probe, channels=["c"]) as subscription:
        with pytest.raises(ConnectionError, match="a PING on it failed") as raised:
            await read(subscription)

    assert raised.value.__cause__ is error


async def test_subscribing_on_a_dead_connection_raises():
    pubsub = ScriptedPubSub([])
    with pytest.raises(ConnectionError, match="a PING on it failed"):
        async with Subscription(opened(pubsub), DEAD, channels=["c"]):
            pass  # pragma: no cover


@pytest.mark.parametrize("read", READERS)
async def test_a_probe_that_succeeds_keeps_reading(read: Reader):
    """A PING that succeeds shows the connection is alive, or that redis-py
    reconnected it, so the reads go on."""
    pubsub = ScriptedPubSub([reply("subscribe", 1), *[None] * 7, PUBLISHED_MESSAGE])
    async with Subscription(opened(pubsub), no_probe, channels=["c"]) as subscription:
        message = await read(subscription)

    assert message == PUBLISHED_MESSAGE


class LoopTurns:
    """How many times a task that does nothing but yield got to run."""

    def __init__(self) -> None:
        self.count = 0

    async def run(self) -> None:
        while True:
            self.count += 1
            await asyncio.sleep(0)


@pytest.fixture
async def loop_turns() -> AsyncGenerator[LoopTurns, None]:
    turns = LoopTurns()
    task = asyncio.create_task(turns.run())
    yield turns
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def test_a_dead_connection_lets_other_tasks_run_between_reads(
    loop_turns: LoopTurns,
):
    pubsub = ScriptedPubSub([reply("subscribe", 1)])
    async with Subscription(opened(pubsub), DEAD, channels=["c"]) as subscription:
        turns_before = loop_turns.count
        with pytest.raises(ConnectionError):
            await read_timed(subscription)

    empty_reads = pubsub.reads - 1
    assert loop_turns.count - turns_before >= empty_reads


@pytest.mark.parametrize("read", READERS)
@pytest.mark.parametrize(
    "script",
    [
        pytest.param([YIELDED], id="health-check-pong"),
        pytest.param([YIELDED, YIELDED, YIELDED, YIELDED], id="many-pongs"),
        pytest.param([None, None], id="buffered-pongs"),
        pytest.param([None, None, YIELDED, None, None], id="reset-by-a-wait"),
        pytest.param(
            [None, None, reply("pong", b"OK"), None, None], id="reset-by-a-reply"
        ),
    ],
)
async def test_empty_reads_short_of_the_probe_do_not_probe(
    read: Reader, script: Script
):
    pubsub = ScriptedPubSub([reply("subscribe", 1), *script, PUBLISHED_MESSAGE])
    async with Subscription(opened(pubsub), DEAD, channels=["c"]) as subscription:
        message = await read(subscription)

    assert message == PUBLISHED_MESSAGE


async def test_a_zero_timeout_returns_none_at_once_without_probing():
    pubsub = ScriptedPubSub(
        [reply("subscribe", 1), None, None, None, None, PUBLISHED_MESSAGE]
    )
    async with Subscription(opened(pubsub), DEAD, channels=["c"]) as subscription:
        message = await next_message(subscription, 0)

    assert message == PUBLISHED_MESSAGE


@pytest.fixture(
    params=[
        # With a socket timeout, a read that writes a PING suspends on Python
        # before 3.12, even when pongs are still queued
        pytest.param({}, id="default-socket-timeout"),
        pytest.param({"socket_timeout": None}, id="no-socket-timeout"),
    ]
)
async def health_checked_pubsub(  # pragma: no cover
    redis_url: str, request: pytest.FixtureRequest
) -> AsyncGenerator[PubSubClient, None]:
    """A pub/sub connection that PINGs on any read that comes more than 50 ms
    after the one before.  A URL takes only whole seconds."""
    client = Redis.from_url(redis_url, health_check_interval=0.05, **request.param)  # pyright: ignore[reportUnknownMemberType]
    async with client, client.pubsub() as pubsub:  # pyright: ignore[reportUnknownMemberType]
        yield cast(PubSubClient, pubsub)


async def read_slowly(  # pragma: no cover
    subscription: Subscription, count: int
) -> None:
    """Read ``count`` messages, each taking longer to handle than the health
    check interval, so each read writes a PING behind the messages queued."""
    for _ in range(count):
        await next_message(subscription, 0.1)
        await asyncio.sleep(0.1)


@skip_memory
@skip_cluster
@pytest.mark.skipif(PROVIDER_ENABLED, reason="the URL carries no credentials")
async def test_a_backlog_of_health_check_pongs_does_not_raise(  # pragma: no cover
    docket: Docket, health_checked_pubsub: PubSubClient
):
    """Once the queued messages are read, the pongs behind them are already
    buffered, so each comes back as a None that did not wait."""
    channel = docket.key("state:a")
    subscription = Subscription(opened(health_checked_pubsub), ping, channels=[channel])
    async with subscription:
        for i in range(6):
            await docket._publish(channel, f"{i}")
        await asyncio.sleep(0.05)
        await read_slowly(subscription, 6)

        await docket._publish(channel, "after")
        message = await next_message(subscription, 0.1)

    assert message["data"] == b"after"


@pytest.fixture
async def block_the_loop() -> AsyncGenerator[
    Callable[[], None], None
]:  # pragma: no cover
    """Starts a task that blocks the event loop for longer than the health
    check interval each time it runs, so every read starts a new interval.
    It starts only once the test subscribed: redis-py's own health check
    misreads replies when the loop blocks while it connects."""
    tasks: list[asyncio.Task[None]] = []

    async def block() -> None:
        while True:
            time.sleep(0.06)
            await asyncio.sleep(0)

    yield lambda: tasks.append(asyncio.create_task(block()))
    for task in tasks:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def read_idle(  # pragma: no cover
    subscription: Subscription, reads: int
) -> None:
    """Timed reads while nothing is published, as a listener waits."""
    for _ in range(reads):
        await subscription.get_message(timeout=0.1)


async def read_yielding(  # pragma: no cover
    subscription: Subscription, count: int
) -> None:
    """Read ``count`` messages, yielding to the event loop after each one, as
    a handler that awaits anything does."""
    for _ in range(count):
        await next_message(subscription, 0.1)
        await asyncio.sleep(0)


@skip_memory
@skip_cluster
@pytest.mark.skipif(PROVIDER_ENABLED, reason="the URL carries no credentials")
async def test_pongs_that_never_drain_do_not_raise(  # pragma: no cover
    docket: Docket,
    health_checked_pubsub: PubSubClient,
    block_the_loop: Callable[[], None],
):
    """Every read, None or not, writes a PING when the loop was blocked since
    the last one.  Once the queued messages are read, each idle read consumes
    a buffered pong without waiting and queues a new one, so the backlog of
    pongs never drains."""
    channel = docket.key("state:a")
    subscription = Subscription(opened(health_checked_pubsub), ping, channels=[channel])
    async with subscription:
        block_the_loop()
        for i in range(6):
            await docket._publish(channel, f"{i}")
        await asyncio.sleep(0.05)
        await read_yielding(subscription, 6)
        await read_idle(subscription, 20)

        await docket._publish(channel, "after")
        message = await next_message(subscription, 0.1)

    assert message["data"] == b"after"
