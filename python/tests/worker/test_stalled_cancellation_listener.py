"""The cancellation listener recovers when its pub/sub connection dies.

When TCP keepalive ends a pub/sub socket, redis-py before 8.2 returns None
from every later timed read without waiting.  A listener that skips each None
then never yields, and the worker's whole event loop stops.
"""

import asyncio
import errno
import logging
from contextlib import asynccontextmanager, suppress
from typing import Any, AsyncGenerator, Iterator
from unittest.mock import patch

import pytest
from redis.exceptions import ConnectionError

import docket._subscription
from docket import Docket, Worker
from docket._redis import RedisConnection
from tests.conftest import skip_cluster, skip_memory, wait_until


class ListenerPubSub:
    """A real pub/sub connection that the test can stall: once ``stalled`` is
    set, reads return None without waiting and a PING fails, as they do on a
    connection that keepalive ended."""

    def __init__(self, wrapped: Any) -> None:
        self._wrapped = wrapped
        self.stalled = False
        self.stalled_reads = 0
        self.confirmations = 0
        self.reads = 0
        self.spun = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    def fail_transport(self) -> None:  # pragma: no cover - needs a real Redis
        """Close the socket the way asyncio does when keepalive gives up on
        it, through asyncio's private ``_fatal_error``."""
        transport = self._wrapped.connection._writer.transport
        transport._fatal_error(
            OSError(errno.ETIMEDOUT, "Connection timed out"),
            "Fatal read error on socket transport",
        )

    async def ping(self) -> Any:
        if self.stalled:
            raise AttributeError("'NoneType' object has no attribute '_add_writer'")
        # redis-py 8.2 reconnects on the read, so only earlier ones get here
        return await self._wrapped.ping()  # pragma: no cover

    async def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float | None = 0.0
    ) -> dict[str, Any] | None:
        # A listener that never yields can't be interrupted by a timeout, so
        # this ends a spin and records it for the test to fail on.
        self.reads += 1
        if self.reads >= 1000:  # pragma: no cover
            self.spun = True
            raise ConnectionError("read the connection 1000 times")
        if self.stalled:
            self.stalled_reads += 1
            return None
        message = await self._wrapped.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages, timeout=timeout
        )
        if message is not None and message["type"] == "psubscribe":
            self.confirmations += 1
        return message


@pytest.fixture
def pubsubs() -> Iterator[list[ListenerPubSub]]:
    """Every pub/sub connection opened while the test runs, in order.  The
    memory backend's subscriptions get no probe, since its reads always
    wait, so here they PING through the wrapper like redis-py's do."""
    opened: list[ListenerPubSub] = []
    original_pubsub = RedisConnection.pubsub

    @asynccontextmanager
    async def listener_pubsub(self: RedisConnection) -> AsyncGenerator[Any, None]:
        async with original_pubsub(self) as pubsub:
            listener = ListenerPubSub(pubsub)
            opened.append(listener)
            yield listener

    with (
        patch.object(RedisConnection, "pubsub", listener_pubsub),
        patch.object(docket._subscription, "no_probe", docket._subscription.ping),
    ):
        yield opened


def confirmations(pubsubs: list[ListenerPubSub]) -> int:
    return sum(pubsub.confirmations for pubsub in pubsubs)


@pytest.fixture
def slow_task(docket: Docket) -> tuple[asyncio.Event, asyncio.Event]:
    """A registered task that runs until it is cancelled; the events say when
    it started and when it was cancelled."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow_task():
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    docket.register(slow_task)
    return started, cancelled


@asynccontextmanager
async def running(worker: Worker) -> AsyncGenerator[None, None]:
    worker_run = asyncio.create_task(worker.run_forever())
    try:
        yield
    finally:
        worker_run.cancel()
        with suppress(asyncio.CancelledError):  # pragma: no branch
            await worker_run


async def cancel_a_running_task(
    docket: Docket, slow_task: tuple[asyncio.Event, asyncio.Event]
) -> None:
    started, cancelled = slow_task
    execution = await docket.add("slow_task")()
    await asyncio.wait_for(started.wait(), timeout=5.0)
    await docket.cancel(execution.key)
    await asyncio.wait_for(cancelled.wait(), timeout=5.0)


async def test_cancellation_listener_reconnects_from_a_stalled_connection(
    docket: Docket,
    worker: Worker,
    pubsubs: list[ListenerPubSub],
    slow_task: tuple[asyncio.Event, asyncio.Event],
    caplog: pytest.LogCaptureFixture,
):
    async with running(worker):
        await wait_until(
            lambda: confirmations(pubsubs) == 1,
            timeout=10.0,
            description="the cancellation listener to subscribe",
        )
        with caplog.at_level(logging.WARNING):
            pubsubs[0].stalled = True
            await wait_until(
                lambda: confirmations(pubsubs) == 2,
                timeout=10.0,
                description="the cancellation listener to subscribe again",
            )

        assert "Redis error in cancellation listener, reconnecting..." in caplog.text
        assert pubsubs[0].stalled_reads < 10
        await cancel_a_running_task(docket, slow_task)


@skip_memory
@skip_cluster
async def test_cancellation_listener_recovers_when_keepalive_ends_its_socket(  # pragma: no cover
    docket: Docket,
    worker: Worker,
    pubsubs: list[ListenerPubSub],
    slow_task: tuple[asyncio.Event, asyncio.Event],
):
    """The listener either reconnects itself or reads on after redis-py
    reconnects and resubscribes, depending on the redis-py and Python
    versions.  Either way it subscribes again and never spins."""
    async with running(worker):
        await wait_until(
            lambda: confirmations(pubsubs) == 1,
            timeout=10.0,
            description="the cancellation listener to subscribe",
        )
        pubsubs[0].fail_transport()
        await wait_until(
            lambda: confirmations(pubsubs) == 2,
            timeout=10.0,
            description="the cancellation listener to subscribe again",
        )

        assert not any(pubsub.spun for pubsub in pubsubs)
        await cancel_a_running_task(docket, slow_task)
