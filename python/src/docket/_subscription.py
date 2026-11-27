"""A pub/sub subscription that hands its reader only published messages."""

import asyncio
from collections import deque
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from types import TracebackType
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable, cast

from redis.asyncio.client import PubSub
from redis.exceptions import ConnectionError

from ._redis import PubSubClient

Message = dict[str, Any]

SUBSCRIBED = ("subscribe", "psubscribe")
PUBLISHED = ("message", "pmessage")

# A read that waits for a reply suspends, so the event loop runs.  A read that
# returns None without suspending found no reply and did not wait for one.
# redis-py does that on every read once TCP keepalive has ended the connection,
# and a reader that skips each None then never yields and stops the event loop.
#
# A healthy connection does it too, when the read consumes a health-check pong
# that was already buffered, and there is no bound on how many such reads come
# in a row: a reader that blocks the loop for longer than health_check_interval
# between reads writes a new PING on every read, so each pong it consumes is
# replaced by another.  Counting those reads cannot tell the two connections
# apart, so after this many in a row the connection is probed instead.  A PING
# settles it: on a dead connection it fails, and when it succeeds the
# connection is alive or redis-py has just reconnected and resubscribed it.
EMPTY_READS_BEFORE_PROBE = 3

# Checks that a pub/sub connection is alive, by raising if it is not.
Probe = Callable[[PubSubClient], Awaitable[object]]


async def ping(pubsub: PubSubClient) -> object:
    """Probe a redis-py pub/sub connection with a PING.  The pong comes back
    later as a reply of type "pong", which Subscription drops."""
    return await cast(PubSub, pubsub).ping()


async def no_probe(pubsub: PubSubClient) -> None:
    """The memory backend's timed and blocking reads always suspend, so its
    connections never need a probe."""


class Subscription:
    """Messages published to some channels and patterns, read by one task.

    Entering it subscribes and waits for the server to confirm, so a message
    published after ``__aenter__`` returns reaches the reader.  The reader gets
    only published messages.  It never sees the confirmations, including the
    ones redis-py reads when it reconnects and subscribes again, or the replies
    to the AUTH commands that a credential rotation writes onto the connection
    (see ``_redis_credentials``).
    """

    def __init__(
        self,
        pubsub: AbstractAsyncContextManager[PubSubClient],
        probe: Probe,
        *,
        channels: Iterable[str] = (),
        patterns: Iterable[str] = (),
    ) -> None:
        self._opening = pubsub
        self._probe = probe
        self._channels = list(channels)
        self._patterns = list(patterns)
        # Published messages that arrived before the last confirmation
        self._early: deque[Message] = deque()
        # Reads in a row that returned None without suspending
        self._empty_reads = 0
        self._stack = AsyncExitStack()

    async def __aenter__(self) -> "Subscription":
        async with AsyncExitStack() as stack:
            self._pubsub = await stack.enter_async_context(self._opening)
            if self._channels:
                await self._pubsub.subscribe(*self._channels)
            if self._patterns:
                await self._pubsub.psubscribe(*self._patterns)
            await self._confirm()
            self._stack = stack.pop_all()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._stack.aclose()

    async def _confirm(self) -> None:
        """Read until the server confirms each channel and pattern.  A message
        published to a channel already confirmed can arrive before the last
        confirmation, so those are kept for the reader."""
        remaining = len(self._channels) + len(self._patterns)
        while remaining:
            message = await self._read(timeout=None)
            if message is None:  # pragma: no cover - CPython gh-106749
                continue
            if message["type"] in SUBSCRIBED:
                remaining -= 1
            elif message["type"] in PUBLISHED:
                self._early.append(message)

    async def get_message(self, timeout: float | None) -> Message | None:
        """The next published message, or None if no reply arrives within
        ``timeout`` seconds.  Each reply that is not a published message is
        dropped, and the wait for the next one starts over.  Raises
        ConnectionError when the connection is dead (see ``_read``)."""
        if self._early:
            return self._early.popleft()
        while True:
            message = await self._read(timeout)
            if message is None or message["type"] in PUBLISHED:
                return message

    async def _read(self, timeout: float | None) -> Message | None:
        """The next reply from the connection, or None.  After
        ``EMPTY_READS_BEFORE_PROBE`` reads in a row return None without
        suspending, it probes the connection, and raises ConnectionError if the
        probe fails, so the reader reconnects instead of spinning.  A zero
        timeout asks for a read that does not wait, so it is not checked."""
        if timeout == 0:
            return await self._pubsub.get_message(timeout=0)

        # The event loop runs this callback only if the read suspends.  Elapsed
        # time can't stand in for this: a health-check pong or a RESP3 push
        # ends a healthy read early with None, and that read did suspend.
        suspended = False

        def mark_suspended() -> None:
            nonlocal suspended
            suspended = True

        handle = asyncio.get_running_loop().call_soon(mark_suspended)
        try:
            message = await self._pubsub.get_message(timeout=timeout)
        finally:
            handle.cancel()

        if message is not None or suspended:
            self._empty_reads = 0
            return message

        self._empty_reads += 1
        # The read did not yield, and a reader that skips the None would read
        # again at once, so yield here.  The marker above is already
        # cancelled, so this does not count as the read suspending.
        await asyncio.sleep(0)
        if self._empty_reads >= EMPTY_READS_BEFORE_PROBE:
            self._empty_reads = 0
            try:
                await self._probe(self._pubsub)
            except Exception as error:
                # On a dead connection the PING's write fails with a redis-py
                # error or, on some Python versions, with the AttributeError
                # or TypeError asyncio raises writing to a closed transport.
                # Callers reconnect on RedisError, so every failure is one.
                raise ConnectionError(
                    "The pub/sub connection keeps returning no reply without "
                    "waiting for one, and a PING on it failed, so it is dead"
                ) from error
        return None

    async def listen(self) -> AsyncIterator[Message]:
        """Every published message, until the reader stops or the connection
        dies (see ``_read``)."""
        while True:
            message = await self.get_message(timeout=None)
            if message is None:  # pragma: no cover - CPython gh-106749
                continue
            yield message
