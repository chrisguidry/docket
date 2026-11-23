"""A pub/sub subscription that hands its reader only published messages."""

from collections import deque
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from types import TracebackType
from typing import Any, AsyncIterator, Iterable

from ._redis import PubSubClient

Message = dict[str, Any]

SUBSCRIBED = ("subscribe", "psubscribe")
PUBLISHED = ("message", "pmessage")


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
        *,
        channels: Iterable[str] = (),
        patterns: Iterable[str] = (),
    ) -> None:
        self._opening = pubsub
        self._channels = list(channels)
        self._patterns = list(patterns)
        # Published messages that arrived before the last confirmation
        self._early: deque[Message] = deque()
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
            message = await self._pubsub.get_message(timeout=None)
            if message is None:  # pragma: no cover - CPython gh-106749
                continue
            if message["type"] in SUBSCRIBED:
                remaining -= 1
            elif message["type"] in PUBLISHED:
                self._early.append(message)

    async def get_message(self, timeout: float | None) -> Message | None:
        """The next published message, or None if no reply arrives within
        ``timeout`` seconds.  Each reply that is not a published message is
        dropped, and the wait for the next one starts over."""
        if self._early:
            return self._early.popleft()
        while True:
            message = await self._pubsub.get_message(timeout=timeout)
            if message is None or message["type"] in PUBLISHED:
                return message

    async def listen(self) -> AsyncIterator[Message]:
        """Every published message, until the reader stops."""
        while True:
            message = await self.get_message(timeout=None)
            if message is None:  # pragma: no cover - CPython gh-106749
                continue
            yield message
