"""A pub/sub subscription that moves to a new connection after a rotation.

A subscribed RESP2 connection can't send AUTH, so a rotation from a
StreamingCredentialProvider never reaches it, and it stays authenticated with
the credentials it connected with.  Some servers close a connection a fixed
time after its last AUTH: ElastiCache with IAM authentication closes it after
12 hours.  redis-py then reconnects and subscribes again, but pub/sub doesn't
redeliver, so anything published in between is lost.

So after each rotation, the task that reads a Subscription opens a second
connection, which authenticates with the current credentials, and waits for the
server to confirm its subscriptions.  Only then does it unsubscribe the first
connection and read what that connection still holds.  The two connections are
subscribed at the same time for one round trip, so a message published then
arrives on both, and the reader receives it twice.  No message is lost.

The reader owns its connection's socket, so the move happens in the reader's
own task, between two reads.
"""

from collections import deque
from contextlib import AsyncExitStack
from types import TracebackType
from typing import TYPE_CHECKING, Any, AsyncIterator, Sequence

from ._redis import PubSubClient

if TYPE_CHECKING:  # pragma: no cover
    from .docket import Docket

# How long listen() waits on one read before it checks for a rotation.  A
# rotation reaches a listening Subscription within this long.
LISTEN_READ_TIMEOUT: float = 1.0

Message = dict[str, Any]

SUBSCRIBED = ("subscribe", "psubscribe")
UNSUBSCRIBED = ("unsubscribe", "punsubscribe")


class Subscription:
    """Messages published to some channels and patterns, read by one task.

    Entering it subscribes and waits for the server to confirm, so a message
    published after ``__aenter__`` returns reaches the reader.
    """

    def __init__(
        self,
        docket: "Docket",
        *,
        channels: Sequence[str] = (),
        patterns: Sequence[str] = (),
    ) -> None:
        self._docket = docket
        self._channels = list(channels)
        self._patterns = list(patterns)
        self._received: deque[Message] = deque()
        self._stack = AsyncExitStack()

    async def __aenter__(self) -> "Subscription":
        self._rotations = self._docket._redis.rotations  # pyright: ignore[reportPrivateUsage]
        try:
            self._pubsub = await self._subscribe(self._stack, self._received)
        except BaseException:
            await self._stack.aclose()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._stack.aclose()

    async def get_message(self, timeout: float) -> Message | None:
        """The next published message, or None if none arrives within
        ``timeout`` seconds.  Subscription confirmations are left out."""
        if self._docket._redis.rotations != self._rotations:  # pyright: ignore[reportPrivateUsage]
            await self._move()
        if self._received:
            return self._received.popleft()
        return await self._pubsub.get_message(
            ignore_subscribe_messages=True, timeout=timeout
        )

    async def listen(self) -> AsyncIterator[Message]:
        """Every published message, until the reader stops."""
        while True:
            message = await self.get_message(timeout=LISTEN_READ_TIMEOUT)
            if message is None:
                continue  # pragma: no cover - CPython gh-106749
            yield message

    async def _subscribe(
        self, stack: AsyncExitStack, received: deque[Message]
    ) -> PubSubClient:
        """Open a connection and subscribe it, waiting for every confirmation.
        A message published to a channel already confirmed can arrive before
        the last confirmation, so those go to ``received``."""
        pubsub = await stack.enter_async_context(self._docket._pubsub())  # pyright: ignore[reportPrivateUsage]
        if self._channels:
            await pubsub.subscribe(*self._channels)
        if self._patterns:
            await pubsub.psubscribe(*self._patterns)
        await self._read_until(pubsub, SUBSCRIBED, received)
        return pubsub

    async def _read_until(
        self,
        pubsub: PubSubClient,
        confirmations: tuple[str, str],
        received: deque[Message],
    ) -> None:
        """Read until the server confirms each channel and pattern, keeping the
        published messages that arrive first."""
        remaining = len(self._channels) + len(self._patterns)
        while remaining:
            message = await pubsub.get_message(
                ignore_subscribe_messages=False, timeout=None
            )
            if message is None:  # pragma: no cover - CPython gh-106749
                continue
            if message["type"] in confirmations:
                remaining -= 1
            else:  # pragma: no cover - CPython gh-106749 hides this on 3.11
                received.append(message)

    async def _move(self) -> None:
        """Subscribe a new connection, then unsubscribe the old one."""
        self._rotations = self._docket._redis.rotations  # pyright: ignore[reportPrivateUsage]

        stack = AsyncExitStack()
        early: deque[Message] = deque()
        try:
            pubsub = await self._subscribe(stack, early)

            # Everything the old connection received before the new one
            # subscribed comes first.  Redis delivers what it published before
            # the UNSUBSCRIBE ahead of the confirmation, but the memory backend
            # drops a channel's queued messages once it is unsubscribed, so
            # those are read first.
            old = self._pubsub
            if self._docket._redis.is_memory:  # pyright: ignore[reportPrivateUsage]  # pragma: no cover - only the memory backend
                while message := await old.get_message(
                    ignore_subscribe_messages=True, timeout=0
                ):
                    self._received.append(message)
            if self._channels:
                await old.unsubscribe(*self._channels)
            if self._patterns:
                await old.punsubscribe(*self._patterns)
            await self._read_until(old, UNSUBSCRIBED, self._received)
        except BaseException:
            await stack.aclose()
            raise
        self._received.extend(early)

        await self._stack.aclose()
        self._stack = stack
        self._pubsub = pubsub
