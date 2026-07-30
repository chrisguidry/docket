"""Reliable, keyed message delivery backed by Redis Streams."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from redis.exceptions import ConnectionError, ResponseError, TimeoutError

from ._queue_scripts import (
    acknowledge_message,
    ensure_consumer_group,
    put_message,
    release_message,
)

if TYPE_CHECKING:
    from .docket import Docket

__all__ = ["Queue", "QueueMessage", "QueueSubscription"]

logger: logging.Logger = logging.getLogger(__name__)


@dataclass(eq=False)
class QueueMessage:
    """A message claimed by a :class:`QueueSubscription`.

    Call :meth:`acknowledge` after the downstream consumer accepts the
    message. Call :meth:`release` to atomically move it to another topic for
    immediate redelivery. If neither method is called, another subscription
    can reclaim it after the visibility timeout.
    """

    data: bytes
    key: str
    topic: str
    _subscription: QueueSubscription = field(repr=False)
    _message_id: bytes = field(repr=False)
    _settled: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    async def acknowledge(self) -> None:
        """Permanently remove this delivery from the queue."""
        await self._subscription._acknowledge(self)

    async def release(self, topic: str, *, max_size: int = 0) -> None:
        """Move this delivery to ``topic`` for immediate redelivery.

        If ``max_size`` is positive, wait until the destination topic has
        capacity. The move is atomic: the original remains claimable until
        the destination accepts it.
        """
        await self._subscription._release(self, topic, max_size=max_size)


class Queue:
    """A durable keyed message queue within a :class:`Docket`.

    Message keys are unique across the queue until acknowledgement. Publishing
    the same key again while it is queued or in flight is an idempotent no-op.
    Topics provide independent FIFO lanes and capacity limits.
    """

    def __init__(
        self,
        docket: Docket,
        name: str,
        *,
        idle_ttl: timedelta = timedelta(hours=1),
        acknowledgement_ttl: timedelta = timedelta(0),
    ) -> None:
        self.docket: Docket = docket
        self.name: str = name
        self.idle_ttl: timedelta = idle_ttl
        self.acknowledgement_ttl: timedelta = acknowledgement_ttl

    async def put(
        self,
        topic: str,
        data: bytes,
        *,
        key: str | None = None,
        max_size: int = 0,
    ) -> bool:
        """Publish a message, waiting for topic capacity when bounded.

        Args:
            topic: FIFO lane that subscriptions consume from.
            data: Opaque message payload.
            key: Idempotency key. Defaults to a fresh UUID.
            max_size: Maximum messages queued or in flight on this topic.
                Zero means unbounded.

        Returns:
            ``True`` when published, or ``False`` when ``key`` was already
            present in this queue.
        """
        if max_size < 0:
            raise ValueError("max_size must be non-negative")
        if self.acknowledgement_ttl < timedelta(0):
            raise ValueError("acknowledgement_ttl must be non-negative")
        message_key = key or str(uuid4())
        while True:
            async with self.docket.redis() as redis:
                result = await put_message(
                    redis,
                    stream_key=self._stream_key(topic),
                    deduplication_key=self._deduplication_key,
                    message_key=message_key,
                    data=data,
                    max_size=max_size,
                    now_timestamp=time.time(),
                )
            if result not in (b"FULL", "FULL"):
                return result not in (b"DUPLICATE", "DUPLICATE")
            await asyncio.sleep(0.01)

    def subscribe(
        self,
        topics: Sequence[str] | Mapping[str, int],
        *,
        visibility_timeout: timedelta,
        group: str = "consumers",
    ) -> QueueSubscription:
        """Create a subscription to one or more topics.

        A mapping assigns lower numeric priorities to topics that should be
        delivered first. A sequence gives every topic equal priority.
        """
        priorities = (
            dict(topics)
            if isinstance(topics, Mapping)
            else {topic: 0 for topic in topics}
        )
        if not priorities:
            raise ValueError("at least one topic is required")
        if visibility_timeout <= timedelta(0):
            raise ValueError("visibility_timeout must be positive")
        return QueueSubscription(self, priorities, visibility_timeout, group)

    @property
    def _deduplication_key(self) -> str:
        return self.docket.key(f"queues:{self.name}:messages")

    def _stream_key(self, topic: str) -> str:
        return self.docket.key(f"queues:{self.name}:topics:{topic}")

    @property
    def _idle_ttl_seconds(self) -> int:
        return max(1, int(self.idle_ttl.total_seconds()))

    @property
    def _acknowledged_until(self) -> float:
        if not self.acknowledgement_ttl:
            return 0
        return time.time() + self.acknowledgement_ttl.total_seconds()


class QueueSubscription:
    """A competing-consumer subscription with visibility-based redelivery."""

    def __init__(
        self,
        queue: Queue,
        priorities: Mapping[str, int],
        visibility_timeout: timedelta,
        group: str,
    ) -> None:
        self.queue: Queue = queue
        self.priorities: dict[str, int] = dict(priorities)
        self.visibility_timeout: timedelta = visibility_timeout
        self.group: str = group
        self.consumer: str = str(uuid4())
        self._available: asyncio.PriorityQueue[tuple[int, int, QueueMessage]] = (
            asyncio.PriorityQueue(maxsize=max(1, len(priorities)))
        )
        self._sequence = itertools.count()
        self._outstanding: set[QueueMessage] = set()
        self._tasks: list[asyncio.Task[None]] = []
        self._initialized_streams: set[str] = set()
        self._next_recovery: dict[str, float] = {}
        self._entered = False

    async def __aenter__(self) -> QueueSubscription:
        if self._entered:
            raise RuntimeError("queue subscription is already active")
        self._entered = True
        for topic, priority in self.priorities.items():
            self._tasks.append(asyncio.create_task(self._consume(topic, priority)))
        self._tasks.append(asyncio.create_task(self._renew_visibility()))
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._entered = False

    async def receive(self, *, timeout: float | None = None) -> QueueMessage:
        """Wait for the next claimed message."""
        if not self._entered:
            raise RuntimeError("queue subscription is not active")
        if timeout is None:
            _, _, message = await self._available.get()
        else:
            _, _, message = await asyncio.wait_for(
                self._available.get(), timeout=timeout
            )
        return message

    async def _consume(self, topic: str, priority: int) -> None:
        while True:
            try:
                message = await self._claim(topic)
                self._outstanding.add(message)
                await self._available.put((priority, next(self._sequence), message))
                await message._settled.wait()
            except asyncio.CancelledError:
                raise
            except (ConnectionError, ResponseError, TimeoutError):
                logger.warning(
                    "Queue consumer %s lost its Redis connection; retrying",
                    self.consumer,
                    exc_info=True,
                )
                await asyncio.sleep(0.5)

    async def _claim(self, topic: str) -> QueueMessage:
        stream_key = self.queue._stream_key(topic)
        while True:
            async with self.queue.docket.redis() as redis:
                if stream_key not in self._initialized_streams:
                    await ensure_consumer_group(
                        redis,
                        stream_key=stream_key,
                        group_name=self.group,
                        idle_ttl_seconds=self.queue._idle_ttl_seconds,
                    )
                    self._initialized_streams.add(stream_key)

                loop_time = asyncio.get_running_loop().time()
                if loop_time >= self._next_recovery.get(topic, 0):
                    recovered = await redis.xautoclaim(
                        stream_key,
                        self.group,
                        self.consumer,
                        min_idle_time=int(
                            self.visibility_timeout.total_seconds() * 1000
                        ),
                        start_id="0-0",
                        count=1,
                    )
                    if recovered[1]:
                        message_id, fields = recovered[1][0]
                        return self._message(topic, message_id, fields)
                    self._next_recovery[topic] = loop_time + min(
                        1, self.visibility_timeout.total_seconds() / 2
                    )

                try:
                    result = await redis.xreadgroup(
                        self.group,
                        self.consumer,
                        streams={stream_key: ">"},
                        count=1,
                        block=1000,
                    )
                except ResponseError as exc:
                    if "NOGROUP" not in str(exc):
                        raise
                    self._initialized_streams.discard(stream_key)
                    continue
            if result:
                _, messages = result[0]
                message_id, fields = messages[0]
                return self._message(topic, message_id, fields)

    def _message(
        self, topic: str, message_id: bytes, fields: Mapping[bytes, bytes]
    ) -> QueueMessage:
        try:
            key = fields[b"key"].decode()
            data = fields[b"data"]
        except KeyError as exc:
            raise ValueError(
                f"queue message {message_id!r} is missing {exc.args[0]!r}"
            ) from exc
        return QueueMessage(
            data=data,
            key=key,
            topic=topic,
            _subscription=self,
            _message_id=message_id,
        )

    async def _acknowledge(self, message: QueueMessage) -> None:
        """Acknowledge a message claimed by this subscription."""
        if message._settled.is_set():
            return
        async with self.queue.docket.redis() as redis:
            await acknowledge_message(
                redis,
                stream_key=self.queue._stream_key(message.topic),
                deduplication_key=self.queue._deduplication_key,
                group_name=self.group,
                message_id=message._message_id,
                message_key=message.key,
                idle_ttl_seconds=self.queue._idle_ttl_seconds,
                acknowledged_until=self.queue._acknowledged_until,
            )
        self._settle(message)

    async def _release(
        self, message: QueueMessage, topic: str, *, max_size: int
    ) -> None:
        """Release a message to another topic for immediate redelivery."""
        if max_size < 0:
            raise ValueError("max_size must be non-negative")
        if message._settled.is_set():
            return
        while True:
            async with self.queue.docket.redis() as redis:
                result = await release_message(
                    redis,
                    source_stream_key=self.queue._stream_key(message.topic),
                    destination_stream_key=self.queue._stream_key(topic),
                    group_name=self.group,
                    message_id=message._message_id,
                    message_key=message.key,
                    data=message.data,
                    max_size=max_size,
                    idle_ttl_seconds=self.queue._idle_ttl_seconds,
                )
            if result not in (b"FULL", "FULL"):
                self._settle(message)
                return
            await asyncio.sleep(0.01)

    def _settle(self, message: QueueMessage) -> None:
        self._outstanding.discard(message)
        message._settled.set()

    async def _renew_visibility(self) -> None:
        interval = max(0.01, self.visibility_timeout.total_seconds() / 4)
        while True:
            await asyncio.sleep(interval)
            by_topic: dict[str, list[bytes]] = {}
            for message in self._outstanding:
                by_topic.setdefault(message.topic, []).append(message._message_id)
            if not by_topic:
                continue
            try:
                async with self.queue.docket.redis() as redis:
                    for topic, message_ids in by_topic.items():
                        await redis.xclaim(
                            self.queue._stream_key(topic),
                            self.group,
                            self.consumer,
                            min_idle_time=0,
                            message_ids=message_ids,
                            idle=0,
                            justid=True,
                        )
            except (ConnectionError, ResponseError, TimeoutError):
                logger.warning(
                    "Queue consumer %s could not renew message visibility",
                    self.consumer,
                    exc_info=True,
                )


class DocketQueueMixin:
    """Construct reliable message queues scoped to a Docket."""

    def queue(
        self,
        name: str,
        *,
        acknowledgement_ttl: timedelta = timedelta(0),
    ) -> Queue:
        """Return a reliable message queue scoped to this Docket.

        Queues provide keyed, at-least-once delivery independently of task
        execution. Use them when another runtime owns the work but needs
        Docket's Redis-backed publication, acknowledgement, and crash
        redelivery semantics.

        Args:
            name: Stable queue name shared by publishers and subscribers.
            acknowledgement_ttl: How long acknowledged message keys remain
                deduplicated. This supports repair loops that may briefly
                rediscover already-delivered work.
        """
        return Queue(
            cast("Docket", self),
            name,
            acknowledgement_ttl=acknowledgement_ttl,
        )
