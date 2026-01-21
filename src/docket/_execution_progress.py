"""Progress tracking for task executions."""

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, AsyncGenerator, Generator, Literal, TypedDict

from ._telemetry import suppress_instrumentation
from typing_extensions import Self

if TYPE_CHECKING:
    from .docket import Docket


class ProgressEvent(TypedDict):
    type: Literal["progress"]
    key: str
    current: int | None
    total: int
    message: str | None
    updated_at: str | None


class StateEvent(TypedDict):
    type: Literal["state"]
    key: str
    state: str
    when: str
    worker: str | None
    started_at: str | None
    completed_at: str | None
    error: str | None


class ExecutionProgress:
    """Manages user-reported progress for a task execution.

    Progress data is stored in Redis hash {docket}:progress:{key} and includes:
    - current: Current progress value (integer)
    - total: Total/target value (integer)
    - message: User-provided status message (string)
    - updated_at: Timestamp of last update (ISO 8601 string)

    This data is ephemeral and deleted when the task completes.
    """

    def __init__(self, docket: "Docket", key: str) -> None:
        """Initialize progress tracker for a specific task.

        Args:
            docket: The docket instance
            key: The task execution key
        """
        self.docket = docket
        self.key = key
        self._redis_key = docket.key(f"progress:{key}")
        self.current: int | None = None
        self.total: int = 1
        self.message: str | None = None
        self.updated_at: datetime | None = None

    @contextmanager
    def _maybe_suppress_instrumentation(self) -> Generator[None, None, None]:
        """Suppress OTel auto-instrumentation for internal Redis operations."""
        if not self.docket.enable_internal_instrumentation:
            with suppress_instrumentation():
                yield
        else:  # pragma: no cover
            yield

    @classmethod
    async def create(cls, docket: "Docket", key: str) -> Self:
        """Create and initialize progress tracker by reading from Redis.

        Args:
            docket: The docket instance
            key: The task execution key

        Returns:
            ExecutionProgress instance with attributes populated from Redis
        """
        instance = cls(docket, key)
        await instance.sync()
        return instance

    async def set_total(self, total: int) -> None:
        """Set the total/target value for progress tracking.

        Args:
            total: The total number of units to complete. Must be at least 1.
        """
        if total < 1:
            raise ValueError("Total must be at least 1")

        updated_at_dt = datetime.now(timezone.utc)
        updated_at = updated_at_dt.isoformat()
        async with self.docket.redis() as redis:
            await redis.hset(
                self._redis_key,
                mapping={
                    "total": str(total),
                    "updated_at": updated_at,
                },
            )
        # Update instance attributes
        self.total = total
        self.updated_at = updated_at_dt
        # Publish update event
        await self._publish({"total": total, "updated_at": updated_at})

    async def increment(self, amount: int = 1) -> None:
        """Atomically increment the current progress value.

        Args:
            amount: Amount to increment by. Must be at least 1.
        """
        if amount < 1:
            raise ValueError("Amount must be at least 1")

        updated_at_dt = datetime.now(timezone.utc)
        updated_at = updated_at_dt.isoformat()
        async with self.docket.redis() as redis:
            new_current = await redis.hincrby(self._redis_key, "current", amount)
            await redis.hset(
                self._redis_key,
                "updated_at",
                updated_at,
            )
        # Update instance attributes using Redis return value
        self.current = new_current
        self.updated_at = updated_at_dt
        # Publish update event with new current value
        await self._publish({"current": new_current, "updated_at": updated_at})

    async def set_message(self, message: str | None) -> None:
        """Update the progress status message.

        Args:
            message: Status message describing current progress
        """
        updated_at_dt = datetime.now(timezone.utc)
        updated_at = updated_at_dt.isoformat()
        async with self.docket.redis() as redis:
            await redis.hset(
                self._redis_key,
                mapping={
                    "message": message,
                    "updated_at": updated_at,
                },
            )
        # Update instance attributes
        self.message = message
        self.updated_at = updated_at_dt
        # Publish update event
        await self._publish({"message": message, "updated_at": updated_at})

    async def sync(self) -> None:
        """Synchronize instance attributes with current progress data from Redis.

        Updates self.current, self.total, self.message, and self.updated_at
        with values from Redis. Sets attributes to None if no data exists.
        """
        with self._maybe_suppress_instrumentation():
            async with self.docket.redis() as redis:
                data = await redis.hgetall(self._redis_key)
                if data:
                    self.current = int(data.get(b"current", b"0"))
                    self.total = int(data.get(b"total", b"100"))
                    self.message = (
                        data[b"message"].decode() if b"message" in data else None
                    )
                    self.updated_at = (
                        datetime.fromisoformat(data[b"updated_at"].decode())
                        if b"updated_at" in data
                        else None
                    )
                else:
                    self.current = None
                    self.total = 100
                    self.message = None
                    self.updated_at = None

    async def delete(self) -> None:
        """Delete the progress data from Redis.

        Called internally when task execution completes.
        """
        with self._maybe_suppress_instrumentation():
            async with self.docket.redis() as redis:
                await redis.delete(self._redis_key)
        # Reset instance attributes
        self.current = None
        self.total = 100
        self.message = None
        self.updated_at = None

    async def _publish(self, data: dict[str, Any]) -> None:
        """Publish progress update to Redis pub/sub channel.

        Args:
            data: Progress data to publish (partial update)
        """
        channel = self.docket.key(f"progress:{self.key}")
        payload: ProgressEvent = {
            "type": "progress",
            "key": self.key,
            "current": self.current if self.current is not None else 0,
            "total": self.total,
            "message": self.message,
            "updated_at": data.get("updated_at"),
        }
        await self.docket._publish(channel, json.dumps(payload))

    async def subscribe(self) -> AsyncGenerator[ProgressEvent, None]:
        """Subscribe to progress updates for this task.

        Yields:
            Dict containing progress update events with fields:
            - type: "progress"
            - key: task key
            - current: current progress value
            - total: total/target value (or None)
            - message: status message (or None)
            - updated_at: ISO 8601 timestamp
        """
        channel = self.docket.key(f"progress:{self.key}")
        async with self.docket._pubsub() as pubsub:
            await pubsub.subscribe(channel)
            async for message in pubsub.listen():  # pragma: no cover
                if message["type"] == "message":
                    yield json.loads(message["data"])


__all__ = [
    "ExecutionProgress",
    "ProgressEvent",
    "StateEvent",
]
