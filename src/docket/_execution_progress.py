"""Progress tracking for task executions."""

import asyncio
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Generator,
    Literal,
    TypedDict,
)

from ._lua import Arg, Args, Key, redis_script
from ._redis import RedisClient

from ._telemetry import suppress_instrumentation
from typing_extensions import Self

if TYPE_CHECKING:
    from .docket import Docket


@redis_script
async def _progress_write(
    redis: RedisClient,
    *,
    progress_key: Key[str],
    payload: Arg[str],
    clear_message: Arg[bool],
    fields: Args[dict[str, str]],
) -> bytes:
    """
    local hset_args = {}
    for i = fields_start, #ARGV, 2 do
        hset_args[#hset_args + 1] = ARGV[i]
        hset_args[#hset_args + 1] = ARGV[i + 1]
    end
    if #hset_args > 0 then
        redis.call('HSET', progress_key, unpack(hset_args))
    end
    if clear_message then
        redis.call('HDEL', progress_key, 'message')
    end
    redis.call('PUBLISH', progress_key, payload)
    return 'OK'
    """
    ...


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
        payload: ProgressEvent = {
            "type": "progress",
            "key": self.key,
            "current": self.current if self.current is not None else 0,
            "total": total,
            "message": self.message,
            "updated_at": updated_at,
        }
        async with self.docket.redis() as redis:
            await _progress_write(
                redis,
                progress_key=self._redis_key,
                payload=json.dumps(payload),
                clear_message=False,
                fields={"total": str(total), "updated_at": updated_at},
            )
        self.total = total
        self.updated_at = updated_at_dt

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
            async with redis.pipeline() as pipe:
                pipe.hincrby(self._redis_key, "current", amount)
                pipe.hset(self._redis_key, "updated_at", updated_at)
                new_current, _ = await pipe.execute()
        # Update instance attributes using Redis return value
        self.current = new_current
        self.updated_at = updated_at_dt
        # Publish update event with new current value
        await self._publish({"current": new_current, "updated_at": updated_at})

    async def set_message(self, message: str | None) -> None:
        """Update the progress status message.

        Args:
            message: Status message describing current progress, or
                ``None`` to clear any previously-set message.
        """
        updated_at_dt = datetime.now(timezone.utc)
        updated_at = updated_at_dt.isoformat()
        payload: ProgressEvent = {
            "type": "progress",
            "key": self.key,
            "current": self.current if self.current is not None else 0,
            "total": self.total,
            "message": message,
            "updated_at": updated_at,
        }
        fields: dict[str, str] = {"updated_at": updated_at}
        if message is not None:
            fields["message"] = message
        async with self.docket.redis() as redis:
            await _progress_write(
                redis,
                progress_key=self._redis_key,
                payload=json.dumps(payload),
                clear_message=message is None,
                fields=fields,
            )
        self.message = message
        self.updated_at = updated_at_dt

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

    async def subscribe(
        self, *, ready: asyncio.Event | None = None
    ) -> AsyncGenerator[ProgressEvent, None]:
        """Subscribe to progress updates for this task.

        Args:
            ready: Optional ``asyncio.Event`` that is ``set()`` once the
                Redis ``SUBSCRIBE`` has been acknowledged.  Lets callers
                deterministically wait until the subscription is live
                before publishing -- avoids the race where early events
                are dropped because the subscriber hadn't connected yet.

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
            if ready is not None:
                ready.set()
            async for message in pubsub.listen():  # pragma: no cover
                if message["type"] == "message":
                    yield json.loads(message["data"])


__all__ = [
    "ExecutionProgress",
    "ProgressEvent",
    "StateEvent",
]
