"""Snapshot and worker tracking mixin for Docket."""

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Collection, Sequence, cast

import redis.exceptions
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster

from .execution import Execution, ExecutionState

if TYPE_CHECKING:
    from .docket import RedisMessage, RedisMessageID, RedisStreamPendingMessage


@dataclass
class WorkerInfo:
    name: str
    last_seen: datetime
    tasks: set[str]


class RunningExecution(Execution):
    worker: str
    started: datetime

    def __init__(
        self,
        execution: Execution,
        worker: str,
        started: datetime,
    ) -> None:
        super().__init__(
            docket=execution.docket,
            function=execution.function,
            args=execution.args,
            kwargs=execution.kwargs,
            key=execution.key,
            when=execution.when,
            attempt=execution.attempt,
            trace_context=execution.trace_context,
            redelivered=execution.redelivered,
        )
        self.state: ExecutionState = execution.state
        self.started_at: datetime | None = execution.started_at
        self.completed_at: datetime | None = execution.completed_at
        self.error: str | None = execution.error
        self.result_key: str | None = execution.result_key
        self.worker = worker
        self.started = started


@dataclass
class DocketSnapshot:
    taken: datetime
    total_tasks: int
    future: Sequence[Execution]
    running: Sequence[RunningExecution]
    workers: Collection[WorkerInfo]


class DocketSnapshotMixin:
    """Mixin providing snapshot and worker tracking functionality for Docket.

    This mixin extracts the observability-related methods from Docket:
    - snapshot(): Get current state of all tasks
    - workers(): List all active workers
    - task_workers(): List workers capable of a specific task

    Note: This mixin expects to be used with a Docket class that provides:
    - name, url, heartbeat_interval, missed_heartbeats, worker_group_name
    - stream_key, queue_key properties
    - key(), parked_task_key(), redis(), _ensure_stream_and_group() methods
    """

    # Type declarations for attributes provided by Docket (stubs for type checking)
    if TYPE_CHECKING:
        url: str
        heartbeat_interval: "timedelta"
        missed_heartbeats: int

        @property
        def stream_key(self) -> str: ...
        @property
        def queue_key(self) -> str: ...
        @property
        def worker_group_name(self) -> str: ...

        def key(self, suffix: str) -> str: ...
        def parked_task_key(self, task_key: str) -> str: ...
        def redis(self) -> AbstractAsyncContextManager[Redis | RedisCluster]: ...
        async def _ensure_stream_and_group(self) -> None: ...

    @property
    def workers_set(self) -> str:
        return self.key("workers")

    def worker_tasks_set(self, worker_name: str) -> str:
        return self.key(f"worker-tasks:{worker_name}")

    def task_workers_set(self, task_name: str) -> str:
        return self.key(f"task-workers:{task_name}")

    async def snapshot(self) -> DocketSnapshot:
        """Get a snapshot of the Docket, including which tasks are scheduled or currently
        running, as well as which workers are active.

        Returns:
            A snapshot of the Docket.
        """
        # For memory:// URLs (fakeredis), ensure the group exists upfront. This
        # avoids a fakeredis bug where xpending_range raises TypeError instead
        # of NOGROUP when the consumer group doesn't exist.
        if self.url.startswith("memory://"):
            await self._ensure_stream_and_group()

        running: list[RunningExecution] = []
        future: list[Execution] = []

        async with self.redis() as r:
            async with r.pipeline() as pipeline:
                pipeline.xlen(self.stream_key)

                pipeline.zcard(self.queue_key)

                pipeline.xpending_range(
                    self.stream_key,
                    self.worker_group_name,
                    min="-",
                    max="+",
                    count=1000,
                )

                pipeline.xrange(self.stream_key, "-", "+", count=1000)

                pipeline.zrange(self.queue_key, 0, -1)

                total_stream_messages: int
                total_schedule_messages: int
                pending_messages: list[RedisStreamPendingMessage]
                stream_messages: list[tuple[RedisMessageID, RedisMessage]]
                scheduled_task_keys: list[bytes]

                now = datetime.now(timezone.utc)
                try:
                    (
                        total_stream_messages,
                        total_schedule_messages,
                        pending_messages,
                        stream_messages,
                        scheduled_task_keys,
                    ) = await pipeline.execute()
                except redis.exceptions.ResponseError as e:
                    # Check for NOGROUP error. Also check for XPENDING because
                    # redis-py 7.0 has a bug where pipeline errors lose the
                    # original NOGROUP message (shows "{exception.args}" instead).
                    error_str = str(e)
                    if "NOGROUP" in error_str or "XPENDING" in error_str:
                        await self._ensure_stream_and_group()
                        return await self.snapshot()
                    raise  # pragma: no cover

                for task_key in scheduled_task_keys:
                    pipeline.hgetall(self.parked_task_key(task_key.decode()))

                # Because these are two separate pipeline commands, it's possible that
                # a message has been moved from the schedule to the stream in the
                # meantime, which would end up being an empty `{}` message
                queued_messages: list[RedisMessage] = [
                    m for m in await pipeline.execute() if m
                ]

        total_tasks = total_stream_messages + total_schedule_messages

        pending_lookup: dict[RedisMessageID, RedisStreamPendingMessage] = {
            pending["message_id"]: pending for pending in pending_messages
        }

        # Import here to avoid circular import
        from .docket import Docket

        docket = cast(Docket, self)

        for message_id, message in stream_messages:
            execution = await Execution.from_message(docket, message)
            if message_id in pending_lookup:
                worker_name = pending_lookup[message_id]["consumer"].decode()
                started = now - timedelta(
                    milliseconds=pending_lookup[message_id]["time_since_delivered"]
                )
                running.append(RunningExecution(execution, worker_name, started))
            else:
                future.append(execution)  # pragma: no cover

        for message in queued_messages:
            execution = await Execution.from_message(docket, message)
            future.append(execution)

        workers = await self.workers()

        return DocketSnapshot(now, total_tasks, future, running, workers)

    async def workers(self) -> Collection[WorkerInfo]:
        """Get a list of all workers that have sent heartbeats to the Docket.

        Returns:
            A list of all workers that have sent heartbeats to the Docket.
        """
        workers: list[WorkerInfo] = []

        oldest = datetime.now(timezone.utc).timestamp() - (
            self.heartbeat_interval.total_seconds() * self.missed_heartbeats
        )

        async with self.redis() as r:
            await r.zremrangebyscore(self.workers_set, 0, oldest)

            worker_name_bytes: bytes
            last_seen_timestamp: float

            for worker_name_bytes, last_seen_timestamp in await r.zrange(
                self.workers_set, 0, -1, withscores=True
            ):
                worker_name = worker_name_bytes.decode()
                last_seen = datetime.fromtimestamp(last_seen_timestamp, timezone.utc)

                task_names: set[str] = {
                    task_name_bytes.decode()
                    for task_name_bytes in cast(
                        set[bytes], await r.smembers(self.worker_tasks_set(worker_name))
                    )
                }

                workers.append(WorkerInfo(worker_name, last_seen, task_names))

        return workers

    async def task_workers(self, task_name: str) -> Collection[WorkerInfo]:
        """Get a list of all workers that are able to execute a given task.

        Args:
            task_name: The name of the task.

        Returns:
            A list of all workers that are able to execute the given task.
        """
        workers: list[WorkerInfo] = []
        oldest = datetime.now(timezone.utc).timestamp() - (
            self.heartbeat_interval.total_seconds() * self.missed_heartbeats
        )

        async with self.redis() as r:
            await r.zremrangebyscore(self.task_workers_set(task_name), 0, oldest)

            worker_name_bytes: bytes
            last_seen_timestamp: float

            for worker_name_bytes, last_seen_timestamp in await r.zrange(
                self.task_workers_set(task_name), 0, -1, withscores=True
            ):
                worker_name = worker_name_bytes.decode()
                last_seen = datetime.fromtimestamp(last_seen_timestamp, timezone.utc)

                task_names: set[str] = {
                    task_name_bytes.decode()
                    for task_name_bytes in cast(
                        set[bytes], await r.smembers(self.worker_tasks_set(worker_name))
                    )
                }

                workers.append(WorkerInfo(worker_name, last_seen, task_names))

        return workers


__all__ = [
    "DocketSnapshot",
    "DocketSnapshotMixin",
    "RunningExecution",
    "WorkerInfo",
]
