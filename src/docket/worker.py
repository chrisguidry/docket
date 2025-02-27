import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol, Self, Sequence, TypeVar, cast
from uuid import uuid4

import redis.exceptions
from redis import RedisError

from .docket import Docket, Execution

logger: logging.Logger = logging.getLogger(__name__)

RedisStreamID = bytes
RedisMessageID = bytes
RedisMessage = dict[bytes, bytes]
RedisStream = tuple[RedisStreamID, Sequence[tuple[RedisMessageID, RedisMessage]]]
RedisReadGroupResponse = Sequence[RedisStream]

if TYPE_CHECKING:  # pragma: no cover
    from .dependencies import Dependency

D = TypeVar("D", bound="Dependency")


class _stream_due_tasks(Protocol):
    async def __call__(
        self, keys: list[str], args: list[str | float]
    ) -> tuple[int, int]: ...  # pragma: no cover


class Worker:
    name: str
    docket: Docket

    def __init__(
        self,
        docket: Docket,
        prefetch_count: int = 10,
        redelivery_timeout: timedelta = timedelta(minutes=5),
        reconnection_delay: timedelta = timedelta(seconds=5),
    ) -> None:
        self.name = f"worker:{uuid4()}"
        self.docket = docket
        self.prefetch_count = prefetch_count
        self.redelivery_timeout = redelivery_timeout
        self.reconnection_delay = reconnection_delay

    async def __aenter__(self) -> Self:
        async with self.docket.redis() as redis:
            try:
                await redis.xgroup_create(
                    groupname=self.consumer_group_name,
                    name=self.docket.stream_key,
                    id="0-0",
                    mkstream=True,
                )
            except RedisError as e:
                if "BUSYGROUP" not in repr(e):
                    raise

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass

    @property
    def consumer_group_name(self) -> str:
        return "docket"

    @property
    def _log_context(self) -> dict[str, str]:
        return {
            "queue_key": self.docket.queue_key,
            "stream_key": self.docket.stream_key,
        }

    async def run_until_current(self) -> None:
        """Run the worker until there are no more tasks to process."""
        return await self._run(forever=False)

    async def run_forever(self) -> None:
        """Run the worker indefinitely."""
        return await self._run(forever=True)  # pragma: no cover

    async def _run(self, forever: bool = False) -> None:
        while True:
            try:
                return await self._worker_loop(forever=forever)
            except redis.exceptions.ConnectionError:
                logger.warning(
                    "Error connecting to redis, retrying in %s...",
                    self.reconnection_delay,
                    exc_info=True,
                )
                await asyncio.sleep(self.reconnection_delay.total_seconds())

    async def _worker_loop(self, forever: bool = False):
        async with self.docket.redis() as redis:
            stream_due_tasks: _stream_due_tasks = cast(
                _stream_due_tasks,
                redis.register_script(
                    # Lua script to atomically move scheduled tasks to the stream
                    # KEYS[1]: queue key (sorted set)
                    # KEYS[2]: stream key
                    # ARGV[1]: current timestamp
                    # ARGV[2]: docket name prefix
                    """
                local total_work = redis.call('ZCARD', KEYS[1])
                local due_work = 0
                local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])

                for i, key in ipairs(tasks) do
                    local hash_key = ARGV[2] .. ":" .. key
                    local task_data = redis.call('HGETALL', hash_key)

                    if #task_data > 0 then
                        local task = {}
                        for j = 1, #task_data, 2 do
                            task[task_data[j]] = task_data[j+1]
                        end

                        redis.call('XADD', KEYS[2], '*',
                            'key', task['key'],
                            'when', task['when'],
                            'function', task['function'],
                            'args', task['args'],
                            'kwargs', task['kwargs'],
                            'attempt', task['attempt']
                        )
                        redis.call('DEL', hash_key)
                        due_work = due_work + 1
                    end
                end

                if due_work > 0 then
                    redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
                end

                return {total_work, due_work}
                """
                ),
            )

            total_work, due_work = sys.maxsize, 0
            while forever or total_work:
                now = datetime.now(timezone.utc)
                total_work, due_work = await stream_due_tasks(
                    keys=[self.docket.queue_key, self.docket.stream_key],
                    args=[now.timestamp(), self.docket.name],
                )
                logger.info(
                    "Moved %d/%d due tasks from %s to %s",
                    due_work,
                    total_work,
                    self.docket.queue_key,
                    self.docket.stream_key,
                    extra=self._log_context,
                )

                _, redeliveries, _ = await redis.xautoclaim(
                    name=self.docket.stream_key,
                    groupname=self.consumer_group_name,
                    consumername=self.name,
                    min_idle_time=int(self.redelivery_timeout.total_seconds() * 1000),
                    start_id="0-0",
                    count=self.prefetch_count,
                )

                new_deliveries: RedisReadGroupResponse = await redis.xreadgroup(
                    groupname=self.consumer_group_name,
                    consumername=self.name,
                    streams={self.docket.stream_key: ">"},
                    count=self.prefetch_count,
                    block=10,
                )

                for source in [[(b"redeliveries", redeliveries)], new_deliveries]:
                    for _, messages in source:
                        for message_id, message in messages:
                            await self._execute(message)

                            async with redis.pipeline() as pipeline:
                                pipeline.xack(
                                    self.docket.stream_key,
                                    self.consumer_group_name,
                                    message_id,
                                )
                                pipeline.xdel(
                                    self.docket.stream_key,
                                    message_id,
                                )
                                await pipeline.execute()

                            # When executing a task, there's always a chance that it was
                            # either retried or it scheduled another task, so let's give
                            # ourselves one more iteration of the loop to handle that.
                            total_work += 1

    async def _execute(self, message: RedisMessage) -> None:
        execution = Execution.from_message(
            self.docket.tasks[message[b"function"].decode()],
            message,
        )

        logger.info(
            "Executing task %s with args %s and kwargs %s",
            execution.key,
            execution.args,
            execution.kwargs,
            extra={
                **self._log_context,
                "function": execution.function.__name__,
            },
        )

        dependencies = self._get_dependencies(execution)

        try:
            await execution.function(
                *execution.args,
                **{
                    **execution.kwargs,
                    **dependencies,
                },
            )
        except Exception:
            logger.exception(
                "Error executing task %s",
                execution.key,
                extra=self._log_context,
            )
            await self._retry_if_requested(execution, dependencies)

    def _get_dependencies(
        self,
        execution: Execution,
    ) -> dict[str, Any]:
        from .dependencies import get_dependency_parameters

        parameters = get_dependency_parameters(execution.function)

        dependencies: dict[str, Any] = {}

        for param_name, dependency in parameters.items():
            # If the argument is already provided, skip it, which allows users to call
            # the function directly with the arguments they want.
            if param_name in execution.kwargs:
                dependencies[param_name] = execution.kwargs[param_name]
                continue

            dependencies[param_name] = dependency(self.docket, self, execution)

        return dependencies

    async def _retry_if_requested(
        self,
        execution: Execution,
        dependencies: dict[str, Any],
    ) -> None:
        from .dependencies import Retry

        retries = [retry for retry in dependencies.values() if isinstance(retry, Retry)]
        if not retries:
            return

        retry = retries[0]

        if execution.attempt < retry.attempts:
            execution.when = datetime.now(timezone.utc) + retry.delay
            execution.attempt += 1
            await self.docket.schedule(execution)
        else:
            logger.error(
                "Task %s failed after %d attempts",
                execution.key,
                retry.attempts,
            )
