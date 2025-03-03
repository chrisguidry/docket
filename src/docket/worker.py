import asyncio
import inspect
import logging
import sys
from datetime import datetime, timedelta, timezone
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    Self,
    Sequence,
    TypeVar,
    cast,
)
from uuid import uuid4

import redis.exceptions
from opentelemetry import propagate, trace
from opentelemetry.trace import Tracer
from redis import RedisError

from .docket import Docket, Execution
from .instrumentation import (
    TASK_DURATION,
    TASK_PUNCTUALITY,
    TASKS_COMPLETED,
    TASKS_FAILED,
    TASKS_RETRIED,
    TASKS_RUNNING,
    TASKS_STARTED,
    TASKS_SUCCEEDED,
    message_getter,
)

logger: logging.Logger = logging.getLogger(__name__)
tracer: Tracer = trace.get_tracer(__name__)


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
    docket: Docket
    name: str

    def __init__(
        self,
        docket: Docket,
        name: str | None = None,
        prefetch_count: int = 10,
        redelivery_timeout: timedelta = timedelta(minutes=5),
        reconnection_delay: timedelta = timedelta(seconds=5),
    ) -> None:
        self.docket = docket
        self.name = name or f"worker:{uuid4()}"
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

    @classmethod
    async def run(
        cls,
        docket_name: str = "docket",
        url: str = "redis://localhost:6379/0",
        name: str | None = None,
        prefetch_count: int = 10,
        redelivery_timeout: timedelta = timedelta(minutes=5),
        reconnection_delay: timedelta = timedelta(seconds=5),
        until_finished: bool = False,
        tasks: list[str] = ["docket.tasks:standard_tasks"],
    ) -> None:
        async with Docket(name=docket_name, url=url) as docket:
            for task_path in tasks:
                docket.register_collection(task_path)

            async with Worker(
                docket=docket,
                name=name,
                prefetch_count=prefetch_count,
                redelivery_timeout=redelivery_timeout,
                reconnection_delay=reconnection_delay,
            ) as worker:
                if until_finished:
                    await worker.run_until_finished()
                else:
                    await worker.run_forever()  # pragma: no cover

    async def run_until_finished(self) -> None:
        """Run the worker until there are no more tasks to process."""
        return await self._run(forever=False)

    async def run_forever(self) -> None:
        """Run the worker indefinitely."""
        return await self._run(forever=True)  # pragma: no cover

    async def _run(self, forever: bool = False) -> None:
        logger.info("Starting worker %r with the following tasks:", self.name)
        for task_name, task in self.docket.tasks.items():
            signature = inspect.signature(task)
            logger.info("* %s%s", task_name, signature)

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
                if due_work > 0:
                    logger.debug(
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
        name = execution.function.__name__
        key = execution.key

        log_context: dict[str, str | float] = {
            **self._log_context,
            "task": name,
            "key": key,
        }
        counter_labels = {
            "docket": self.docket.name,
            "worker": self.name,
            "task": name,
        }

        dependencies = self._get_dependencies(execution)

        context = propagate.extract(message, getter=message_getter)
        initiating_context = trace.get_current_span(context).get_span_context()
        links = [trace.Link(initiating_context)] if initiating_context.is_valid else []

        start = datetime.now(timezone.utc)
        punctuality = start - execution.when
        log_context["punctuality"] = punctuality.total_seconds()
        duration = timedelta(0)

        TASKS_STARTED.add(1, counter_labels)
        TASKS_RUNNING.add(1, counter_labels)
        TASK_PUNCTUALITY.record(punctuality.total_seconds(), counter_labels)

        arrow = "↬" if execution.attempt > 1 else "↪"
        call = execution.call_repr()
        logger.info("%s [%s] %s", arrow, punctuality, call, extra=log_context)

        try:
            with tracer.start_as_current_span(
                execution.function.__name__,
                kind=trace.SpanKind.CONSUMER,
                attributes={
                    "docket.name": self.docket.name,
                    "docket.execution.when": execution.when.isoformat(),
                    "docket.execution.key": execution.key,
                    "docket.execution.attempt": execution.attempt,
                    "docket.execution.punctuality": punctuality.total_seconds(),
                    "code.function.name": execution.function.__name__,
                },
                links=links,
            ):
                await execution.function(
                    *execution.args,
                    **{
                        **execution.kwargs,
                        **dependencies,
                    },
                )

            TASKS_SUCCEEDED.add(1, counter_labels)
            duration = datetime.now(timezone.utc) - start
            log_context["duration"] = duration.total_seconds()
            logger.info("%s [%s] %s", "↩", duration, call, extra=log_context)
        except Exception:
            TASKS_FAILED.add(1, counter_labels)
            duration = datetime.now(timezone.utc) - start
            log_context["duration"] = duration.total_seconds()
            retried = await self._retry_if_requested(execution, dependencies)
            arrow = "↫" if retried else "↩"
            logger.exception("%s [%s] %s", arrow, duration, call, extra=log_context)
        finally:
            TASKS_RUNNING.add(-1, counter_labels)
            TASKS_COMPLETED.add(1, counter_labels)
            TASK_DURATION.record(duration.total_seconds(), counter_labels)

    def _get_dependencies(
        self,
        execution: Execution,
    ) -> dict[str, Any]:
        from .dependencies import get_dependency_parameters

        parameters = get_dependency_parameters(execution.function)

        dependencies: dict[str, Any] = {}

        for parameter_name, dependency in parameters.items():
            # If the argument is already provided, skip it, which allows users to call
            # the function directly with the arguments they want.
            if parameter_name in execution.kwargs:
                dependencies[parameter_name] = execution.kwargs[parameter_name]
                continue

            dependencies[parameter_name] = dependency(self.docket, self, execution)

        return dependencies

    async def _retry_if_requested(
        self,
        execution: Execution,
        dependencies: dict[str, Any],
    ) -> bool:
        from .dependencies import Retry

        retries = [retry for retry in dependencies.values() if isinstance(retry, Retry)]
        if not retries:
            return False

        retry = retries[0]

        if retry.attempts is None or execution.attempt < retry.attempts:
            execution.when = datetime.now(timezone.utc) + retry.delay
            execution.attempt += 1
            await self.docket.schedule(execution)

            counter_labels = {
                "docket": self.docket.name,
                "worker": self.name,
                "task": execution.function.__name__,
            }
            TASKS_RETRIED.add(1, counter_labels)
            return True

        return False
