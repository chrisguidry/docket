import asyncio
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from types import TracebackType
from typing import (
    Coroutine,
    Mapping,
    Protocol,
    Self,
    cast,
)
from uuid import uuid4

from opentelemetry import trace
from opentelemetry.trace import Tracer
from redis.asyncio import Redis
from redis.exceptions import ConnectionError, LockError

from docket.execution import get_signature

from .dependencies import (
    Dependency,
    FailedDependency,
    Perpetual,
    Retry,
    Timeout,
    get_single_dependency_of_type,
    get_single_dependency_parameter_of_type,
    resolved_dependencies,
)
from .docket import (
    Docket,
    Execution,
    RedisMessage,
    RedisMessageID,
    RedisReadGroupResponse,
)
from .instrumentation import (
    QUEUE_DEPTH,
    REDIS_DISRUPTIONS,
    SCHEDULE_DEPTH,
    TASK_DURATION,
    TASK_PUNCTUALITY,
    TASKS_COMPLETED,
    TASKS_FAILED,
    TASKS_PERPETUATED,
    TASKS_RETRIED,
    TASKS_RUNNING,
    TASKS_STARTED,
    TASKS_STRICKEN,
    TASKS_SUCCEEDED,
    metrics_server,
)

logger: logging.Logger = logging.getLogger(__name__)
tracer: Tracer = trace.get_tracer(__name__)


class _stream_due_tasks(Protocol):
    async def __call__(
        self, keys: list[str], args: list[str | float]
    ) -> tuple[int, int]: ...  # pragma: no cover


class Worker:
    docket: Docket
    name: str
    concurrency: int
    redelivery_timeout: timedelta
    reconnection_delay: timedelta
    minimum_check_interval: timedelta
    scheduling_resolution: timedelta
    schedule_automatic_tasks: bool

    def __init__(
        self,
        docket: Docket,
        name: str | None = None,
        concurrency: int = 10,
        redelivery_timeout: timedelta = timedelta(minutes=5),
        reconnection_delay: timedelta = timedelta(seconds=5),
        minimum_check_interval: timedelta = timedelta(milliseconds=250),
        scheduling_resolution: timedelta = timedelta(milliseconds=250),
        schedule_automatic_tasks: bool = True,
    ) -> None:
        self.docket = docket
        self.name = name or f"worker:{uuid4()}"
        self.concurrency = concurrency
        self.redelivery_timeout = redelivery_timeout
        self.reconnection_delay = reconnection_delay
        self.minimum_check_interval = minimum_check_interval
        self.scheduling_resolution = scheduling_resolution
        self.schedule_automatic_tasks = schedule_automatic_tasks

    async def __aenter__(self) -> Self:
        self._heartbeat_task = asyncio.create_task(self._heartbeat())
        self._execution_counts = {}
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del self._execution_counts

        self._heartbeat_task.cancel()
        try:
            await self._heartbeat_task
        except asyncio.CancelledError:
            pass
        del self._heartbeat_task

    def labels(self) -> Mapping[str, str]:
        return {
            **self.docket.labels(),
            "docket.worker": self.name,
        }

    def _log_context(self) -> Mapping[str, str]:
        return {
            **self.labels(),
            "docket.queue_key": self.docket.queue_key,
            "docket.stream_key": self.docket.stream_key,
        }

    @classmethod
    async def run(
        cls,
        docket_name: str = "docket",
        url: str = "redis://localhost:6379/0",
        name: str | None = None,
        concurrency: int = 10,
        redelivery_timeout: timedelta = timedelta(minutes=5),
        reconnection_delay: timedelta = timedelta(seconds=5),
        minimum_check_interval: timedelta = timedelta(milliseconds=100),
        scheduling_resolution: timedelta = timedelta(milliseconds=250),
        schedule_automatic_tasks: bool = True,
        until_finished: bool = False,
        metrics_port: int | None = None,
        tasks: list[str] = ["docket.tasks:standard_tasks"],
    ) -> None:
        with metrics_server(port=metrics_port):
            async with Docket(name=docket_name, url=url) as docket:
                for task_path in tasks:
                    docket.register_collection(task_path)

                async with Worker(
                    docket=docket,
                    name=name,
                    concurrency=concurrency,
                    redelivery_timeout=redelivery_timeout,
                    reconnection_delay=reconnection_delay,
                    minimum_check_interval=minimum_check_interval,
                    scheduling_resolution=scheduling_resolution,
                    schedule_automatic_tasks=schedule_automatic_tasks,
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

    _execution_counts: dict[str, int]

    async def run_at_most(self, iterations_by_key: Mapping[str, int]) -> None:
        """
        Run the worker until there are no more tasks to process, but limit specified
        task keys to a maximum number of iterations.

        This is particularly useful for testing self-perpetuating tasks that would
        otherwise run indefinitely.

        Args:
            iterations_by_key: Maps task keys to their maximum allowed executions
        """
        self._execution_counts = {key: 0 for key in iterations_by_key}

        def has_reached_max_iterations(execution: Execution) -> bool:
            key = execution.key

            if key not in iterations_by_key:
                return False

            if self._execution_counts[key] >= iterations_by_key[key]:
                return True

            return False

        self.docket.strike_list.add_condition(has_reached_max_iterations)
        try:
            await self.run_until_finished()
        finally:
            self.docket.strike_list.remove_condition(has_reached_max_iterations)
            self._execution_counts = {}

    async def _run(self, forever: bool = False) -> None:
        logger.info("Starting worker %r with the following tasks:", self.name)
        for task_name, task in self.docket.tasks.items():
            signature = get_signature(task)
            logger.info("* %s%s", task_name, signature)

        while True:
            try:
                async with self.docket.redis() as redis:
                    return await self._worker_loop(redis, forever=forever)
            except ConnectionError:
                REDIS_DISRUPTIONS.add(1, self.labels())
                logger.warning(
                    "Error connecting to redis, retrying in %s...",
                    self.reconnection_delay,
                    exc_info=True,
                )
                await asyncio.sleep(self.reconnection_delay.total_seconds())

    async def _worker_loop(self, redis: Redis, forever: bool = False):
        worker_stopping = asyncio.Event()

        if self.schedule_automatic_tasks:
            await self._schedule_all_automatic_perpetual_tasks()

        scheduler_task = asyncio.create_task(
            self._scheduler_loop(redis, worker_stopping)
        )

        active_tasks: dict[asyncio.Task[None], RedisMessageID] = {}
        available_slots = self.concurrency

        log_context = self._log_context()

        async def check_for_work() -> bool:
            logger.debug("Checking for work", extra=log_context)
            async with redis.pipeline() as pipeline:
                pipeline.xlen(self.docket.stream_key)
                pipeline.zcard(self.docket.queue_key)
                results: list[int] = await pipeline.execute()
                stream_len = results[0]
                queue_len = results[1]
                return stream_len > 0 or queue_len > 0

        async def get_redeliveries(redis: Redis) -> RedisReadGroupResponse:
            logger.debug("Getting redeliveries", extra=log_context)
            _, redeliveries, *_ = await redis.xautoclaim(
                name=self.docket.stream_key,
                groupname=self.docket.worker_group_name,
                consumername=self.name,
                min_idle_time=int(self.redelivery_timeout.total_seconds() * 1000),
                start_id="0-0",
                count=available_slots,
            )
            return [(b"__redelivery__", redeliveries)]

        async def get_new_deliveries(redis: Redis) -> RedisReadGroupResponse:
            logger.debug("Getting new deliveries", extra=log_context)
            return await redis.xreadgroup(
                groupname=self.docket.worker_group_name,
                consumername=self.name,
                streams={self.docket.stream_key: ">"},
                block=int(self.minimum_check_interval.total_seconds() * 1000),
                count=available_slots,
            )

        def start_task(message_id: RedisMessageID, message: RedisMessage) -> bool:
            function_name = message[b"function"].decode()
            if not (function := self.docket.tasks.get(function_name)):
                logger.warning(
                    "Task function %r not found",
                    function_name,
                    extra=log_context,
                )
                return False

            execution = Execution.from_message(function, message)

            task = asyncio.create_task(self._execute(execution), name=execution.key)
            active_tasks[task] = message_id

            nonlocal available_slots
            available_slots -= 1

            return True

        async def process_completed_tasks() -> None:
            completed_tasks = {task for task in active_tasks if task.done()}
            for task in completed_tasks:
                message_id = active_tasks.pop(task)
                await task
                await ack_message(redis, message_id)

        async def ack_message(redis: Redis, message_id: RedisMessageID) -> None:
            logger.debug("Acknowledging message", extra=log_context)
            async with redis.pipeline() as pipeline:
                pipeline.xack(
                    self.docket.stream_key,
                    self.docket.worker_group_name,
                    message_id,
                )
                pipeline.xdel(
                    self.docket.stream_key,
                    message_id,
                )
                await pipeline.execute()

        has_work: bool = True

        try:
            while forever or has_work or active_tasks:
                await process_completed_tasks()

                available_slots = self.concurrency - len(active_tasks)

                if available_slots <= 0:
                    await asyncio.sleep(self.minimum_check_interval.total_seconds())
                    continue

                for source in [get_redeliveries, get_new_deliveries]:
                    for _, messages in await source(redis):
                        for message_id, message in messages:
                            if not message:  # pragma: no cover
                                continue

                            if not start_task(message_id, message):
                                await self._delete_known_task(redis, message)
                                await ack_message(redis, message_id)

                    if available_slots <= 0:
                        break

                if not forever and not active_tasks:
                    has_work = await check_for_work()

        except asyncio.CancelledError:
            if active_tasks:  # pragma: no cover
                logger.info(
                    "Shutdown requested, finishing %d active tasks...",
                    len(active_tasks),
                    extra=log_context,
                )
        finally:
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)
                await process_completed_tasks()

            worker_stopping.set()
            await scheduler_task

    async def _scheduler_loop(
        self,
        redis: Redis,
        worker_stopping: asyncio.Event,
    ) -> None:
        """Loop that moves due tasks from the queue to the stream."""

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

            if total_work > 0 then
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
            end

            if due_work > 0 then
                redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
            end

            return {total_work, due_work}
            """
            ),
        )

        total_work: int = sys.maxsize

        log_context = self._log_context()

        while not worker_stopping.is_set() or total_work:
            try:
                logger.debug("Scheduling due tasks", extra=log_context)
                total_work, due_work = await stream_due_tasks(
                    keys=[self.docket.queue_key, self.docket.stream_key],
                    args=[datetime.now(timezone.utc).timestamp(), self.docket.name],
                )

                if due_work > 0:
                    logger.debug(
                        "Moved %d/%d due tasks from %s to %s",
                        due_work,
                        total_work,
                        self.docket.queue_key,
                        self.docket.stream_key,
                        extra=log_context,
                    )
            except Exception:  # pragma: no cover
                logger.exception(
                    "Error in scheduler loop",
                    exc_info=True,
                    extra=log_context,
                )
            finally:
                await asyncio.sleep(self.scheduling_resolution.total_seconds())

        logger.debug("Scheduler loop finished", extra=log_context)

    async def _schedule_all_automatic_perpetual_tasks(self) -> None:
        async with self.docket.redis() as redis:
            try:
                async with redis.lock(
                    f"{self.docket.name}:perpetual:lock", timeout=10, blocking=False
                ):
                    for task_function in self.docket.tasks.values():
                        perpetual = get_single_dependency_parameter_of_type(
                            task_function, Perpetual
                        )
                        if perpetual is None:
                            continue

                        if not perpetual.automatic:
                            continue

                        key = task_function.__name__

                        await self.docket.add(task_function, key=key)()
            except LockError:  # pragma: no cover
                return

    async def _delete_known_task(
        self, redis: Redis, execution_or_message: Execution | RedisMessage
    ) -> None:
        if isinstance(execution_or_message, Execution):
            key = execution_or_message.key
        elif bytes_key := execution_or_message.get(b"key"):
            key = bytes_key.decode()
        else:  # pragma: no cover
            return

        logger.debug("Deleting known task", extra=self._log_context())
        known_task_key = self.docket.known_task_key(key)
        await redis.delete(known_task_key)

    async def _execute(self, execution: Execution) -> None:
        log_context = {**self._log_context(), **execution.specific_labels()}
        counter_labels = {**self.labels(), **execution.general_labels()}

        call = execution.call_repr()

        if self.docket.strike_list.is_stricken(execution):
            async with self.docket.redis() as redis:
                await self._delete_known_task(redis, execution)

            logger.warning("🗙 %s", call, extra=log_context)
            TASKS_STRICKEN.add(1, counter_labels | {"docket.where": "worker"})
            return

        if execution.key in self._execution_counts:
            self._execution_counts[execution.key] += 1

        start = time.time()
        punctuality = start - execution.when.timestamp()
        log_context = {**log_context, "punctuality": punctuality}
        duration = 0.0

        TASKS_STARTED.add(1, counter_labels)
        TASKS_RUNNING.add(1, counter_labels)
        TASK_PUNCTUALITY.record(punctuality, counter_labels)

        arrow = "↬" if execution.attempt > 1 else "↪"
        logger.info("%s [%s] %s", arrow, ms(punctuality), call, extra=log_context)

        with tracer.start_as_current_span(
            execution.function.__name__,
            kind=trace.SpanKind.CONSUMER,
            attributes={
                **self.labels(),
                **execution.specific_labels(),
                "code.function.name": execution.function.__name__,
            },
            links=execution.incoming_span_links(),
        ):
            async with resolved_dependencies(self, execution) as dependencies:
                # Preemptively reschedule the perpetual task for the future, or clear
                # the known task key for this task
                rescheduled = await self._perpetuate_if_requested(
                    execution, dependencies
                )
                if not rescheduled:
                    async with self.docket.redis() as redis:
                        await self._delete_known_task(redis, execution)

                try:
                    dependency_failures = {
                        k: v
                        for k, v in dependencies.items()
                        if isinstance(v, FailedDependency)
                    }
                    if dependency_failures:
                        raise ExceptionGroup(
                            (
                                "Failed to resolve dependencies for parameter(s): "
                                + ", ".join(dependency_failures.keys())
                            ),
                            [
                                dependency.error
                                for dependency in dependency_failures.values()
                            ],
                        )

                    if timeout := get_single_dependency_of_type(dependencies, Timeout):
                        await self._run_function_with_timeout(
                            execution, dependencies, timeout
                        )
                    else:
                        await execution.function(
                            *execution.args,
                            **{
                                **execution.kwargs,
                                **dependencies,
                            },
                        )

                    duration = log_context["duration"] = time.time() - start
                    TASKS_SUCCEEDED.add(1, counter_labels)

                    rescheduled = await self._perpetuate_if_requested(
                        execution, dependencies, timedelta(seconds=duration)
                    )

                    arrow = "↫" if rescheduled else "↩"
                    logger.info(
                        "%s [%s] %s", arrow, ms(duration), call, extra=log_context
                    )
                except Exception:
                    duration = log_context["duration"] = time.time() - start
                    TASKS_FAILED.add(1, counter_labels)

                    retried = await self._retry_if_requested(execution, dependencies)
                    if not retried:
                        retried = await self._perpetuate_if_requested(
                            execution, dependencies, timedelta(seconds=duration)
                        )

                    arrow = "↫" if retried else "↩"
                    logger.exception(
                        "%s [%s] %s", arrow, ms(duration), call, extra=log_context
                    )
                finally:
                    TASKS_RUNNING.add(-1, counter_labels)
                    TASKS_COMPLETED.add(1, counter_labels)
                    TASK_DURATION.record(duration, counter_labels)

    async def _run_function_with_timeout(
        self,
        execution: Execution,
        dependencies: dict[str, Dependency],
        timeout: Timeout,
    ) -> None:
        task_coro = cast(
            Coroutine[None, None, None],
            execution.function(*execution.args, **execution.kwargs, **dependencies),
        )
        task = asyncio.create_task(task_coro)
        try:
            while not task.done():  # pragma: no branch
                remaining = timeout.remaining().total_seconds()
                if timeout.expired():
                    task.cancel()
                    break

                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=remaining)
                    return
                except asyncio.TimeoutError:
                    continue
        finally:
            if not task.done():
                task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                raise asyncio.TimeoutError

    async def _retry_if_requested(
        self,
        execution: Execution,
        dependencies: dict[str, Dependency],
    ) -> bool:
        retry = get_single_dependency_of_type(dependencies, Retry)
        if not retry:
            return False

        if retry.attempts is not None and execution.attempt >= retry.attempts:
            return False

        execution.when = datetime.now(timezone.utc) + retry.delay
        execution.attempt += 1
        await self.docket.schedule(execution)

        TASKS_RETRIED.add(1, {**self.labels(), **execution.specific_labels()})
        return True

    async def _perpetuate_if_requested(
        self,
        execution: Execution,
        dependencies: dict[str, Dependency],
        duration: timedelta | None = None,
    ) -> bool:
        perpetual = get_single_dependency_of_type(dependencies, Perpetual)
        if not perpetual:
            return False

        if perpetual.cancelled:
            await self.docket.cancel(execution.key)
            return False

        now = datetime.now(timezone.utc)
        when = max(now, now + perpetual.every - (duration or timedelta(0)))

        await self.docket.replace(execution.function, when, execution.key)(
            *perpetual.args,
            **perpetual.kwargs,
        )

        if duration is not None:
            TASKS_PERPETUATED.add(1, {**self.labels(), **execution.specific_labels()})

        return True

    @property
    def workers_set(self) -> str:
        return self.docket.workers_set

    def worker_tasks_set(self, worker_name: str) -> str:
        return self.docket.worker_tasks_set(worker_name)

    def task_workers_set(self, task_name: str) -> str:
        return self.docket.task_workers_set(task_name)

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self.docket.heartbeat_interval.total_seconds())
            try:
                now = datetime.now(timezone.utc).timestamp()
                maximum_age = (
                    self.docket.heartbeat_interval * self.docket.missed_heartbeats
                )
                oldest = now - maximum_age.total_seconds()

                task_names = list(self.docket.tasks)

                async with self.docket.redis() as r:
                    async with r.pipeline() as pipeline:
                        pipeline.zremrangebyscore(self.workers_set, 0, oldest)
                        pipeline.zadd(self.workers_set, {self.name: now})

                        for task_name in task_names:
                            task_workers_set = self.task_workers_set(task_name)
                            pipeline.zremrangebyscore(task_workers_set, 0, oldest)
                            pipeline.zadd(task_workers_set, {self.name: now})

                        pipeline.sadd(self.worker_tasks_set(self.name), *task_names)
                        pipeline.expire(
                            self.worker_tasks_set(self.name),
                            max(maximum_age, timedelta(seconds=1)),
                        )

                        await pipeline.execute()

                    async with r.pipeline() as pipeline:
                        pipeline.xlen(self.docket.stream_key)
                        pipeline.zcount(self.docket.queue_key, 0, now)
                        pipeline.zcount(self.docket.queue_key, now, "+inf")

                        results: list[int] = await pipeline.execute()
                        stream_depth = results[0]
                        overdue_depth = results[1]
                        schedule_depth = results[2]

                        QUEUE_DEPTH.set(
                            stream_depth + overdue_depth, self.docket.labels()
                        )
                        SCHEDULE_DEPTH.set(schedule_depth, self.docket.labels())

            except asyncio.CancelledError:  # pragma: no cover
                return
            except ConnectionError:
                REDIS_DISRUPTIONS.add(1, self.labels())
                logger.exception(
                    "Error sending worker heartbeat",
                    exc_info=True,
                    extra=self._log_context(),
                )
            except Exception:
                logger.exception(
                    "Error sending worker heartbeat",
                    exc_info=True,
                    extra=self._log_context(),
                )


def ms(seconds: float) -> str:
    if seconds < 100:
        return f"{seconds * 1000:6.0f}ms"
    else:
        return f"{seconds:6.0f}s "
