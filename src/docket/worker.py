from __future__ import annotations

import asyncio
import base64
import importlib
import logging
import os
import signal
import socket
import sys
import time
from contextlib import AsyncExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from types import TracebackType
from typing import (
    Any,
    Generator,
    Mapping,
    Protocol,
    TypeAlias,
    TypedDict,
    cast,
)

import cloudpickle

if sys.version_info < (3, 11):
    from exceptiongroup import ExceptionGroup  # pragma: no cover
    from taskgroup import TaskGroup  # pragma: no cover
else:
    from asyncio import TaskGroup  # pragma: no cover

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode, Tracer

from ._cancellation import CANCEL_MSG_CLEANUP, cancel_task
from ._telemetry import suppress_instrumentation
from redis.asyncio import Redis
from redis.exceptions import ConnectionError, LockError, ResponseError
from typing_extensions import Self

from .dependencies import (
    AdmissionBlocked,
    CompletionHandler,
    CurrentExecution,
    Dependency,
    FailedDependency,
    FailureHandler,
    Perpetual,
    Runtime,
    SharedContext,
    TaskLogger,
    TaskOutcome,
    format_duration,
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
from .execution import TaskFunction, compact_signature, get_signature
from .instrumentation import (
    QUEUE_DEPTH,
    REDIS_DISRUPTIONS,
    SCHEDULE_DEPTH,
    TASK_DURATION,
    TASK_PUNCTUALITY,
    TASKS_COMPLETED,
    TASKS_FAILED,
    TASKS_REDELIVERED,
    TASKS_RUNNING,
    TASKS_STARTED,
    TASKS_STRICKEN,
    TASKS_SUCCEEDED,
    healthcheck_server,
    metrics_server,
)

# Delay before retrying a task blocked by admission control (e.g., concurrency limits)
# Must be larger than redelivery_timeout to ensure atomic reschedule+ACK completes
# before Redis would consider redelivering the message
ADMISSION_BLOCKED_RETRY_DELAY = timedelta(milliseconds=100)

# Lock timeout for coordinating automatic perpetual task scheduling at startup.
# If a worker crashes while holding this lock, it expires after this many seconds.
AUTOMATIC_PERPETUAL_LOCK_TIMEOUT_SECONDS = 10

# Minimum TTL in seconds for Redis keys to avoid immediate expiration when
# redelivery_timeout is very small (e.g., in tests with 200ms timeouts).
MINIMUM_TTL_SECONDS = 1

TaskKey: TypeAlias = str


class PubSubMessage(TypedDict):
    """Message received from Redis pub/sub pattern subscription."""

    type: str
    pattern: bytes
    channel: bytes
    data: bytes | str


async def default_fallback_task(
    *args: Any,
    execution: Execution = CurrentExecution(),
    logger: logging.LoggerAdapter[logging.Logger] = TaskLogger(),
    **kwargs: Any,
) -> None:
    """Default fallback that logs a warning and completes the task."""
    logger.warning(
        "Unknown task %r received - dropping. "
        "Register via CLI (--tasks your.module:tasks) or API (docket.register(func)).",
        execution.function_name,
    )


logger: logging.Logger = logging.getLogger(__name__)
tracer: Tracer = trace.get_tracer(__name__)


class _stream_due_tasks(Protocol):
    async def __call__(
        self, keys: list[str], args: list[str | float]
    ) -> tuple[int, int]: ...  # pragma: no cover


class Worker:
    """A Worker executes tasks on a Docket.  You may run as many workers as you like
    to work a single Docket.

    Example:

    ```python
    async with Docket() as docket:
        async with Worker(docket) as worker:
            await worker.run_forever()
    ```
    """

    docket: Docket
    name: str
    concurrency: int
    redelivery_timeout: timedelta
    reconnection_delay: timedelta
    minimum_check_interval: timedelta
    scheduling_resolution: timedelta
    schedule_automatic_tasks: bool
    enable_internal_instrumentation: bool
    fallback_task: TaskFunction

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
        enable_internal_instrumentation: bool = False,
        fallback_task: TaskFunction | None = None,
    ) -> None:
        self.docket = docket
        self.name = name or f"{socket.gethostname()}#{os.getpid()}"
        self.concurrency = concurrency
        self.redelivery_timeout = redelivery_timeout
        self.reconnection_delay = reconnection_delay
        self.minimum_check_interval = minimum_check_interval
        self.scheduling_resolution = scheduling_resolution
        self.schedule_automatic_tasks = schedule_automatic_tasks
        self.enable_internal_instrumentation = enable_internal_instrumentation
        self.fallback_task = fallback_task or default_fallback_task

    @contextmanager
    def _maybe_suppress_instrumentation(self) -> Generator[None, None, None]:
        """Suppress OTel auto-instrumentation for internal Redis operations.

        When enable_internal_instrumentation is False (default), this context manager
        suppresses OpenTelemetry auto-instrumentation spans for internal Redis polling
        operations like XREADGROUP, XAUTOCLAIM, and Lua script evaluations. This prevents
        thousands of noisy spans per minute from overwhelming trace storage.

        Task execution spans and user-facing operations (schedule, cancel, etc.) are
        NOT suppressed.
        """
        if not self.enable_internal_instrumentation:
            with suppress_instrumentation():
                yield
        else:  # pragma: no cover
            yield

    async def __aenter__(self) -> Self:
        self._stack = AsyncExitStack()
        await self._stack.__aenter__()

        # Events for coordinating worker loop shutdown (cleaned up last)
        self._worker_stopping = asyncio.Event()
        self._stack.callback(lambda: delattr(self, "_worker_stopping"))
        self._worker_done = asyncio.Event()
        self._stack.callback(lambda: delattr(self, "_worker_done"))
        self._worker_done.set()  # Initially done (not running)
        self._cancellation_ready = asyncio.Event()
        self._stack.callback(lambda: delattr(self, "_cancellation_ready"))

        self._execution_counts: dict[str, int] = {}
        self._stack.callback(lambda: delattr(self, "_execution_counts"))
        self._tasks_by_key: dict[TaskKey, asyncio.Task[None]] = {}
        self._stack.callback(lambda: delattr(self, "_tasks_by_key"))

        self._heartbeat_task = asyncio.create_task(
            self._heartbeat(), name=f"{self.docket.name} - heartbeat"
        )
        self._stack.callback(lambda: delattr(self, "_heartbeat_task"))
        self._stack.push_async_callback(
            cancel_task, self._heartbeat_task, CANCEL_MSG_CLEANUP
        )

        # Shared context is set up last, so it's cleaned up first (LIFO)
        self._shared_context = SharedContext(self.docket, self)
        self._stack.callback(lambda: delattr(self, "_shared_context"))
        await self._stack.enter_async_context(self._shared_context)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # Signal worker loop to stop and wait for it to drain
        self._worker_stopping.set()
        await self._worker_done.wait()

        # Stack handles LIFO cleanup: shared_context first, then heartbeat
        try:
            await self._stack.__aexit__(exc_type, exc_value, traceback)
        finally:
            del self._stack

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
        enable_internal_instrumentation: bool = False,
        until_finished: bool = False,
        healthcheck_port: int | None = None,
        metrics_port: int | None = None,
        tasks: list[str] = ["docket.tasks:standard_tasks"],
        fallback_task: str | None = None,
    ) -> None:
        """Run a worker as the main entry point (CLI).

        This method installs signal handlers for graceful shutdown since it
        assumes ownership of the event loop. When embedding Docket in another
        framework (e.g., FastAPI with uvicorn), use Worker.run_forever() or
        Worker.run_until_finished() directly - those methods do not install
        signal handlers and rely on the framework to handle shutdown signals.
        """
        # Parse fallback_task string if provided (module:function format)
        resolved_fallback_task: TaskFunction | None = None
        if fallback_task:
            module_name, _, member_name = fallback_task.rpartition(":")
            module = importlib.import_module(module_name)
            resolved_fallback_task = getattr(module, member_name)

        with (
            healthcheck_server(port=healthcheck_port),
            metrics_server(port=metrics_port),
        ):
            async with Docket(
                name=docket_name,
                url=url,
                enable_internal_instrumentation=enable_internal_instrumentation,
            ) as docket:
                for task_path in tasks:
                    docket.register_collection(task_path)

                async with (
                    Worker(  # pragma: no branch - context manager exit varies across interpreters
                        docket=docket,
                        name=name,
                        concurrency=concurrency,
                        redelivery_timeout=redelivery_timeout,
                        reconnection_delay=reconnection_delay,
                        minimum_check_interval=minimum_check_interval,
                        scheduling_resolution=scheduling_resolution,
                        schedule_automatic_tasks=schedule_automatic_tasks,
                        enable_internal_instrumentation=enable_internal_instrumentation,
                        fallback_task=resolved_fallback_task,
                    ) as worker
                ):
                    # Install signal handlers for graceful shutdown.
                    # This is only appropriate when we own the event loop (CLI entry point).
                    # Embedded usage should let the framework handle signals.
                    loop = asyncio.get_running_loop()
                    run_task: asyncio.Task[None] | None = None

                    def handle_shutdown(sig_name: str) -> None:  # pragma: no cover
                        logger.info(
                            "Received %s, initiating graceful shutdown...", sig_name
                        )
                        if run_task and not run_task.done():
                            run_task.cancel()

                    if hasattr(signal, "SIGTERM"):  # pragma: no cover
                        loop.add_signal_handler(
                            signal.SIGTERM, lambda: handle_shutdown("SIGTERM")
                        )
                        loop.add_signal_handler(
                            signal.SIGINT, lambda: handle_shutdown("SIGINT")
                        )

                    try:
                        if until_finished:
                            run_task = asyncio.create_task(
                                worker.run_until_finished(),
                                name=f"{docket_name} - worker",
                            )
                        else:
                            run_task = asyncio.create_task(
                                worker.run_forever(),
                                name=f"{docket_name} - worker",
                            )  # pragma: no cover
                        await run_task
                    except asyncio.CancelledError:  # pragma: no cover
                        pass
                    finally:
                        if hasattr(signal, "SIGTERM"):  # pragma: no cover
                            loop.remove_signal_handler(signal.SIGTERM)
                            loop.remove_signal_handler(signal.SIGINT)

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
        self._startup_log()

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
        self._worker_stopping.clear()
        self._worker_done.clear()
        self._cancellation_ready.clear()  # Reset for reconnection scenarios

        active_tasks: dict[asyncio.Task[None], RedisMessageID] = {}
        task_executions: dict[asyncio.Task[None], Execution] = {}
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
            try:
                with self._maybe_suppress_instrumentation():
                    _, redeliveries, *_ = await redis.xautoclaim(
                        name=self.docket.stream_key,
                        groupname=self.docket.worker_group_name,
                        consumername=self.name,
                        min_idle_time=int(
                            self.redelivery_timeout.total_seconds() * 1000
                        ),
                        start_id="0-0",
                        count=available_slots,
                    )
            except ResponseError as e:
                if "NOGROUP" in str(e):
                    await self.docket._ensure_stream_and_group()
                    return await get_redeliveries(redis)
                raise  # pragma: no cover
            return [(b"__redelivery__", redeliveries)]

        async def get_new_deliveries(redis: Redis) -> RedisReadGroupResponse:
            logger.debug("Getting new deliveries", extra=log_context)
            # Use non-blocking read with in-memory backend + manual sleep
            # This is necessary because fakeredis's async blocking operations don't
            # properly yield control to the asyncio event loop
            is_memory = self.docket.url.startswith("memory://")
            try:
                with self._maybe_suppress_instrumentation():
                    result = await redis.xreadgroup(
                        groupname=self.docket.worker_group_name,
                        consumername=self.name,
                        streams={self.docket.stream_key: ">"},
                        block=0
                        if is_memory
                        else int(self.minimum_check_interval.total_seconds() * 1000),
                        count=available_slots,
                    )
            except ResponseError as e:
                if "NOGROUP" in str(e):
                    await self.docket._ensure_stream_and_group()
                    return await get_new_deliveries(redis)
                raise  # pragma: no cover
            if is_memory and not result:
                await asyncio.sleep(self.minimum_check_interval.total_seconds())
            return result

        async def start_task(
            message_id: RedisMessageID,
            message: RedisMessage,
            is_redelivery: bool = False,
        ) -> None:
            execution = await Execution.from_message(
                self.docket,
                message,
                redelivered=is_redelivery,
                fallback_task=self.fallback_task,
            )

            task = asyncio.create_task(
                self._execute(execution),
                name=f"{self.docket.name} - task:{execution.key}",
            )
            active_tasks[task] = message_id
            task_executions[task] = execution
            self._tasks_by_key[execution.key] = task

            nonlocal available_slots
            available_slots -= 1

        async def process_completed_tasks() -> None:
            completed_tasks = {task for task in active_tasks if task.done()}
            for task in completed_tasks:
                message_id = active_tasks.pop(task)
                execution = task_executions.pop(task)
                self._tasks_by_key.pop(execution.key, None)
                try:
                    await task
                    await ack_message(redis, message_id)
                except AdmissionBlocked as e:
                    logger.debug(
                        "🔒 Task %s blocked by admission control, rescheduling",
                        e.execution.key,
                        extra=log_context,
                    )
                    e.execution.when = (
                        datetime.now(timezone.utc) + ADMISSION_BLOCKED_RETRY_DELAY
                    )
                    await e.execution.schedule(reschedule_message=message_id)

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

        try:
            async with TaskGroup() as infra:
                # Start cancellation listener and wait for it to be ready
                infra.create_task(
                    self._cancellation_listener(),
                    name=f"{self.docket.name} - cancellation listener",
                )
                await self._cancellation_ready.wait()

                if self.schedule_automatic_tasks:
                    await self._schedule_all_automatic_perpetual_tasks()

                infra.create_task(
                    self._scheduler_loop(redis),
                    name=f"{self.docket.name} - scheduler",
                )
                infra.create_task(
                    self._renew_leases(redis, active_tasks),
                    name=f"{self.docket.name} - lease renewal",
                )

                has_work: bool = True
                stopping = self._worker_stopping.is_set
                while (forever or has_work or active_tasks) and not stopping():
                    await process_completed_tasks()

                    available_slots = self.concurrency - len(active_tasks)

                    if available_slots <= 0:
                        await asyncio.sleep(self.minimum_check_interval.total_seconds())
                        continue

                    for source in [get_redeliveries, get_new_deliveries]:
                        for stream_key, messages in await source(redis):
                            is_redelivery = stream_key == b"__redelivery__"
                            for message_id, message in messages:
                                if not message:  # pragma: no cover
                                    continue

                                await start_task(message_id, message, is_redelivery)

                        if available_slots <= 0:
                            break

                    if not forever and not active_tasks:
                        has_work = await check_for_work()

                # Signal internal tasks to stop before exiting TaskGroup
                self._worker_stopping.set()

        except asyncio.CancelledError:
            if active_tasks:  # pragma: no cover
                logger.info(
                    "Shutdown requested, finishing %d active tasks...",
                    len(active_tasks),
                    extra=log_context,
                )
        finally:
            # Drain any remaining active tasks
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)
                await process_completed_tasks()

            self._worker_done.set()

    async def _scheduler_loop(self, redis: Redis) -> None:
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

                        -- Set run state to queued
                        local run_key = ARGV[2] .. ":runs:" .. task['key']
                        redis.call('HSET', run_key, 'state', 'queued')

                        -- Publish state change event to pub/sub
                        local channel = ARGV[2] .. ":state:" .. task['key']
                        local payload = '{"type":"state","key":"' .. task['key'] .. '","state":"queued","when":"' .. task['when'] .. '"}'
                        redis.call('PUBLISH', channel, payload)

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

        log_context = self._log_context()

        while not self._worker_stopping.is_set():  # pragma: no branch
            try:
                logger.debug("Scheduling due tasks", extra=log_context)
                with self._maybe_suppress_instrumentation():
                    total_work, due_work = await stream_due_tasks(
                        keys=[self.docket.queue_key, self.docket.stream_key],
                        args=[
                            datetime.now(timezone.utc).timestamp(),
                            self.docket.prefix,
                        ],
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

            # Use interruptible wait so we respond to stopping quickly
            try:
                await asyncio.wait_for(
                    self._worker_stopping.wait(),
                    timeout=self.scheduling_resolution.total_seconds(),
                )
                return  # Event was set, exit the loop
            except asyncio.TimeoutError:
                pass  # Normal timeout, continue scheduling

    async def _renew_leases(
        self,
        redis: Redis,
        active_messages: dict[asyncio.Task[None], RedisMessageID],
    ) -> None:
        """Periodically renew leases on stream messages.

        Calls XCLAIM with idle=0 to reset the message's idle time, preventing
        XAUTOCLAIM from reclaiming it while we're still processing.
        """
        # Renew leases 4 times per redelivery_timeout period
        renewal_interval = self.redelivery_timeout.total_seconds() / 4

        while not self._worker_stopping.is_set():  # pragma: no branch
            # Use interruptible wait so we respond to stopping quickly
            try:
                await asyncio.wait_for(
                    self._worker_stopping.wait(), timeout=renewal_interval
                )
                # Event was set, exit the loop
                return
            except asyncio.TimeoutError:
                # Normal timeout, continue with lease renewal
                pass

            message_ids = list(active_messages.values())
            if not message_ids:
                continue

            try:
                with self._maybe_suppress_instrumentation():
                    await redis.xclaim(
                        name=self.docket.stream_key,
                        groupname=self.docket.worker_group_name,
                        consumername=self.name,
                        min_idle_time=0,
                        message_ids=message_ids,
                        idle=0,
                    )
            except Exception:
                logger.warning("Failed to renew leases", exc_info=True)

    async def _schedule_all_automatic_perpetual_tasks(self) -> None:
        # Wait for strikes to be fully loaded before scheduling to avoid
        # scheduling struck tasks or missing restored tasks
        await self.docket.wait_for_strikes_loaded()

        async with self.docket.redis() as redis:
            try:
                async with redis.lock(
                    self.docket.key("perpetual:lock"),
                    timeout=AUTOMATIC_PERPETUAL_LOCK_TIMEOUT_SECONDS,
                    blocking=False,
                ):
                    for task_function in self.docket.tasks.values():
                        perpetual = get_single_dependency_parameter_of_type(
                            task_function, Perpetual
                        )

                        if perpetual is not None and perpetual.automatic:
                            key = task_function.__name__
                            await self.docket.add(
                                task_function, when=perpetual.initial_when, key=key
                            )()
            except LockError:  # pragma: no cover
                return

    async def _delete_known_task(self, redis: Redis, execution: Execution) -> None:
        logger.debug("Deleting known task", extra=self._log_context())
        # Delete known/stream_id from runs hash to allow task rescheduling
        runs_key = self.docket.runs_key(execution.key)
        await redis.hdel(runs_key, "known", "stream_id")

        # TODO: Remove in next breaking release (v0.14.0) - legacy key cleanup
        known_task_key = self.docket.known_task_key(execution.key)
        stream_id_key = self.docket.stream_id_key(execution.key)
        await redis.delete(known_task_key, stream_id_key)

    async def _execute(self, execution: Execution) -> None:
        log_context = {**self._log_context(), **execution.specific_labels()}
        counter_labels = {**self.labels(), **execution.general_labels()}

        call = execution.call_repr()

        if self.docket.strike_list.is_stricken(execution):
            async with self.docket.redis() as redis:
                await self._delete_known_task(redis, execution)

            await execution.mark_as_cancelled()
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
        if execution.redelivered:
            TASKS_REDELIVERED.add(1, counter_labels)
        TASKS_RUNNING.add(1, counter_labels)
        TASK_PUNCTUALITY.record(punctuality, counter_labels)

        arrow = "↬" if execution.attempt > 1 else "↪"
        logger.info(
            "%s [%s] %s", arrow, format_duration(punctuality), call, extra=log_context
        )

        # Atomically claim task and transition to running state
        # This also initializes progress and cleans up known/stream_id to allow rescheduling
        await execution.claim(self.name)

        dependencies: dict[str, Dependency] = {}

        with tracer.start_as_current_span(
            execution.function_name,
            kind=trace.SpanKind.CONSUMER,
            attributes={
                **self.labels(),
                **execution.specific_labels(),
                "code.function.name": execution.function_name,
            },
            links=execution.incoming_span_links(),
        ) as span:
            try:
                async with resolved_dependencies(self, execution) as dependencies:
                    dependency_failures = {
                        k: v
                        for k, v in dependencies.items()
                        if isinstance(v, FailedDependency)
                    }

                    # Check for AdmissionBlocked - re-raise directly (not wrapped in ExceptionGroup)
                    # This happens when ConcurrencyLimit couldn't acquire a slot
                    for failure in dependency_failures.values():
                        if isinstance(failure.error, AdmissionBlocked):
                            raise failure.error

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

                    # Merge resolved dependencies into execution kwargs
                    final_kwargs = {**execution.kwargs, **dependencies}

                    # Check for a Runtime dependency (e.g., Timeout) that controls execution
                    runtime = get_single_dependency_of_type(dependencies, Runtime)
                    if runtime:
                        result = await runtime.run(
                            execution,
                            execution.function,
                            execution.args,
                            final_kwargs,
                        )
                    else:
                        result = await execution.function(
                            *execution.args, **final_kwargs
                        )

                    duration = log_context["duration"] = time.time() - start
                    TASKS_SUCCEEDED.add(1, counter_labels)

                    span.set_status(Status(StatusCode.OK))

                    # Check for completion handler (e.g., Perpetual)
                    completion_handler = get_single_dependency_of_type(
                        dependencies, CompletionHandler
                    )
                    outcome = TaskOutcome(
                        duration=timedelta(seconds=duration),
                        result=result,
                    )
                    if completion_handler and await completion_handler.on_complete(
                        execution, outcome
                    ):
                        # Handler took responsibility (rescheduled, logged, recorded metrics)
                        await execution.mark_as_completed(result_key=None)
                    else:
                        # No handler or handler didn't handle - normal completion
                        result_key = None
                        if result is not None and self.docket.execution_ttl:
                            # Serialize and store result
                            pickled_result = cloudpickle.dumps(result)  # type: ignore[arg-type]
                            # Base64-encode for JSON serialization
                            encoded_result = base64.b64encode(pickled_result).decode(
                                "ascii"
                            )
                            result_key = execution.key
                            ttl_seconds = int(self.docket.execution_ttl.total_seconds())
                            await self.docket.result_storage.put(
                                result_key, {"data": encoded_result}, ttl=ttl_seconds
                            )
                        await execution.mark_as_completed(result_key=result_key)
                        logger.info(
                            "↩ [%s] %s",
                            format_duration(duration),
                            call,
                            extra=log_context,
                        )
            except AdmissionBlocked:
                # Re-raise to be handled by process_completed_tasks
                raise
            except asyncio.CancelledError:
                # Task was cancelled externally via docket.cancel()
                duration = log_context["duration"] = time.time() - start
                span.set_status(Status(StatusCode.OK))
                await execution.mark_as_cancelled()
                logger.info(
                    "✗ [%s] %s (cancelled)",
                    format_duration(duration),
                    call,
                    extra=log_context,
                )
            except Exception as e:
                duration = log_context["duration"] = time.time() - start
                TASKS_FAILED.add(1, counter_labels)

                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))

                outcome = TaskOutcome(
                    duration=timedelta(seconds=duration),
                    exception=e,
                )

                # Check for failure handler (e.g., Retry)
                failure_handler = get_single_dependency_of_type(
                    dependencies, FailureHandler
                )
                if failure_handler and await failure_handler.handle_failure(
                    execution, outcome
                ):
                    # Handler took responsibility (scheduled retry, logged, recorded metrics)
                    # Don't mark as failed - task is being retried
                    pass
                else:
                    # Not retried - check for completion handler (e.g., Perpetual)
                    completion_handler = get_single_dependency_of_type(
                        dependencies, CompletionHandler
                    )
                    if completion_handler and await completion_handler.on_complete(
                        execution, outcome
                    ):
                        # Handler took responsibility (rescheduled, logged, recorded metrics)
                        pass
                    else:
                        # No handler took responsibility - log normally
                        logger.exception(
                            "↩ [%s] %s",
                            format_duration(duration),
                            call,
                            extra=log_context,
                        )

                    # Store exception in result_storage (only when not retrying)
                    result_key = None
                    if self.docket.execution_ttl:
                        pickled_exception = cloudpickle.dumps(e)  # type: ignore[arg-type]
                        # Base64-encode for JSON serialization
                        encoded_exception = base64.b64encode(pickled_exception).decode(
                            "ascii"
                        )
                        result_key = execution.key
                        ttl_seconds = int(self.docket.execution_ttl.total_seconds())
                        await self.docket.result_storage.put(
                            result_key, {"data": encoded_exception}, ttl=ttl_seconds
                        )

                    # Mark execution as failed with error message
                    error_msg = f"{type(e).__name__}: {str(e)}"
                    await execution.mark_as_failed(error_msg, result_key=result_key)
            finally:
                TASKS_RUNNING.add(-1, counter_labels)
                TASKS_COMPLETED.add(1, counter_labels)
                TASK_DURATION.record(duration, counter_labels)

    def _startup_log(self) -> None:
        logger.info("Starting worker %r with the following tasks:", self.name)
        for task_name, task in self.docket.tasks.items():
            logger.info("* %s(%s)", task_name, compact_signature(get_signature(task)))

    @property
    def workers_set(self) -> str:
        return self.docket.workers_set

    def worker_tasks_set(self, worker_name: str) -> str:
        return self.docket.worker_tasks_set(worker_name)

    def task_workers_set(self, task_name: str) -> str:
        return self.docket.task_workers_set(task_name)

    async def _heartbeat(self) -> None:
        while True:
            try:
                now = datetime.now(timezone.utc).timestamp()
                maximum_age = (
                    self.docket.heartbeat_interval * self.docket.missed_heartbeats
                )
                oldest = now - maximum_age.total_seconds()

                task_names = list(self.docket.tasks)

                async with self.docket.redis() as r:
                    with self._maybe_suppress_instrumentation():
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
                                max(
                                    maximum_age, timedelta(seconds=MINIMUM_TTL_SECONDS)
                                ),
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

                    QUEUE_DEPTH.set(stream_depth + overdue_depth, self.docket.labels())
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
            except Exception:  # pragma: no cover
                logger.exception(
                    "Error sending worker heartbeat",
                    exc_info=True,
                    extra=self._log_context(),
                )

            await asyncio.sleep(self.docket.heartbeat_interval.total_seconds())

    async def _cancellation_listener(self) -> None:
        """Listen for cancellation signals and cancel matching tasks."""
        cancel_pattern = self.docket.key("cancel:*")
        log_context = self._log_context()

        while not self._worker_stopping.is_set():
            try:
                async with self.docket._pubsub() as pubsub:
                    await pubsub.psubscribe(cancel_pattern)
                    self._cancellation_ready.set()
                    # Poll for messages, checking _worker_stopping periodically
                    while not self._worker_stopping.is_set():
                        message = await pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=0.1
                        )
                        if message is not None and message["type"] == "pmessage":
                            await self._handle_cancellation(message)
            except ConnectionError:
                if self._worker_stopping.is_set():
                    return  # pragma: no cover
                REDIS_DISRUPTIONS.add(1, self.labels())
                logger.warning(
                    "Redis connection error in cancellation listener, reconnecting...",
                    extra=log_context,
                )
                await asyncio.sleep(1)
            except Exception:
                if self._worker_stopping.is_set():
                    return  # pragma: no cover
                logger.exception(
                    "Error in cancellation listener",
                    exc_info=True,
                    extra=log_context,
                )
                await asyncio.sleep(1)

    async def _handle_cancellation(self, message: PubSubMessage) -> None:
        """Handle a cancellation message by cancelling the matching task."""
        data = message["data"]
        key: TaskKey = data.decode() if isinstance(data, bytes) else data

        if task := self._tasks_by_key.get(key):  # pragma: no branch
            logger.info(
                "Cancelling running task %r",
                key,
                extra=self._log_context(),
            )
            task.cancel()
