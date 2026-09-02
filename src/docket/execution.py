import asyncio
import base64
import enum
import inspect
import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    Mapping,
    Sequence,
)

import cloudpickle
import opentelemetry.context
import uncalled_for
from opentelemetry import propagate, trace
from ._telemetry import suppress_instrumentation
from typing_extensions import Self

# Re-export _signature_cache from uncalled-for so that docket and uncalled-for
# share one cache dict.  FastMCP clears `docket.execution._signature_cache` after
# mutating function signatures, so this must be the same object that
# uncalled-for's get_dependency_parameters uses internally.
from uncalled_for.introspection import (
    _signature_cache as _signature_cache,
    get_signature as _uncalled_for_get_signature,
)

from ._execution_progress import ExecutionProgress, ProgressEvent, StateEvent
from ._execution_scripts import _claim, _schedule, _terminal
from ._redis import RedisClient, confirm_subscriptions, is_cluster_client
from .annotations import Logged
from .instrumentation import CACHE_SIZE, message_getter, message_setter

if TYPE_CHECKING:
    from .docket import Docket, RedisMessageID

logger: logging.Logger = logging.getLogger(__name__)


class ExecutionCancelled(Exception):
    """Raised when get_result() is called on a cancelled execution."""

    pass


TaskFunction = Callable[..., Awaitable[Any]]
Message = dict[bytes, bytes]


def _hash_reply(flat: Sequence[bytes]) -> Message:
    """Pair up a hash that a Lua script returned as a flat field/value array."""
    fields = iter(flat)
    return dict(zip(fields, fields))


async def schedule_many(
    redis: RedisClient,
    executions: "Sequence[Execution]",
    *,
    replace: bool,
    chunk_size: int | None = 1000,
) -> None:
    """Schedule N executions in O(N / chunk_size) pipelined round-trips.

    Runs the same ``_schedule`` script as ``Execution.schedule()`` for each
    execution, but queued on non-transactional pipelines instead of one
    round-trip apiece.  Each script invocation is still individually atomic
    in Redis, so per-key dedup/replace semantics are identical to the
    single-call path; there is intentionally no atomicity *across* the
    batch.  Each execution's ``disposition`` and ``state`` are updated from
    its own reply, and a per-command Redis error marks just that execution
    ``Disposition.FAILED`` (with the exception attached as
    ``Execution.schedule_exception``) rather than aborting the rest of the
    batch.  Connection-level failures still raise; executions in chunks
    that never reached Redis keep ``Disposition.LOADED``.

    ``chunk_size`` bounds how many schedules are buffered client-side and
    sent per round-trip: smaller chunks bound memory and give other clients
    more room to interleave, larger chunks minimize round-trips.  Pass
    ``None`` to send the entire batch as one pipeline.

    On standalone Redis and the memory backend, each invocation is an
    EVALSHA primed by one idempotent ``SCRIPT LOAD`` (a pipelined EVALSHA
    has no NOSCRIPT fallback).  On Redis Cluster, the full source is sent
    with EVAL instead: redis-py blocks pipelined EVALSHA in cluster mode,
    and a node's script cache can't be relied upon across failover and
    resharding anyway.

    Neither this nor ``Execution.schedule()`` takes a per-key lock: the
    ``_schedule`` script is atomic on its own, and pipelined invocations
    already serialize in Redis.
    """
    if chunk_size is not None and chunk_size < 1:
        raise ValueError(f"chunk_size must be at least 1, got {chunk_size}")

    if not executions:
        return

    # A conditional expression (not an if/else) so each CI backend leg can
    # still reach 100% branch coverage: only cluster legs take the EVAL arm.
    enqueue_schedule = (
        _schedule.enqueue_eval if is_cluster_client(redis) else _schedule.enqueue
    )
    # Prime the script cache for the EVALSHA path.  SCRIPT LOAD is idempotent
    # and O(1) per batch; on a cluster (where the EVAL arm carries its own
    # source) it is a redundant-but-harmless broadcast to the primaries,
    # which keeps this function free of per-backend branches.
    await redis.script_load(_schedule.lua)

    per_round_trip = chunk_size if chunk_size is not None else len(executions)
    for chunk_start in range(0, len(executions), per_round_trip):
        chunk = executions[chunk_start : chunk_start + per_round_trip]
        per_execution: list[tuple[Execution, dict[str, Any], bool]] = [
            (execution, *execution._schedule_script_args(replace))  # pyright: ignore[reportPrivateUsage]
            for execution in chunk
        ]

        # Non-transactional: wrapping a chunk in MULTI/EXEC would make it one
        # uninterruptible block on the server, head-of-line-blocking worker
        # claims and heartbeats, and per-execution semantics don't need it.
        try:
            pipeline = redis.pipeline(transaction=False)
        except TypeError:  # pragma: no cover - burner's pipeline() takes no arguments
            # The in-process memory backend executes commands sequentially
            # with no MULTI/EXEC concept, so its default pipeline already
            # behaves this way.
            pipeline = redis.pipeline()
        async with pipeline:
            for _, script_args, _ in per_execution:
                enqueue_schedule(pipeline, **script_args)
            replies = await pipeline.execute(raise_on_error=False)

        for (execution, _, is_immediate), reply in zip(
            per_execution, replies, strict=True
        ):
            if isinstance(reply, Exception):
                execution.disposition = Disposition.FAILED
                execution.schedule_exception = reply
            else:
                execution._apply_schedule_reply(reply, is_immediate)  # pyright: ignore[reportPrivateUsage]


def get_signature(function: Callable[..., Any]) -> inspect.Signature:
    signature = _uncalled_for_get_signature(function)
    CACHE_SIZE.set(len(_signature_cache), {"cache": "signature"})
    return signature


class ExecutionState(enum.Enum):
    """Lifecycle states for task execution."""

    SCHEDULED = "scheduled"
    """Task is scheduled and waiting in the queue for its execution time."""

    QUEUED = "queued"
    """Task has been moved to the stream and is ready to be claimed by a worker."""

    RUNNING = "running"
    """Task is currently being executed by a worker."""

    COMPLETED = "completed"
    """Task execution finished successfully."""

    FAILED = "failed"
    """Task execution failed."""

    CANCELLED = "cancelled"
    """Task was explicitly cancelled before completion."""


class Disposition(enum.Enum):
    """Outcome of a scheduling attempt for an Execution.

    This is distinct from ExecutionState: ExecutionState tracks the lifecycle
    of the task itself (scheduled → queued → running → completed / failed /
    cancelled), while Disposition records what happened the moment a caller
    tried to schedule it via Docket.add or Docket.replace.
    """

    LOADED = "loaded"
    """This Execution was not produced by a fresh scheduling attempt. Default
    for any Execution constructed outside of ``Docket.add`` / ``Docket.replace``
    (for example, one reconstructed from a stream message inside the worker)."""

    SCHEDULED = "scheduled"
    """The task was placed on the queue (or stream, for immediate tasks)."""

    ALREADY_SCHEDULED = "already_scheduled"
    """A task with the same key was already known to the docket; the prior
    schedule was preserved and this attempt was a no-op. Only possible with
    ``Docket.add`` (``Docket.replace`` overwrites)."""

    STRUCK = "struck"
    """A strike rule blocked the call before any Redis state was touched."""

    SUPERSEDED = "superseded"
    """A newer schedule already holds this key, so the attempt left it alone.
    Only possible when the caller states the generation it expects, which
    ``Perpetual`` does when it reschedules the attempt that just finished;
    ``Docket.add`` and ``Docket.replace`` never produce it."""

    FAILED = "failed"
    """The Redis command scheduling this task returned an error.  Only
    possible with ``Docket.add_many`` / ``Docket.replace_many``, which record
    each execution's error instead of aborting the batch; the underlying
    exception is attached as ``Execution.schedule_exception``."""


@dataclass(frozen=True)
class TaskCall:
    """A fully-resolved request to schedule one task.

    Built with :meth:`Docket.call` and consumed in bulk by
    :meth:`Docket.add_many` / :meth:`Docket.replace_many`.  All scheduling
    inputs (function, arguments, key, and time) are resolved at construction,
    so a ``TaskCall`` describes exactly one future execution.
    """

    function: TaskFunction
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    key: str
    when: datetime
    function_name: str | None = None


class Execution:
    """Represents a task execution with state management and progress tracking.

    Combines task invocation metadata (function, args, when, etc.) with
    Redis-backed lifecycle state tracking and user-reported progress.
    """

    def __init__(
        self,
        docket: "Docket",
        function: TaskFunction,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        key: str,
        when: datetime,
        attempt: int,
        trace_context: opentelemetry.context.Context | None = None,
        redelivered: bool = False,
        function_name: str | None = None,
        generation: int = 0,
        message_id: "RedisMessageID | None" = None,
    ) -> None:
        # Task definition (immutable)
        self._docket = docket
        self._function = function
        self._function_name = function_name or function.__name__
        self._args = args
        self._kwargs = kwargs
        self._key = key

        # Scheduling metadata
        self.when = when
        self.attempt = attempt
        self._trace_context = trace_context
        self._redelivered = redelivered
        self._generation = generation
        self.message_id = message_id

        # True once the stream message identified by ``message_id`` has been
        # XACKed (by ``_terminal``, ``_claim`` on SUPERSEDED, or ``_schedule``
        # when re-routing this same message).  The worker uses this as a
        # safety-net signal: anything that calls a ``FailureHandler`` whose
        # ``handle_failure`` returns True without rescheduling will leave this
        # False, and the worker can ack defensively.
        self._acked: bool = False

        # Lifecycle state (mutable)
        self.state: ExecutionState = ExecutionState.SCHEDULED
        self.worker: str | None = None
        self.started_at: datetime | None = None
        self.completed_at: datetime | None = None
        self.error: str | None = None
        self.result_key: str | None = None
        self.disposition: Disposition = Disposition.LOADED
        # Set alongside Disposition.FAILED when a batch schedule's Redis
        # command errored for this execution specifically.
        self.schedule_exception: BaseException | None = None

        # Progress tracking
        self.progress: ExecutionProgress = ExecutionProgress(docket, key)

        # Redis key
        self._redis_key = docket.key(f"runs:{key}")

    # Task definition properties (immutable)
    @property
    def docket(self) -> "Docket":
        """Parent docket instance."""
        return self._docket

    @property
    def function(self) -> TaskFunction:
        """Task function to execute."""
        return self._function

    @property
    def args(self) -> tuple[Any, ...]:
        """Positional arguments for the task."""
        return self._args

    @property
    def kwargs(self) -> dict[str, Any]:
        """Keyword arguments for the task."""
        return self._kwargs

    @property
    def key(self) -> str:
        """Unique task identifier."""
        return self._key

    @property
    def function_name(self) -> str:
        """Name of the task function (from message, may differ from function.__name__ for fallback tasks)."""
        return self._function_name

    # Scheduling metadata properties
    @property
    def trace_context(self) -> opentelemetry.context.Context | None:
        """OpenTelemetry trace context."""
        return self._trace_context

    @property
    def redelivered(self) -> bool:
        """Whether this message was redelivered."""
        return self._redelivered

    @property
    def generation(self) -> int:
        """Scheduling generation counter for supersession detection."""
        return self._generation

    @contextmanager
    def _maybe_suppress_instrumentation(self) -> Generator[None, None, None]:
        """Suppress OTel auto-instrumentation for internal Redis operations."""
        if not self._docket.enable_internal_instrumentation:
            with suppress_instrumentation():
                yield
        else:  # pragma: no cover
            yield

    def as_message(self) -> Message:
        return {
            b"key": self.key.encode(),
            b"when": self.when.isoformat().encode(),
            b"function": self.function_name.encode(),
            b"args": cloudpickle.dumps(self.args),
            b"kwargs": cloudpickle.dumps(self.kwargs),
            b"attempt": str(self.attempt).encode(),
            b"generation": str(self.generation).encode(),
        }

    @classmethod
    async def from_message(
        cls,
        docket: "Docket",
        message: Message,
        redelivered: bool = False,
        fallback_task: TaskFunction | None = None,
        message_id: "RedisMessageID | None" = None,
        sync: bool = True,
    ) -> Self:
        """Rebuild an execution from the stream message that carries it.

        ``sync`` reads the current lifecycle state back from Redis.  A worker
        about to claim the task passes ``sync=False``, because ``claim()``
        fills in the same attributes from its own reply a moment later.
        """
        function_name = message[b"function"].decode()
        if not (function := docket.tasks.get(function_name)):
            if fallback_task is None:
                raise ValueError(
                    f"Task function {function_name!r} is not registered with the current docket"
                )
            function = fallback_task

        instance = cls(
            docket=docket,
            function=function,
            args=cloudpickle.loads(message[b"args"]),
            kwargs=cloudpickle.loads(message[b"kwargs"]),
            key=message[b"key"].decode(),
            when=datetime.fromisoformat(message[b"when"].decode()),
            attempt=int(message[b"attempt"].decode()),
            trace_context=propagate.extract(message, getter=message_getter),
            redelivered=redelivered,
            function_name=function_name,
            generation=int(message.get(b"generation", b"0")),
            message_id=message_id,
        )
        if sync:
            await instance.sync()
        return instance

    def general_labels(self) -> Mapping[str, str]:
        return {"docket.task": self.function_name}

    def specific_labels(self) -> Mapping[str, str | int]:
        return {
            "docket.task": self.function_name,
            "docket.key": self.key,
            "docket.when": self.when.isoformat(),
            "docket.attempt": self.attempt,
        }

    def get_argument(self, parameter: str) -> Any:
        signature = get_signature(self.function)
        bound_args = signature.bind(*self.args, **self.kwargs)
        return bound_args.arguments[parameter]

    def call_repr(self) -> str:
        arguments: list[str] = []
        function_name = self.function_name

        signature = get_signature(self.function)
        logged_parameters = Logged.annotated_parameters(signature)
        parameter_names = list(signature.parameters.keys())

        for i, argument in enumerate(self.args[: len(parameter_names)]):
            parameter_name = parameter_names[i]
            if logged := logged_parameters.get(parameter_name):
                arguments.append(logged.format(argument))
            else:
                arguments.append("...")

        for parameter_name, argument in self.kwargs.items():
            if logged := logged_parameters.get(parameter_name):
                arguments.append(f"{parameter_name}={logged.format(argument)}")
            else:
                arguments.append(f"{parameter_name}=...")

        return f"{function_name}({', '.join(arguments)}){{{self.key}}}"

    def incoming_span_links(self) -> list[trace.Link]:
        initiating_span = trace.get_current_span(self.trace_context)
        initiating_context = initiating_span.get_span_context()
        return [trace.Link(initiating_context)] if initiating_context.is_valid else []

    async def schedule(
        self,
        replace: bool = False,
        reschedule_message: "RedisMessageID | None" = None,
        expected_generation: int = 0,
    ) -> Disposition:
        """Schedule this task atomically in Redis.

        This performs an atomic operation that:
        - Adds the task to the stream (immediate) or queue (future)
        - Writes the execution state record
        - Tracks metadata for later cancellation

        Usage patterns:
        - Normal add: schedule(replace=False)
        - Replace existing: schedule(replace=True)
        - Reschedule from stream: schedule(reschedule_message=message_id)
          This atomically acknowledges and deletes the stream message, then
          reschedules the task to the queue. Prevents both task loss and
          duplicate execution when rescheduling tasks (e.g., due to concurrency limits).

        Args:
            replace: If True, replaces any existing task with the same key.
                    If False and the task already exists, this is a no-op
                    (the existing schedule is preserved).
            reschedule_message: If provided, atomically acknowledges and deletes
                    this stream message ID before rescheduling the task to the queue.
                    Used when a task needs to be rescheduled from an active stream message.
            expected_generation: The generation the caller believes holds this
                    key.  When it is non-zero and Redis holds a newer one, the
                    script leaves the key alone.  0 skips the check.

        Returns:
            ``Disposition.SCHEDULED`` if the task was placed on the queue/stream,
            ``Disposition.ALREADY_SCHEDULED`` if a task with the same key was
            already known and ``replace=False`` (in which case the existing
            schedule is preserved and no local state changes are published),
            or ``Disposition.SUPERSEDED`` if ``expected_generation`` was stale.
            Sets ``self.disposition`` to the same value.
        """
        script_args, is_immediate = self._schedule_script_args(
            replace, reschedule_message, expected_generation
        )
        async with self.docket.redis() as redis:
            reply = await _schedule(redis, **script_args)

        return self._apply_schedule_reply(reply, is_immediate, reschedule_message)

    def _schedule_script_args(
        self,
        replace: bool,
        reschedule_message: "RedisMessageID | None" = None,
        expected_generation: int = 0,
    ) -> tuple[dict[str, Any], bool]:
        """Build the keyword arguments for one ``_schedule`` script invocation.

        Shared by the single-call path (``schedule()``) and the batch path
        (``schedule_many``), so both schedule with identical semantics.
        Returns the kwargs plus the ``is_immediate`` decision (stream vs.
        queue), which the caller must hand back to
        ``_apply_schedule_reply`` when interpreting the script's reply.
        """
        message: dict[bytes, bytes] = self.as_message()
        propagate.inject(message, setter=message_setter)

        key = self.key
        when = self.when
        is_immediate = when <= datetime.now(timezone.utc)

        # The Lua takes the payload as a pre-formatted string so it can just
        # call PUBLISH; cjson isn't available on the in-memory backend.  State
        # is QUEUED when the task lands directly on the stream (any
        # is_immediate path, including immediate retries), SCHEDULED when it's
        # parked for a future time.
        published_state = (
            ExecutionState.QUEUED.value
            if is_immediate
            else ExecutionState.SCHEDULED.value
        )
        state_payload = json.dumps(
            {
                "type": "state",
                "key": key,
                "state": published_state,
                "when": when.isoformat(),
            }
        )

        script_args: dict[str, Any] = {
            "stream_key": self.docket.stream_key,
            "known_key": self.docket.known_task_key(key),
            "parked_key": self.docket.parked_task_key(key),
            "queue_key": self.docket.queue_key,
            "stream_id_key": self.docket.stream_id_key(key),
            "runs_key": self._redis_key,
            "state_channel": self.docket.key(f"state:{key}"),
            "task_key": key,
            "when_timestamp": when.timestamp(),
            "is_immediate": is_immediate,
            "replace": replace,
            "reschedule_message_id": reschedule_message or b"",
            "expected_generation": expected_generation,
            "worker_group_name": self.docket.worker_group_name,
            "state_payload": state_payload,
            "message": message,
        }
        return script_args, is_immediate

    def _apply_schedule_reply(
        self,
        reply: bytes | str,
        is_immediate: bool,
        reschedule_message: "RedisMessageID | None" = None,
    ) -> Disposition:
        """Fold one ``_schedule`` script reply back into this execution."""
        if reply in (b"SUPERSEDED", "SUPERSEDED"):
            # A newer generation holds the key and the script left it alone,
            # so this execution never became real anywhere.
            self.disposition = Disposition.SUPERSEDED
            return self.disposition

        if reply in (b"EXISTS", "EXISTS"):
            # An existing schedule for this key remains untouched; leave local
            # state alone and do not publish a misleading state event.
            self.disposition = Disposition.ALREADY_SCHEDULED
            return self.disposition

        if is_immediate:
            self.state = ExecutionState.QUEUED
        else:
            self.state = ExecutionState.SCHEDULED

        # The reschedule branch in `_schedule` XACKed and XDELed the original
        # stream message, so any caller passing in our own message_id has
        # implicitly retired this Execution's pending entry.
        if reschedule_message and reschedule_message == self.message_id:
            self._acked = True

        self.disposition = Disposition.SCHEDULED
        return self.disposition

    async def claim(self, worker: str) -> bool:
        """Atomically check supersession and claim task in a single round-trip.

        This consolidates worker operations when claiming a task into a single
        atomic Lua script that:
        - Checks if the task has been superseded by a newer generation
        - Sets state to RUNNING with worker name and timestamp
        - Initializes progress tracking (current=0, total=100)
        - Deletes known/stream_id fields to allow task rescheduling
        - Cleans up legacy keys for backwards compatibility
        - Reads the runs hash and the progress hash back

        The script returns those two hashes as they stand when it finishes, so
        the claim leaves this execution's lifecycle attributes exactly where a
        ``sync()`` would.  That holds on both paths: a claimed task reports its
        own running state and reset progress, and a refused one reports what
        the newer generation left on the key (or the ``sync()`` defaults, if
        the key is gone).  Callers on the delivery path can therefore skip the
        ``sync()`` in ``from_message`` and let the claim fill the attributes in.

        Args:
            worker: Name of the worker claiming the task

        Returns:
            True if the task was claimed, False if it was superseded.
        """
        started_at = datetime.now(timezone.utc)
        started_at_iso = started_at.isoformat()

        # Pre-build the running-state payload; Lua only publishes it on the
        # non-SUPERSEDED path.
        state_payload = json.dumps(
            {
                "type": "state",
                "key": self.key,
                "state": ExecutionState.RUNNING.value,
                "worker": worker,
                "started_at": started_at_iso,
            }
        )

        with self._maybe_suppress_instrumentation():
            async with self.docket.redis() as redis:
                status, runs_data, progress_data = await _claim(
                    redis,
                    runs_key=self._redis_key,
                    progress_key=self.progress._redis_key,
                    known_key=self.docket.known_task_key(self.key),
                    stream_id_key=self.docket.stream_id_key(self.key),
                    state_channel=self.docket.key(f"state:{self.key}"),
                    stream_key=self.docket.stream_key,
                    worker=worker,
                    started_at=started_at_iso,
                    generation=self._generation,
                    state_payload=state_payload,
                    worker_group_name=self.docket.worker_group_name,
                    message_id=self.message_id or b"",
                )

        self._apply_runs_data(_hash_reply(runs_data))
        self.progress._apply(_hash_reply(progress_data))  # pyright: ignore[reportPrivateUsage]

        if status == b"SUPERSEDED":
            # The `_claim` Lua XACKed and XDELed the stale stream message
            # before returning SUPERSEDED (skipping the ack when message_id
            # is empty -- harmless either way).
            self._acked = True
            return False

        return True

    async def _mark_as_terminal(
        self,
        state: ExecutionState,
        *,
        error: str | None = None,
        result_key: str | None = None,
    ) -> None:
        """Mark task as having reached a terminal state.

        Args:
            state: The terminal state (COMPLETED, FAILED, or CANCELLED)
            error: Optional error message (for FAILED state)
            result_key: Optional key where the result/exception is stored

        Uses a Lua script to atomically check supersession, write the
        terminal state, publish the completion event, delete the progress
        hash, and ACK/XDEL the stream message in a single round-trip.  If
        the runs hash has been claimed by a successor (e.g. a Perpetual
        on_complete already called docket.replace()), the hash is left
        untouched, but progress cleanup, the completion event, and the
        stream ACK/XDEL still happen.
        """
        completed_at = datetime.now(timezone.utc).isoformat()

        # Build the optional HSET fields
        extra_fields: list[str] = []
        if error:
            extra_fields.extend(["error", error])
        if result_key is not None:
            extra_fields.extend(["result_key", result_key])

        ttl_seconds = (
            int(self.docket.execution_ttl.total_seconds())
            if self.docket.execution_ttl
            else 0
        )

        # Pre-build the terminal-state payload; the Lua publishes it on both
        # the success and supersession paths.
        state_payload_data: dict[str, str] = {
            "type": "state",
            "key": self.key,
            "state": state.value,
            "completed_at": completed_at,
        }
        if error:
            state_payload_data["error"] = error
        state_payload = json.dumps(state_payload_data)

        # Set ``_acked = True`` *before* the awaited Lua call: the server
        # commit is the source of truth, so once we hand the call off we own
        # the ack semantically.  A network blip on the response path (server
        # committed, client raised) would otherwise leave us looking unacked
        # and let the worker safety net overwrite the committed terminal
        # state with FAILED/None.
        self._acked = True

        with self._maybe_suppress_instrumentation():
            async with self.docket.redis() as redis:
                await _terminal(
                    redis,
                    runs_key=self._redis_key,
                    state_channel=self.docket.key(f"state:{self.key}"),
                    progress_key=self.progress._redis_key,
                    stream_key=self.docket.stream_key,
                    generation=self._generation,
                    state=state.value,
                    completed_at=completed_at,
                    ttl_seconds=ttl_seconds,
                    state_payload=state_payload,
                    worker_group_name=self.docket.worker_group_name,
                    message_id=self.message_id or b"",
                    extra_fields=extra_fields,
                )

        self.state = state
        if result_key is not None:
            self.result_key = result_key

        self.progress.current = None
        self.progress.total = 100
        self.progress.message = None
        self.progress.updated_at = None

    async def mark_as_completed(self, result_key: str | None = None) -> None:
        """Mark task as completed successfully.

        Args:
            result_key: Optional key where the task result is stored
        """
        await self._mark_as_terminal(ExecutionState.COMPLETED, result_key=result_key)

    async def mark_as_failed(
        self, error: str | None = None, result_key: str | None = None
    ) -> None:
        """Mark task as failed.

        Args:
            error: Optional error message describing the failure
            result_key: Optional key where the exception is stored
        """
        await self._mark_as_terminal(
            ExecutionState.FAILED, error=error, result_key=result_key
        )

    async def mark_as_cancelled(self) -> None:
        """Mark task as cancelled."""
        await self._mark_as_terminal(ExecutionState.CANCELLED)

    async def get_result(
        self,
        *,
        timeout: timedelta | None = None,
        deadline: datetime | None = None,
    ) -> Any:
        """Retrieve the result of this task execution.

        If the execution is not yet complete, this method will wait using
        pub/sub for state updates until completion.

        Args:
            timeout: Optional duration to wait before giving up.
                    If None and deadline is None, waits indefinitely.
            deadline: Optional absolute datetime when to stop waiting.
                     If None and timeout is None, waits indefinitely.

        Returns:
            The result of the task execution, or None if the task returned None.

        Raises:
            ValueError: If both timeout and deadline are provided
            ExecutionCancelled: If the execution was cancelled before completing
            Exception: If the task failed, raises the stored exception
            TimeoutError: If timeout/deadline is reached before execution completes
        """
        # Validate that only one time limit is provided
        if timeout is not None and deadline is not None:
            raise ValueError("Cannot specify both timeout and deadline")

        # Convert timeout to deadline if provided
        if timeout is not None:
            deadline = datetime.now(timezone.utc) + timeout

        terminal_states = (
            ExecutionState.COMPLETED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
        )

        # Wait for execution to complete if not already done
        if self.state not in terminal_states:
            # Calculate timeout duration if absolute deadline provided
            timeout_seconds = None
            if deadline is not None:
                timeout_seconds = (
                    deadline - datetime.now(timezone.utc)
                ).total_seconds()
                if timeout_seconds <= 0:
                    raise TimeoutError(
                        f"Timeout waiting for execution {self.key} to complete"
                    )

            try:

                async def wait_for_completion():
                    async for event in self.subscribe():  # pragma: no branch
                        if event["type"] == "state":
                            state = ExecutionState(event["state"])
                            if state in terminal_states:
                                # Sync to get latest data including result key
                                await self.sync()
                                break

                # Use asyncio.wait_for to enforce timeout
                await asyncio.wait_for(wait_for_completion(), timeout=timeout_seconds)
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"Timeout waiting for execution {self.key} to complete"
                )

        # If cancelled, raise ExecutionCancelled
        if self.state == ExecutionState.CANCELLED:
            raise ExecutionCancelled(f"Execution {self.key} was cancelled")

        # If failed, retrieve and raise the exception
        if self.state == ExecutionState.FAILED:
            if self.result_key:
                # Retrieve serialized exception from result_storage
                result_data = await self.docket.result_storage.get(self.result_key)
                if result_data and "data" in result_data:
                    # Base64-decode and unpickle
                    pickled_exception = base64.b64decode(result_data["data"])
                    exception = cloudpickle.loads(pickled_exception)
                    raise exception
            # If no stored exception, raise a generic error with the error message
            error_msg = self.error or "Task execution failed"
            raise Exception(error_msg)

        # If completed successfully, retrieve result if available
        if self.result_key:
            result_data = await self.docket.result_storage.get(self.result_key)
            if result_data is not None and "data" in result_data:
                # Base64-decode and unpickle
                pickled_result = base64.b64decode(result_data["data"])
                return cloudpickle.loads(pickled_result)

        # No result stored - task returned None
        return None

    def _apply_runs_data(self, data: Mapping[bytes, bytes]) -> None:
        """Take the lifecycle attributes from one read of the runs hash.

        An empty hash means Redis holds nothing for this key, so everything
        goes back to the defaults of a task that has not started.  A hash
        without a ``state`` field leaves the current state alone.
        """
        if data:
            state_value = data.get(b"state")
            if state_value:
                self.state = ExecutionState(state_value.decode())

            self.worker = data[b"worker"].decode() if b"worker" in data else None
            self.started_at = (
                datetime.fromisoformat(data[b"started_at"].decode())
                if b"started_at" in data
                else None
            )
            self.completed_at = (
                datetime.fromisoformat(data[b"completed_at"].decode())
                if b"completed_at" in data
                else None
            )
            self.error = data[b"error"].decode() if b"error" in data else None
            self.result_key = (
                data[b"result_key"].decode() if b"result_key" in data else None
            )
        else:
            self.state = ExecutionState.SCHEDULED
            self.worker = None
            self.started_at = None
            self.completed_at = None
            self.error = None
            self.result_key = None

    async def sync(self) -> None:
        """Synchronize instance attributes with current execution data from Redis.

        Updates self.state, execution metadata, and progress data from Redis.
        Sets attributes to None if no data exists.  No branch sits between the
        two reads, so the runs hash and the progress hash go out together.
        """
        with self._maybe_suppress_instrumentation():
            async with self.docket.redis() as redis:
                async with redis.pipeline() as pipe:
                    pipe.hgetall(self._redis_key)
                    self.progress._read(pipe)  # pyright: ignore[reportPrivateUsage]
                    data, progress_data = await pipe.execute()

        self._apply_runs_data(data)
        self.progress._apply(progress_data)  # pyright: ignore[reportPrivateUsage]

    async def is_superseded(self) -> bool:
        """Check whether a newer schedule has superseded this execution.

        Compares this execution's generation against the current generation
        stored in the runs hash. If the stored generation is strictly greater,
        this execution has been superseded by a newer schedule() call.

        Generation 0 means the message predates generation tracking (e.g. it
        was moved from queue to stream by an older worker's scheduler that
        doesn't pass through the generation field). These are never considered
        superseded since we can't tell.
        """
        if self._generation == 0:
            return False
        with self._maybe_suppress_instrumentation():
            async with self.docket.redis() as redis:
                current = await redis.hget(self._redis_key, "generation")
        current_gen = int(current) if current is not None else 0
        return current_gen > self._generation

    async def subscribe(
        self, *, ready: asyncio.Event | None = None
    ) -> AsyncGenerator[StateEvent | ProgressEvent, None]:
        """Subscribe to both state and progress updates for this task.

        Emits the current state as the first event, then subscribes to real-time
        state and progress updates via Redis pub/sub.

        Args:
            ready: Optional ``asyncio.Event`` that is ``set()`` once the
                Redis ``SUBSCRIBE`` has been acknowledged.  Lets callers
                deterministically wait until the subscription is live
                before publishing -- avoids the race where early events
                are dropped because the subscriber hadn't connected yet.

        Yields:
            Dict containing state or progress update events with a 'type' field:
            - For state events: type="state", state, worker, timestamps, error
            - For progress events: type="progress", current, total, message, updated_at
        """
        # First, emit the current state
        await self.sync()

        # Build initial state event from current attributes
        initial_state: StateEvent = {
            "type": "state",
            "key": self.key,
            "state": self.state,
            "when": self.when.isoformat(),
            "worker": self.worker,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "error": self.error,
        }

        yield initial_state

        progress_event: ProgressEvent = {
            "type": "progress",
            "key": self.key,
            "current": self.progress.current,
            "total": self.progress.total,
            "message": self.progress.message,
            "updated_at": self.progress.updated_at.isoformat()
            if self.progress.updated_at
            else None,
        }

        yield progress_event

        # Then subscribe to real-time updates
        state_channel = self.docket.key(f"state:{self.key}")
        progress_channel = self.docket.key(f"progress:{self.key}")
        async with self.docket._pubsub() as pubsub:
            await pubsub.subscribe(state_channel, progress_channel)
            await confirm_subscriptions(pubsub, 2)
            if ready is not None:
                ready.set()
            async for message in pubsub.listen():  # pragma: no cover
                if message["type"] == "message":
                    message_data = json.loads(message["data"])
                    if message_data["type"] == "state":
                        message_data["state"] = ExecutionState(message_data["state"])
                    yield message_data


def compact_signature(signature: inspect.Signature) -> str:
    parameters: list[str] = []
    dependencies: int = 0

    for parameter in signature.parameters.values():
        if isinstance(parameter.default, uncalled_for.Dependency):
            dependencies += 1
            continue

        parameter_definition = parameter.name
        if parameter.annotation is not parameter.empty:
            annotation = parameter.annotation
            if hasattr(annotation, "__origin__"):
                annotation = annotation.__args__[0]

            type_name = getattr(annotation, "__name__", str(annotation))
            parameter_definition = f"{parameter.name}: {type_name}"

        if parameter.default is not parameter.empty:
            parameter_definition = f"{parameter_definition} = {parameter.default!r}"

        parameters.append(parameter_definition)

    if dependencies > 0:
        parameters.append("...")

    return ", ".join(parameters)
