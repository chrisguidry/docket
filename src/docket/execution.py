import abc
import enum
import inspect
import json
import logging
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Hashable,
    Literal,
    Mapping,
    Protocol,
    cast,
)

from redis.asyncio import Redis
from typing_extensions import Self

import cloudpickle  # type: ignore[import]
import opentelemetry.context
from opentelemetry import propagate, trace

from .annotations import Logged
from .instrumentation import CACHE_SIZE, message_getter, message_setter

if TYPE_CHECKING:
    from .docket import Docket

logger: logging.Logger = logging.getLogger(__name__)

TaskFunction = Callable[..., Awaitable[Any]]
Message = dict[bytes, bytes]


class _schedule_task(Protocol):
    async def __call__(
        self, keys: list[str], args: list[str | float | bytes]
    ) -> str: ...  # pragma: no cover


_signature_cache: dict[Callable[..., Any], inspect.Signature] = {}


def get_signature(function: Callable[..., Any]) -> inspect.Signature:
    if function in _signature_cache:
        CACHE_SIZE.set(len(_signature_cache), {"cache": "signature"})
        return _signature_cache[function]

    signature_attr = getattr(function, "__signature__", None)
    if isinstance(signature_attr, inspect.Signature):
        _signature_cache[function] = signature_attr
        CACHE_SIZE.set(len(_signature_cache), {"cache": "signature"})
        return signature_attr

    signature = inspect.signature(function)
    _signature_cache[function] = signature
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
        self._redis_key = f"{docket.name}:progress:{key}"
        self.current: int | None = None
        self.total: int | None = None
        self.message: str | None = None
        self.updated_at: datetime | None = None

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
            total: The total number of units to complete
        """
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
            amount: Amount to increment by (default: 1)
        """
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

    async def set_message(self, message: str) -> None:
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
        async with self.docket.redis() as redis:
            data = await redis.hgetall(self._redis_key)
            if data:
                self.current = int(data.get(b"current", b"0"))
                self.total = int(data[b"total"]) if b"total" in data else None
                self.message = data[b"message"].decode() if b"message" in data else None
                self.updated_at = (
                    datetime.fromisoformat(data[b"updated_at"].decode())
                    if b"updated_at" in data
                    else None
                )
            else:
                self.current = None
                self.total = None
                self.message = None
                self.updated_at = None

    async def _delete(self) -> None:
        """Delete the progress data from Redis.

        Called internally when task execution completes.
        """
        async with self.docket.redis() as redis:
            await redis.delete(self._redis_key)
        # Reset instance attributes
        self.current = None
        self.total = None
        self.message = None
        self.updated_at = None

    async def _publish(self, data: dict) -> None:
        """Publish progress update to Redis pub/sub channel.

        Args:
            data: Progress data to publish (partial update)
        """
        # Skip pub/sub for memory:// backend
        if self.docket.url.startswith("memory://"):
            return

        channel = f"{self.docket.name}:progress:{self.key}"
        # Create ephemeral Redis client for publishing
        redis = Redis.from_url(self.docket.url)
        try:
            # Use instance attributes for current state
            payload = {
                "type": "progress",
                "key": self.key,
                "current": self.current if self.current is not None else 0,
                "total": self.total,
                "message": self.message,
                "updated_at": data.get("updated_at"),
            }

            # Publish JSON payload
            await redis.publish(channel, json.dumps(payload))
        finally:
            await redis.aclose()

    async def subscribe(self) -> AsyncGenerator[dict[str, Any], None]:
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
        channel = f"{self.docket.name}:progress:{self.key}"
        redis = Redis.from_url(self.docket.url)
        pubsub = redis.pubsub()

        try:
            await pubsub.subscribe(channel)
            async for message in pubsub.listen():
                if message["type"] == "message":
                    yield json.loads(message["data"])
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
            await redis.aclose()


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
        when: datetime,
        key: str,
        attempt: int,
        trace_context: opentelemetry.context.Context | None = None,
        redelivered: bool = False,
    ) -> None:
        self.docket = docket
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.when = when
        self.key = key
        self.attempt = attempt
        self.trace_context = trace_context
        self.redelivered = redelivered
        self.state: ExecutionState = ExecutionState.SCHEDULED
        self.progress: ExecutionProgress = ExecutionProgress(docket, key)
        self._redis_key = f"{docket.name}:runs:{key}"

    def as_message(self) -> Message:
        return {
            b"key": self.key.encode(),
            b"when": self.when.isoformat().encode(),
            b"function": self.function.__name__.encode(),
            b"args": cloudpickle.dumps(self.args),  # type: ignore[arg-type]
            b"kwargs": cloudpickle.dumps(self.kwargs),  # type: ignore[arg-type]
            b"attempt": str(self.attempt).encode(),
        }

    @classmethod
    def from_message(
        cls, docket: "Docket", function: TaskFunction, message: Message
    ) -> Self:
        return cls(
            docket=docket,
            function=function,
            args=cloudpickle.loads(message[b"args"]),
            kwargs=cloudpickle.loads(message[b"kwargs"]),
            when=datetime.fromisoformat(message[b"when"].decode()),
            key=message[b"key"].decode(),
            attempt=int(message[b"attempt"].decode()),
            trace_context=propagate.extract(message, getter=message_getter),
            redelivered=False,  # Default to False, will be set to True in worker if it's a redelivery
        )

    def general_labels(self) -> Mapping[str, str]:
        return {"docket.task": self.function.__name__}

    def specific_labels(self) -> Mapping[str, str | int]:
        return {
            "docket.task": self.function.__name__,
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
        function_name = self.function.__name__

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

    async def schedule(self, replace: bool = False) -> None:
        """Schedule this task atomically in Redis.

        This performs an atomic operation that:
        - Adds the task to the stream (immediate) or queue (future)
        - Writes the execution state record
        - Tracks metadata for later cancellation

        Args:
            replace: If True, replaces any existing task with the same key.
                    If False, raises an error if the task already exists.
        """
        message: dict[bytes, bytes] = self.as_message()
        propagate.inject(message, setter=message_setter)

        key = self.key
        when = self.when
        known_task_key = self.docket.known_task_key(key)
        is_immediate = when <= datetime.now(timezone.utc)

        async with self.docket.redis() as redis:
            # Lock per task key to prevent race conditions between concurrent operations
            async with redis.lock(f"{known_task_key}:lock", timeout=10):
                # Register script for this connection (not cached to avoid event loop issues)
                schedule_script = cast(
                    _schedule_task,
                    redis.register_script(
                        # KEYS: stream_key, known_key, parked_key, queue_key, stream_id_key, runs_key
                        # ARGV: task_key, when_timestamp, is_immediate, replace, ...message_fields
                        """
                            local stream_key = KEYS[1]
                            local known_key = KEYS[2]
                            local parked_key = KEYS[3]
                            local queue_key = KEYS[4]
                            local stream_id_key = KEYS[5]
                            local runs_key = KEYS[6]

                            local task_key = ARGV[1]
                            local when_timestamp = ARGV[2]
                            local is_immediate = ARGV[3] == '1'
                            local replace = ARGV[4] == '1'

                            -- Extract message fields from ARGV[5] onwards
                            local message = {}
                            for i = 5, #ARGV, 2 do
                                message[#message + 1] = ARGV[i]     -- field name
                                message[#message + 1] = ARGV[i + 1] -- field value
                            end

                            -- Handle replacement: cancel existing task if needed
                            if replace then
                                local existing_message_id = redis.call('GET', stream_id_key)
                                if existing_message_id then
                                    redis.call('XDEL', stream_key, existing_message_id)
                                end
                                redis.call('DEL', known_key, parked_key, stream_id_key)
                                redis.call('ZREM', queue_key, task_key)
                            else
                                -- Check if task already exists
                                if redis.call('EXISTS', known_key) == 1 then
                                    return 'EXISTS'
                                end
                            end

                            if is_immediate then
                                -- Add to stream and store message ID for later cancellation
                                local message_id = redis.call('XADD', stream_key, '*', unpack(message))
                                redis.call('SET', known_key, when_timestamp)
                                redis.call('SET', stream_id_key, message_id)
                                -- Set run state to queued (ready for workers to claim)
                                redis.call('HSET', runs_key, 'state', 'queued', 'when', when_timestamp)
                            else
                                -- Add to queue with task data in parked hash
                                redis.call('SET', known_key, when_timestamp)
                                redis.call('HSET', parked_key, unpack(message))
                                redis.call('ZADD', queue_key, when_timestamp, task_key)
                                -- Set run state to scheduled (waiting for due time)
                                redis.call('HSET', runs_key, 'state', 'scheduled', 'when', when_timestamp)
                            end

                            return 'OK'
                            """
                    ),
                )

                await schedule_script(
                    keys=[
                        self.docket.stream_key,
                        known_task_key,
                        self.docket.parked_task_key(key),
                        self.docket.queue_key,
                        self.docket.stream_id_key(key),
                        self._redis_key,
                    ],
                    args=[
                        key,
                        str(when.timestamp()),
                        "1" if is_immediate else "0",
                        "1" if replace else "0",
                        *[
                            item
                            for field, value in message.items()
                            for item in (field, value)
                        ],
                    ],
                )

        # Update local state based on whether task is immediate or scheduled
        if is_immediate:
            self.state = ExecutionState.QUEUED
            await self._publish_state(
                {"state": ExecutionState.QUEUED.value, "when": when.isoformat()}
            )
        else:
            self.state = ExecutionState.SCHEDULED
            await self._publish_state(
                {"state": ExecutionState.SCHEDULED.value, "when": when.isoformat()}
            )

    async def set_running(self, worker: str) -> None:
        """Mark task as running.

        Args:
            worker: Name of the worker executing the task
        """
        started_at = datetime.now(timezone.utc).isoformat()
        async with self.docket.redis() as redis:
            await redis.hset(
                self._redis_key,
                mapping={
                    "state": ExecutionState.RUNNING.value,
                    "worker": worker,
                    "started_at": started_at,
                },
            )
            # Initialize progress current to 0
            await redis.hset(self.progress._redis_key, "current", "0")
        self.state = ExecutionState.RUNNING
        # Publish state change event
        await self._publish_state(
            {
                "state": ExecutionState.RUNNING.value,
                "worker": worker,
                "started_at": started_at,
            }
        )

    async def set_completed(self) -> None:
        """Mark task as completed successfully.

        Sets 1-hour TTL on state data and deletes progress data.
        """
        completed_at = datetime.now(timezone.utc).isoformat()
        async with self.docket.redis() as redis:
            await redis.hset(
                self._redis_key,
                mapping={
                    "state": ExecutionState.COMPLETED.value,
                    "completed_at": completed_at,
                },
            )
            # Set 1 hour TTL
            await redis.expire(self._redis_key, 3600)
        self.state = ExecutionState.COMPLETED
        # Delete progress data
        await self.progress._delete()
        # Publish state change event
        await self._publish_state(
            {"state": ExecutionState.COMPLETED.value, "completed_at": completed_at}
        )

    async def set_failed(self, error: str | None = None) -> None:
        """Mark task as failed.

        Args:
            error: Optional error message describing the failure

        Sets 1-hour TTL on state data and deletes progress data.
        """
        completed_at = datetime.now(timezone.utc).isoformat()
        async with self.docket.redis() as redis:
            mapping = {
                "state": ExecutionState.FAILED.value,
                "completed_at": completed_at,
            }
            if error:
                mapping["error"] = error
            await redis.hset(self._redis_key, mapping=mapping)
            # Set 1 hour TTL
            await redis.expire(self._redis_key, 3600)
        self.state = ExecutionState.FAILED
        # Delete progress data
        await self.progress._delete()
        # Publish state change event
        state_data = {
            "state": ExecutionState.FAILED.value,
            "completed_at": completed_at,
        }
        if error:
            state_data["error"] = error
        await self._publish_state(state_data)

    async def get_state(self) -> ExecutionState | None:
        """Retrieve the current execution state.

        Returns:
            The current ExecutionState, or None if no state data exists
        """
        async with self.docket.redis() as redis:
            state_value = await redis.hget(self._redis_key, "state")
            if state_value:
                # Decode bytes to string if necessary
                if isinstance(state_value, bytes):
                    state_value = state_value.decode()
                return ExecutionState(state_value)
            return None

    async def _publish_state(self, data: dict) -> None:
        """Publish state change to Redis pub/sub channel.

        Args:
            data: State data to publish
        """
        # Skip pub/sub for memory:// backend
        if self.docket.url.startswith("memory://"):
            return

        channel = f"{self.docket.name}:state:{self.key}"
        # Create ephemeral Redis client for publishing
        redis = Redis.from_url(self.docket.url)
        try:
            # Build payload with all relevant state information
            payload = {
                "type": "state",
                "key": self.key,
                **data,  # Include all state fields from caller
            }

            # Publish JSON payload
            await redis.publish(channel, json.dumps(payload))
        finally:
            await redis.aclose()

    async def subscribe(self) -> AsyncGenerator[dict[str, Any], None]:
        """Subscribe to both state and progress updates for this task.

        Yields:
            Dict containing state or progress update events with a 'type' field:
            - For state events: type="state", state, worker, timestamps, error
            - For progress events: type="progress", current, total, message, updated_at
        """
        state_channel = f"{self.docket.name}:state:{self.key}"
        progress_channel = f"{self.docket.name}:progress:{self.key}"
        redis = Redis.from_url(self.docket.url)
        pubsub = redis.pubsub()

        try:
            # Subscribe to both channels
            await pubsub.subscribe(state_channel, progress_channel)
            async for message in pubsub.listen():
                if message["type"] == "message":
                    yield json.loads(message["data"])
        finally:
            await pubsub.unsubscribe(state_channel, progress_channel)
            await pubsub.aclose()
            await redis.aclose()


def compact_signature(signature: inspect.Signature) -> str:
    from .dependencies import Dependency

    parameters: list[str] = []
    dependencies: int = 0

    for parameter in signature.parameters.values():
        if isinstance(parameter.default, Dependency):
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


class Operator(str, enum.Enum):
    EQUAL = "=="
    NOT_EQUAL = "!="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    BETWEEN = "between"


LiteralOperator = Literal["==", "!=", ">", ">=", "<", "<=", "between"]


class StrikeInstruction(abc.ABC):
    direction: Literal["strike", "restore"]
    operator: Operator

    def __init__(
        self,
        function: str | None,
        parameter: str | None,
        operator: Operator,
        value: Hashable,
    ) -> None:
        self.function = function
        self.parameter = parameter
        self.operator = operator
        self.value = value

    def as_message(self) -> Message:
        message: dict[bytes, bytes] = {b"direction": self.direction.encode()}
        if self.function:
            message[b"function"] = self.function.encode()
        if self.parameter:
            message[b"parameter"] = self.parameter.encode()
        message[b"operator"] = self.operator.encode()
        message[b"value"] = cloudpickle.dumps(self.value)  # type: ignore[arg-type]
        return message

    @classmethod
    def from_message(cls, message: Message) -> "StrikeInstruction":
        direction = cast(Literal["strike", "restore"], message[b"direction"].decode())
        function = message[b"function"].decode() if b"function" in message else None
        parameter = message[b"parameter"].decode() if b"parameter" in message else None
        operator = cast(Operator, message[b"operator"].decode())
        value = cloudpickle.loads(message[b"value"])
        if direction == "strike":
            return Strike(function, parameter, operator, value)
        else:
            return Restore(function, parameter, operator, value)

    def labels(self) -> Mapping[str, str]:
        labels: dict[str, str] = {}
        if self.function:
            labels["docket.task"] = self.function

        if self.parameter:
            labels["docket.parameter"] = self.parameter
            labels["docket.operator"] = self.operator
            labels["docket.value"] = repr(self.value)

        return labels

    def call_repr(self) -> str:
        return (
            f"{self.function or '*'}"
            "("
            f"{self.parameter or '*'}"
            " "
            f"{self.operator}"
            " "
            f"{repr(self.value) if self.parameter else '*'}"
            ")"
        )


class Strike(StrikeInstruction):
    direction: Literal["strike", "restore"] = "strike"


class Restore(StrikeInstruction):
    direction: Literal["strike", "restore"] = "restore"


MinimalStrike = tuple[Operator, Hashable]
ParameterStrikes = dict[str, set[MinimalStrike]]
TaskStrikes = dict[str, ParameterStrikes]


class StrikeList:
    task_strikes: TaskStrikes
    parameter_strikes: ParameterStrikes
    _conditions: list[Callable[[Execution], bool]]

    def __init__(self) -> None:
        self.task_strikes = {}
        self.parameter_strikes = {}
        self._conditions = [self._matches_task_or_parameter_strike]

    def add_condition(self, condition: Callable[[Execution], bool]) -> None:
        """Adds a temporary condition that indicates an execution is stricken."""
        self._conditions.insert(0, condition)

    def remove_condition(self, condition: Callable[[Execution], bool]) -> None:
        """Adds a temporary condition that indicates an execution is stricken."""
        assert condition is not self._matches_task_or_parameter_strike
        self._conditions.remove(condition)

    def is_stricken(self, execution: Execution) -> bool:
        """
        Checks if an execution is stricken based on task, parameter, or temporary
        conditions.

        Returns:
            bool: True if the execution is stricken, False otherwise.
        """
        return any(condition(execution) for condition in self._conditions)

    def _matches_task_or_parameter_strike(self, execution: Execution) -> bool:
        function_name = execution.function.__name__

        # Check if the entire task is stricken (without parameter conditions)
        task_strikes = self.task_strikes.get(function_name, {})
        if function_name in self.task_strikes and not task_strikes:
            return True

        signature = get_signature(execution.function)

        try:
            bound_args = signature.bind(*execution.args, **execution.kwargs)
            bound_args.apply_defaults()
        except TypeError:
            # If we can't make sense of the arguments, just assume the task is fine
            return False

        all_arguments = {
            **bound_args.arguments,
            **{
                k: v
                for k, v in execution.kwargs.items()
                if k not in bound_args.arguments
            },
        }

        for parameter, argument in all_arguments.items():
            for strike_source in [task_strikes, self.parameter_strikes]:
                if parameter not in strike_source:
                    continue

                for operator, strike_value in strike_source[parameter]:
                    if self._is_match(argument, operator, strike_value):
                        return True

        return False

    def _is_match(self, value: Any, operator: Operator, strike_value: Any) -> bool:
        """Determines if a value matches a strike condition."""
        try:
            match operator:
                case "==":
                    return value == strike_value
                case "!=":
                    return value != strike_value
                case ">":
                    return value > strike_value
                case ">=":
                    return value >= strike_value
                case "<":
                    return value < strike_value
                case "<=":
                    return value <= strike_value
                case "between":  # pragma: no branch
                    lower, upper = strike_value
                    return lower <= value <= upper
                case _:  # pragma: no cover
                    raise ValueError(f"Unknown operator: {operator}")
        except (ValueError, TypeError):
            # If we can't make the comparison due to incompatible types, just log the
            # error and assume the task is not stricken
            logger.warning(
                "Incompatible type for strike condition: %r %s %r",
                strike_value,
                operator,
                value,
                exc_info=True,
            )
            return False

    def update(self, instruction: StrikeInstruction) -> None:
        try:
            hash(instruction.value)
        except TypeError:
            logger.warning(
                "Incompatible type for strike condition: %s %r",
                instruction.operator,
                instruction.value,
            )
            return

        if isinstance(instruction, Strike):
            self._strike(instruction)
        elif isinstance(instruction, Restore):  # pragma: no branch
            self._restore(instruction)

    def _strike(self, strike: Strike) -> None:
        if strike.function and strike.parameter:
            try:
                task_strikes = self.task_strikes[strike.function]
            except KeyError:
                task_strikes = self.task_strikes[strike.function] = {}

            try:
                parameter_strikes = task_strikes[strike.parameter]
            except KeyError:
                parameter_strikes = task_strikes[strike.parameter] = set()

            parameter_strikes.add((strike.operator, strike.value))

        elif strike.function:
            try:
                task_strikes = self.task_strikes[strike.function]
            except KeyError:
                task_strikes = self.task_strikes[strike.function] = {}

        elif strike.parameter:  # pragma: no branch
            try:
                parameter_strikes = self.parameter_strikes[strike.parameter]
            except KeyError:
                parameter_strikes = self.parameter_strikes[strike.parameter] = set()

            parameter_strikes.add((strike.operator, strike.value))

    def _restore(self, restore: Restore) -> None:
        if restore.function and restore.parameter:
            try:
                task_strikes = self.task_strikes[restore.function]
            except KeyError:
                return

            try:
                parameter_strikes = task_strikes[restore.parameter]
            except KeyError:
                task_strikes.pop(restore.parameter, None)
                return

            try:
                parameter_strikes.remove((restore.operator, restore.value))
            except KeyError:
                pass

            if not parameter_strikes:
                task_strikes.pop(restore.parameter, None)
                if not task_strikes:
                    self.task_strikes.pop(restore.function, None)

        elif restore.function:
            try:
                task_strikes = self.task_strikes[restore.function]
            except KeyError:
                return

            # If there are no parameter strikes, this was a full task strike
            if not task_strikes:
                self.task_strikes.pop(restore.function, None)

        elif restore.parameter:  # pragma: no branch
            try:
                parameter_strikes = self.parameter_strikes[restore.parameter]
            except KeyError:
                return

            try:
                parameter_strikes.remove((restore.operator, restore.value))
            except KeyError:
                pass

            if not parameter_strikes:
                self.parameter_strikes.pop(restore.parameter, None)
