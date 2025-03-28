import abc
import enum
import inspect
import logging
from datetime import datetime
from typing import Any, Awaitable, Callable, Hashable, Literal, Mapping, Self, cast

import cloudpickle  # type: ignore[import]

from opentelemetry import trace, propagate
import opentelemetry.context

from .annotations import Logged
from docket.instrumentation import message_getter

logger: logging.Logger = logging.getLogger(__name__)

TaskFunction = Callable[..., Awaitable[Any]]
Message = dict[bytes, bytes]


_signature_cache: dict[Callable[..., Any], inspect.Signature] = {}


def get_signature(function: Callable[..., Any]) -> inspect.Signature:
    if function in _signature_cache:
        return _signature_cache[function]

    signature = inspect.signature(function)
    _signature_cache[function] = signature
    return signature


class Execution:
    def __init__(
        self,
        function: TaskFunction,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        when: datetime,
        key: str,
        attempt: int,
        trace_context: opentelemetry.context.Context | None = None,
    ) -> None:
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.when = when
        self.key = key
        self.attempt = attempt
        self.trace_context = trace_context

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
    def from_message(cls, function: TaskFunction, message: Message) -> Self:
        return cls(
            function=function,
            args=cloudpickle.loads(message[b"args"]),
            kwargs=cloudpickle.loads(message[b"kwargs"]),
            when=datetime.fromisoformat(message[b"when"].decode()),
            key=message[b"key"].decode(),
            attempt=int(message[b"attempt"].decode()),
            trace_context=propagate.extract(message, getter=message_getter),
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


class Operator(enum.StrEnum):
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
