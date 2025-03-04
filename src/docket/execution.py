import abc
import enum
import inspect
import logging
from datetime import datetime
from typing import Any, Awaitable, Callable, Hashable, Literal, Self, cast

import cloudpickle  # type: ignore[import]


from .annotations import Logged

logger: logging.Logger = logging.getLogger(__name__)

Message = dict[bytes, bytes]


class Execution:
    def __init__(
        self,
        function: Callable[..., Awaitable[Any]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        when: datetime,
        key: str,
        attempt: int,
    ) -> None:
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.when = when
        self.key = key
        self.attempt = attempt

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
        cls, function: Callable[..., Awaitable[Any]], message: Message
    ) -> Self:
        return cls(
            function=function,
            args=cloudpickle.loads(message[b"args"]),
            kwargs=cloudpickle.loads(message[b"kwargs"]),
            when=datetime.fromisoformat(message[b"when"].decode()),
            key=message[b"key"].decode(),
            attempt=int(message[b"attempt"].decode()),
        )

    def call_repr(self) -> str:
        arguments: list[str] = []
        signature = inspect.signature(self.function)
        function_name = self.function.__name__

        logged_parameters = Logged.annotated_parameters(signature)

        parameter_names = list(signature.parameters.keys())

        for i, argument in enumerate(self.args[: len(parameter_names)]):
            parameter_name = parameter_names[i]
            if parameter_name in logged_parameters:
                arguments.append(repr(argument))
            else:
                arguments.append("...")

        for parameter_name, argument in self.kwargs.items():
            if parameter_name in logged_parameters:
                arguments.append(f"{parameter_name}={repr(argument)}")
            else:
                arguments.append(f"{parameter_name}=...")

        return f"{function_name}({', '.join(arguments)}){{{self.key}}}"


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

    def as_span_attributes(self) -> dict[str, str]:
        span_attributes: dict[str, str] = {}
        if self.function:
            span_attributes["docket.function"] = self.function

        if self.parameter:
            span_attributes["docket.parameter"] = self.parameter
            span_attributes["docket.operator"] = self.operator
            span_attributes["docket.value"] = repr(self.value)

        return span_attributes

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

    def __init__(self) -> None:
        self.task_strikes = {}
        self.parameter_strikes = {}

    def is_stricken(self, execution: Execution) -> bool:
        """
        Checks if an execution is stricken based on task name or parameter values.

        Returns:
            bool: True if the execution is stricken, False otherwise.
        """
        function_name = execution.function.__name__

        # Check if the entire task is stricken (without parameter conditions)
        task_strikes = self.task_strikes.get(function_name, {})
        if function_name in self.task_strikes and not task_strikes:
            return True

        sig = inspect.signature(execution.function)

        try:
            bound_args = sig.bind(*execution.args, **execution.kwargs)
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
