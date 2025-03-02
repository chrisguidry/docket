import inspect
from datetime import datetime
from typing import Any, Awaitable, Callable, Self

import cloudpickle

from docket.annotations import Logged

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
            b"args": cloudpickle.dumps(self.args),
            b"kwargs": cloudpickle.dumps(self.kwargs),
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
