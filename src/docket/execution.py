from datetime import datetime
from typing import Any, Awaitable, Callable, Self

import cloudpickle

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
