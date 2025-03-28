import abc
import inspect
from typing import Any, Iterable, Mapping, Self


class Annotation(abc.ABC):
    _cache: dict[tuple[type[Self], inspect.Signature], Mapping[str, Self]] = {}

    @classmethod
    def annotated_parameters(cls, signature: inspect.Signature) -> Mapping[str, Self]:
        key = (cls, signature)
        if key in cls._cache:
            return cls._cache[key]

        annotated: dict[str, Self] = {}

        for param_name, param in signature.parameters.items():
            if param.annotation == inspect.Parameter.empty:
                continue

            try:
                metadata: Iterable[Any] = param.annotation.__metadata__
            except AttributeError:
                continue

            for arg_type in metadata:
                if isinstance(arg_type, cls):
                    annotated[param_name] = arg_type
                elif isinstance(arg_type, type) and issubclass(arg_type, cls):
                    annotated[param_name] = arg_type()

        cls._cache[key] = annotated
        return annotated


class Logged(Annotation):
    """Instructs docket to include arguments to this parameter in the log."""

    length_only: bool = False

    def __init__(self, length_only: bool = False) -> None:
        self.length_only = length_only

    def format(self, argument: Any) -> str:
        if self.length_only:
            if isinstance(argument, (dict, set)):
                return f"{{len {len(argument)}}}"
            elif isinstance(argument, tuple):
                return f"(len {len(argument)})"
            elif hasattr(argument, "__len__"):
                return f"[len {len(argument)}]"

        return repr(argument)
