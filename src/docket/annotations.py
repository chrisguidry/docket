import abc
import inspect
from typing import Any, Iterable, Mapping, Self


class Annotation(abc.ABC):
    @classmethod
    def annotated_parameters(cls, signature: inspect.Signature) -> Mapping[str, Self]:
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

        return annotated


class Logged(Annotation):
    """Instructs docket to include arguments to this parameter in the log."""
