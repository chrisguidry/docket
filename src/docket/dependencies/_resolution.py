"""Dependency resolution helpers and context manager."""

from __future__ import annotations

from contextlib import AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, Counter, TypeVar

from ._base import Dependency
from ._contextual import _TaskArgument
from ._depends import _Depends, get_dependency_parameters

if TYPE_CHECKING:  # pragma: no cover
    from ..execution import Execution, TaskFunction
    from ..worker import Worker

D = TypeVar("D", bound=Dependency)


def get_single_dependency_parameter_of_type(
    function: TaskFunction, dependency_type: type[D]
) -> D | None:
    assert dependency_type.single, "Dependency must be single"
    for _, dependency in get_dependency_parameters(function).items():
        if isinstance(dependency, dependency_type):
            return dependency
    return None


def get_single_dependency_of_type(
    dependencies: dict[str, Dependency], dependency_type: type[D]
) -> D | None:
    assert dependency_type.single, "Dependency must be single"
    for _, dependency in dependencies.items():
        if isinstance(dependency, dependency_type):
            return dependency
    return None


def validate_dependencies(function: TaskFunction) -> None:
    parameters = get_dependency_parameters(function)

    counts = Counter(type(dependency) for dependency in parameters.values())

    for dependency_type, count in counts.items():
        if dependency_type.single and count > 1:
            raise ValueError(
                f"Only one {dependency_type.__name__} dependency is allowed per task"
            )


class FailedDependency:
    def __init__(self, parameter: str, error: Exception) -> None:
        self.parameter = parameter
        self.error = error


@asynccontextmanager
async def resolved_dependencies(
    worker: Worker, execution: Execution
) -> AsyncGenerator[dict[str, Any], None]:
    # Capture tokens for all contextvar sets to ensure proper cleanup
    docket_token = Dependency.docket.set(worker.docket)
    worker_token = Dependency.worker.set(worker)
    execution_token = Dependency.execution.set(execution)
    cache_token = _Depends.cache.set({})

    try:
        async with AsyncExitStack() as stack:
            stack_token = _Depends.stack.set(stack)
            try:
                arguments: dict[str, Any] = {}

                parameters = get_dependency_parameters(execution.function)
                for parameter, dependency in parameters.items():
                    kwargs = execution.kwargs
                    if parameter in kwargs:
                        arguments[parameter] = kwargs[parameter]
                        continue

                    # Special case for TaskArguments, they are "magical" and infer the parameter
                    # they refer to from the parameter name (unless otherwise specified).  At
                    # the top-level task function call, it doesn't make sense to specify one
                    # _without_ a parameter name, so we'll call that a failed dependency.
                    if (
                        isinstance(dependency, _TaskArgument)
                        and not dependency.parameter
                    ):
                        arguments[parameter] = FailedDependency(
                            parameter, ValueError("No parameter name specified")
                        )
                        continue

                    try:
                        arguments[parameter] = await stack.enter_async_context(
                            dependency
                        )
                    except Exception as error:
                        arguments[parameter] = FailedDependency(parameter, error)

                yield arguments
            finally:
                _Depends.stack.reset(stack_token)
    finally:
        _Depends.cache.reset(cache_token)
        Dependency.execution.reset(execution_token)
        Dependency.worker.reset(worker_token)
        Dependency.docket.reset(docket_token)
