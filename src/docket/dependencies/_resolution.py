"""Dependency resolution helpers and context manager."""

from __future__ import annotations

from contextlib import AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, TypeVar

from uncalled_for import (
    FailedDependency as FailedDependency,
    get_annotation_dependencies as get_annotation_dependencies,
    validate_dependencies as validate_dependencies,
)

from ._base import Dependency, current_docket, current_execution, current_worker
from ._contextual import _TaskArgument
from ._functional import _Depends, get_dependency_parameters

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
    for _, deps in get_annotation_dependencies(function).items():
        for dependency in deps:
            if isinstance(dependency, dependency_type):
                return dependency  # type: ignore[return-value]
    return None


def get_single_dependency_of_type(
    dependencies: dict[str, Dependency[Any]], dependency_type: type[D]
) -> D | None:
    assert dependency_type.single, "Dependency must be single"
    for _, dependency in dependencies.items():
        if isinstance(dependency, dependency_type):
            return dependency
    return None


@asynccontextmanager
async def resolved_dependencies(
    worker: Worker, execution: Execution
) -> AsyncGenerator[dict[str, Any], None]:
    docket_token = current_docket.set(worker.docket)
    worker_token = current_worker.set(worker)
    execution_token = current_execution.set(execution)
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

                    # At the top-level task function call, a bare TaskArgument without
                    # a parameter name doesn't make sense, so mark it as failed.
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

                annotation_deps = get_annotation_dependencies(execution.function)
                for param_name, deps in annotation_deps.items():
                    value = execution.kwargs.get(param_name, arguments.get(param_name))
                    for dep in deps:
                        bound = dep.bind_to_parameter(param_name, value)
                        try:
                            await stack.enter_async_context(bound)
                        except Exception as error:
                            arguments[param_name] = FailedDependency(param_name, error)

                yield arguments
            finally:
                _Depends.stack.reset(stack_token)
    finally:
        _Depends.cache.reset(cache_token)
        current_execution.reset(execution_token)
        current_worker.reset(worker_token)
        current_docket.reset(docket_token)
