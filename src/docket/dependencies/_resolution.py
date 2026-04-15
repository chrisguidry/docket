"""Dependency resolution helpers and context manager."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, TypeVar

from uncalled_for import (
    DependencyFactory,
    FailedDependency,
    get_annotation_dependencies,
)

from ._base import (
    CompletionHandler,
    Dependency,
    FailureHandler,
    Runtime,
    current_docket,
    current_execution,
    current_worker,
)
from ._concurrency import ConcurrencyLimit
from ._contextual import _TaskArgument
from ._debounce import Debounce
from ._functional import _Depends, get_dependency_parameters

SINGLE_DEPENDENCY_TYPES: tuple[type[Dependency[Any]], ...] = (
    Runtime,
    FailureHandler,
    CompletionHandler,
    ConcurrencyLimit,
    Debounce,
)


def validate_worker_dependencies(
    dependencies: (
        Mapping[str, DependencyFactory[Any]] | Sequence[DependencyFactory[Any]] | None
    ),
) -> dict[str, DependencyFactory[Any]]:
    if not dependencies:
        return {}
    if isinstance(dependencies, Mapping):
        items: list[tuple[str | None, DependencyFactory[Any]]] = [
            (name, factory) for name, factory in dependencies.items()
        ]
    else:
        items = [(None, factory) for factory in dependencies]

    validated: dict[str, DependencyFactory[Any]] = {}
    for index, (given_name, factory) in enumerate(items):
        if not callable(factory):
            label = given_name if given_name is not None else f"index {index}"
            raise TypeError(
                f"Worker dependency {label!r} must be a callable factory, "
                f"got {type(factory).__name__}."
            )
        if given_name is None:
            name = f"__worker_dep_{index}__"
        else:
            if given_name.startswith("__"):
                raise ValueError(
                    f"Worker dependency name {given_name!r} is reserved; "
                    "names starting with '__' are not allowed."
                )
            name = given_name
        validated[name] = factory
    return validated


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
    for _, dependencies in get_annotation_dependencies(function).items():
        for dependency in dependencies:
            if isinstance(dependency, dependency_type):
                return dependency
    return None


def get_single_dependency_of_type(
    dependencies: dict[str, Dependency[Any]], dependency_type: type[D]
) -> D | None:
    assert dependency_type.single, "Dependency must be single"
    for _, dependency in dependencies.items():
        if isinstance(dependency, dependency_type):
            return dependency
    return None


def detect_single_conflicts(
    arguments: dict[str, Any],
    annotations: Mapping[str, Sequence[Dependency[Any]]],
) -> dict[str, FailedDependency]:
    conflicts: dict[str, FailedDependency] = {}
    for single_type in SINGLE_DEPENDENCY_TYPES:
        matches: list[tuple[str, type[Dependency[Any]]]] = []
        for key, resolved in arguments.items():
            if isinstance(resolved, single_type):
                matches.append((key, type(resolved)))
        for parameter_name, deps in annotations.items():
            for dependency in deps:
                if isinstance(dependency, single_type):
                    matches.append((parameter_name, type(dependency)))
        if len(matches) > 1:
            conflict_key = f"__conflict_{single_type.__name__}__"
            described = ", ".join(f"{key!r} ({cls.__name__})" for key, cls in matches)
            conflicts[conflict_key] = FailedDependency(
                conflict_key,
                ValueError(
                    f"Conflicting single=True {single_type.__name__} "
                    f"dependencies declared in this execution: {described}. "
                    "Declare it in exactly one place (task or worker)."
                ),
            )
    return conflicts


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

                for name, factory in worker.dependencies.items():
                    slot = f"__worker_dep__{name}"
                    try:
                        arguments[slot] = await stack.enter_async_context(
                            _Depends(factory)
                        )
                    except Exception as error:
                        arguments[slot] = FailedDependency(name, error)

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

                annotations = get_annotation_dependencies(execution.function)
                for parameter_name, dependencies in annotations.items():
                    argument_value = execution.kwargs.get(
                        parameter_name, arguments.get(parameter_name)
                    )
                    for dependency in dependencies:
                        bound = dependency.bind_to_parameter(
                            parameter_name, argument_value
                        )
                        try:
                            await stack.enter_async_context(bound)
                        except Exception as error:
                            arguments[parameter_name] = FailedDependency(
                                parameter_name, error
                            )

                arguments.update(
                    worker.validate_task_dependencies(
                        execution.function, arguments, annotations
                    )
                )

                yield arguments
            finally:
                _Depends.stack.reset(stack_token)
    finally:
        _Depends.cache.reset(cache_token)
        current_execution.reset(execution_token)
        current_worker.reset(worker_token)
        current_docket.reset(docket_token)
