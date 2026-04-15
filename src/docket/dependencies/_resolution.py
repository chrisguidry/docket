"""Dependency resolution helpers and context manager."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, TypeVar

from uncalled_for import (
    FailedDependency,
    get_annotation_dependencies,
)

from ._base import (
    Dependency,
    current_docket,
    current_execution,
    current_worker,
)
from ._contextual import _TaskArgument
from ._functional import _Depends, get_dependency_parameters


def _single_bases(cls: type) -> list[type[Dependency[Any]]]:
    """``Dependency`` ancestors of ``cls`` (inherited or declared) with
    ``single=True``.  Mirrors ``uncalled_for.validate_dependencies``.
    """
    return [
        base
        for base in cls.__mro__
        if base is not Dependency
        and issubclass(base, Dependency)
        and getattr(base, "single", False)
    ]


def validate_worker_dependencies(
    dependencies: Mapping[str, Any] | Sequence[Any] | None,
) -> dict[str, Dependency[Any]]:
    if not dependencies:
        return {}
    if isinstance(dependencies, Mapping):
        items: list[tuple[str | None, Dependency[Any]]] = [
            (name, dependency) for name, dependency in dependencies.items()
        ]
    else:
        items = [(None, dependency) for dependency in dependencies]

    validated: dict[str, Dependency[Any]] = {}
    for index, (given_name, dependency) in enumerate(items):
        if given_name is None:
            name = f"__worker_dep_{index}__"
        else:
            if given_name.startswith("__"):
                raise ValueError(
                    f"Worker dependency name {given_name!r} is reserved; "
                    "names starting with '__' are not allowed."
                )
            name = given_name
        if not isinstance(dependency, Dependency):
            label = given_name if given_name is not None else f"index {index}"
            raise TypeError(
                f"Worker dependency {label!r} must be a Dependency instance "
                f"(e.g. Depends(fn) or Retry(...)), got "
                f"{type(dependency).__name__}."
            )
        validated[name] = dependency
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
    """Detect ``single=True`` conflicts across worker- and task-scope
    dependencies.  Mirrors ``uncalled_for.validate_dependencies``: check
    concrete-type duplicates first so errors name the exact type (e.g.
    ``Retry``), then cross-subclass conflicts under a shared single base
    (e.g. ``Timeout`` + a custom ``Runtime``).
    """
    items: list[tuple[str, Dependency[Any]]] = [
        (key, value)
        for key, value in arguments.items()
        if isinstance(value, Dependency)
    ]
    for parameter_name, deps in annotations.items():
        for dependency in deps:
            items.append((parameter_name, dependency))

    conflicts: dict[str, FailedDependency] = {}
    reported: set[type[Dependency[Any]]] = set()

    by_type: dict[type[Dependency[Any]], list[str]] = {}
    for key, dependency in items:
        by_type.setdefault(type(dependency), []).append(key)
    for concrete, keys in by_type.items():
        if getattr(concrete, "single", False) and len(keys) > 1:
            conflict_key = f"__conflict_{concrete.__name__}__"
            described = ", ".join(repr(k) for k in keys)
            conflicts[conflict_key] = FailedDependency(
                conflict_key,
                ValueError(
                    f"Only one {concrete.__name__} dependency is allowed, "
                    f"but found at: {described}. "
                    "Declare it in exactly one place (task or worker)."
                ),
            )
            reported.add(concrete)

    bases: set[type[Dependency[Any]]] = set()
    for _, dependency in items:
        bases.update(_single_bases(type(dependency)))
    for base in bases:
        if base in reported:
            continue
        matches = [(key, type(dep)) for key, dep in items if isinstance(dep, base)]
        if len({cls for _, cls in matches}) > 1:
            conflict_key = f"__conflict_{base.__name__}__"
            described = ", ".join(f"{key!r} ({cls.__name__})" for key, cls in matches)
            conflicts[conflict_key] = FailedDependency(
                conflict_key,
                ValueError(
                    f"Only one {base.__name__} dependency is allowed, "
                    f"but found: {described}. "
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

                for name, dependency in worker.dependencies.items():
                    slot = f"__worker_dep__{name}"
                    try:
                        arguments[slot] = await stack.enter_async_context(dependency)
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
