"""Contract tests for uncalled-for behaviors that docket relies on.

These tests verify that the uncalled-for library provides the specific
API surface and semantics that docket's dependency injection depends on.
If uncalled-for changes behavior, these tests will catch the breakage
before it reaches docket's integration tests.
"""

from __future__ import annotations

from typing import Annotated, Any

import pytest

from uncalled_for import (
    Dependency,
    FailedDependency,
    get_annotation_dependencies,
    get_dependency_parameters,
    validate_dependencies,
)


class StubDependency(Dependency["str"]):
    """Minimal dependency for contract testing."""

    async def __aenter__(self) -> str:  # pragma: no cover
        return "stub"


class SingleDependency(Dependency["str"]):
    """Dependency with single=True."""

    single = True

    async def __aenter__(self) -> str:  # pragma: no cover
        return "single"


class TrackingDependency(Dependency["str"]):
    """Dependency that records bind_to_parameter calls."""

    def __init__(self) -> None:
        self.bound_name: str | None = None
        self.bound_value: Any = None

    def bind_to_parameter(self, name: str, value: Any) -> TrackingDependency:
        copy = TrackingDependency()
        copy.bound_name = name
        copy.bound_value = value
        return copy

    async def __aenter__(self) -> str:  # pragma: no cover
        return f"bound:{self.bound_name}={self.bound_value}"


# --- get_annotation_dependencies ---


def test_extracts_dependency_from_annotated():
    async def func(x: Annotated[int, StubDependency()]) -> None: ...

    deps = get_annotation_dependencies(func)

    assert "x" in deps
    assert len(deps["x"]) == 1
    assert isinstance(deps["x"][0], StubDependency)


def test_ignores_plain_type_hints():
    async def func(x: int, y: str) -> None: ...

    deps = get_annotation_dependencies(func)

    assert deps == {}


def test_ignores_non_dependency_annotated_metadata():
    async def func(x: Annotated[int, "not a dependency"]) -> None: ...

    deps = get_annotation_dependencies(func)

    assert deps == {}


def test_extracts_multiple_dependencies_from_one_parameter():
    async def func(
        x: Annotated[int, StubDependency(), TrackingDependency()],
    ) -> None: ...

    deps = get_annotation_dependencies(func)

    assert "x" in deps
    assert len(deps["x"]) == 2


def test_extracts_from_multiple_parameters():
    async def func(
        a: Annotated[int, StubDependency()],
        b: Annotated[str, TrackingDependency()],
    ) -> None: ...

    deps = get_annotation_dependencies(func)

    assert "a" in deps
    assert "b" in deps


# --- get_dependency_parameters ---


def test_returns_default_parameter_dependencies():
    async def func(x: str = StubDependency()) -> None:  # type: ignore[assignment]
        ...

    params = get_dependency_parameters(func)

    assert "x" in params
    assert isinstance(params["x"], StubDependency)


def test_does_not_return_annotation_dependencies():
    async def func(x: Annotated[int, StubDependency()]) -> None: ...

    params = get_dependency_parameters(func)

    assert params == {}


# --- bind_to_parameter ---


def test_default_bind_returns_self():
    dep = StubDependency()
    bound = dep.bind_to_parameter("x", 42)

    assert bound is dep


def test_override_bind_captures_context():
    dep = TrackingDependency()
    bound = dep.bind_to_parameter("customer_id", 42)

    assert isinstance(bound, TrackingDependency)
    assert bound.bound_name == "customer_id"
    assert bound.bound_value == 42
    assert bound is not dep


# --- validate_dependencies ---


def test_validates_single_conflict_in_defaults():
    with pytest.raises(ValueError, match="Only one SingleDependency"):

        async def func(
            a: str = SingleDependency(),  # type: ignore[assignment]
            b: str = SingleDependency(),  # type: ignore[assignment]
        ) -> None: ...

        validate_dependencies(func)


def test_validates_single_conflict_in_annotations():
    with pytest.raises(ValueError, match="Only one SingleDependency"):

        async def func(
            a: Annotated[int, SingleDependency()],
            b: Annotated[int, SingleDependency()],
        ) -> None: ...

        validate_dependencies(func)


def test_validates_single_conflict_across_defaults_and_annotations():
    with pytest.raises(ValueError, match="Only one SingleDependency"):

        async def func(
            a: str = SingleDependency(),  # type: ignore[assignment]
            b: Annotated[int, SingleDependency()] = 0,
        ) -> None: ...

        validate_dependencies(func)


def test_no_conflict_with_non_single_dependencies():
    async def func(
        a: Annotated[int, StubDependency()],
        b: Annotated[int, StubDependency()],
    ) -> None: ...

    validate_dependencies(func)  # should not raise


# --- FailedDependency ---


def test_failed_dependency_captures_parameter_and_error():
    error = ValueError("boom")
    failed = FailedDependency("my_param", error)

    assert failed.parameter == "my_param"
    assert failed.error is error
