"""Tests for Annotated-style dependency injection.

Annotated dependencies allow attaching Dependency instances as type-hint
metadata instead of as default parameter values:

    async def process(customer_id: Annotated[int, ConcurrencyLimit(1)]): ...

The parameter keeps its real value (the int); the dependency runs as a
side-effect (acquiring the concurrency slot).
"""

from __future__ import annotations

from typing import Annotated

from docket import ConcurrencyLimit, Docket, Worker
from docket.dependencies import Depends, get_single_dependency_parameter_of_type

_side_effects: list[str] = []


async def my_side_effect() -> str:
    _side_effects.append("side_effect_ran")
    return "unused"


async def test_annotated_depends_side_effect(docket: Docket, worker: Worker):
    """Annotated Depends runs as a side-effect (doesn't produce the param value)."""
    _side_effects.clear()

    async def task(
        customer_id: int,
        _setup: Annotated[str, Depends(my_side_effect)] = "",
    ):
        _side_effects.append(f"task_{customer_id}")

    await docket.add(task)(customer_id=1)
    await worker.run_until_finished()

    assert "side_effect_ran" in _side_effects
    assert "task_1" in _side_effects


async def test_mixed_default_and_annotation_deps(docket: Docket, worker: Worker):
    """Default-param and annotation dependencies coexist on the same function."""
    results: list[str] = []

    async def get_label() -> str:
        return "labeled"

    async def task(
        customer_id: Annotated[int, ConcurrencyLimit(1)],
        label: str = Depends(get_label),
    ):
        results.append(f"{label}_{customer_id}")

    await docket.add(task)(customer_id=42)
    await worker.run_until_finished()

    assert results == ["labeled_42"]


CustomerId = Annotated[int, ConcurrencyLimit(1)]


async def test_type_alias(docket: Docket, worker: Worker):
    """Annotated type aliases work for dependency extraction."""
    results: list[str] = []

    async def task(customer_id: CustomerId):
        results.append(f"done_{customer_id}")

    await docket.add(task)(customer_id=7)
    await worker.run_until_finished()

    assert results == ["done_7"]


def test_get_single_dependency_finds_annotation():
    """get_single_dependency_parameter_of_type finds deps in Annotated metadata."""

    async def task(
        customer_id: Annotated[int, ConcurrencyLimit(1)],
    ): ...  # pragma: no cover

    dep = get_single_dependency_parameter_of_type(task, ConcurrencyLimit)

    assert dep is not None
    assert isinstance(dep, ConcurrencyLimit)
