"""Tests for Annotated-style dependency injection.

Annotated dependencies allow attaching Dependency instances as type-hint
metadata instead of as default parameter values:

    async def process(customer_id: Annotated[int, ConcurrencyLimit(1)]): ...

The parameter keeps its real value (the int); the dependency runs as a
side-effect (acquiring the concurrency slot).
"""

from __future__ import annotations

import asyncio
import time
from typing import Annotated

import pytest

from docket import ConcurrencyLimit, Docket, Worker
from docket.dependencies import Depends, get_single_dependency_parameter_of_type

from tests.concurrency_limits.overlap import assert_some_overlap

_side_effects: list[str] = []


async def _setup_logging() -> str:
    _side_effects.append("logging_configured")
    return "unused"


# --- ConcurrencyLimit via Annotated ---


async def test_annotated_concurrency_limit(docket: Docket, worker: Worker):
    """Annotated[int, ConcurrencyLimit(1)] limits concurrency per-parameter."""
    results: list[str] = []

    async def task(customer_id: Annotated[int, ConcurrencyLimit(1)]):
        results.append(f"start_{customer_id}")
        await asyncio.sleep(0.01)
        results.append(f"end_{customer_id}")

    await docket.add(task)(customer_id=1)
    await docket.add(task)(customer_id=1)

    await worker.run_until_finished()

    assert results == ["start_1", "end_1", "start_1", "end_1"]


async def test_annotated_concurrency_different_values(docket: Docket, worker: Worker):
    """Different argument values get independent concurrency slots."""
    execution_intervals: dict[int, tuple[float, float]] = {}

    async def task(customer_id: Annotated[int, ConcurrencyLimit(1)]):
        start = time.monotonic()
        await asyncio.sleep(0.1)
        end = time.monotonic()
        execution_intervals[customer_id] = (start, end)

    await docket.add(task)(customer_id=1)
    await docket.add(task)(customer_id=2)
    await docket.add(task)(customer_id=3)

    worker.concurrency = 10
    await worker.run_until_finished()

    assert len(execution_intervals) == 3
    intervals = list(execution_intervals.values())
    assert_some_overlap(intervals, "Different customers should run concurrently")


async def test_annotated_concurrency_max_concurrent(docket: Docket, worker: Worker):
    """max_concurrent>1 allows that many concurrent executions per value."""
    active_tasks: list[int] = []
    max_concurrent_seen = 0
    lock = asyncio.Lock()

    async def task(
        db_name: str,
        task_id: Annotated[int, ConcurrencyLimit("db_name", max_concurrent=2)],
    ):
        nonlocal max_concurrent_seen
        async with lock:
            active_tasks.append(task_id)
            max_concurrent_seen = max(max_concurrent_seen, len(active_tasks))
        await asyncio.sleep(0.1)
        async with lock:
            active_tasks.remove(task_id)

    for i in range(5):
        await docket.add(task)(db_name="postgres", task_id=i)

    worker.concurrency = 10
    await worker.run_until_finished()

    assert max_concurrent_seen <= 2


# --- Depends via Annotated ---


async def test_annotated_depends_side_effect(docket: Docket, worker: Worker):
    """Annotated Depends runs as a side-effect (doesn't produce the param value)."""
    _side_effects.clear()

    async def task(
        customer_id: int,
        _setup: Annotated[str, Depends(_setup_logging)] = "",
    ):
        _side_effects.append(f"task_{customer_id}")

    await docket.add(task)(customer_id=1)
    await worker.run_until_finished()

    assert "logging_configured" in _side_effects
    assert "task_1" in _side_effects


# --- Mixed default-param and annotation deps ---


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


# --- Type alias ---


CustomerId = Annotated[int, ConcurrencyLimit(1)]


async def test_type_alias(docket: Docket, worker: Worker):
    """Annotated type aliases work for dependency extraction."""
    results: list[str] = []

    async def task(customer_id: CustomerId):
        results.append(f"done_{customer_id}")

    await docket.add(task)(customer_id=7)
    await worker.run_until_finished()

    assert results == ["done_7"]


# --- Resolution helpers find annotation deps ---


def test_get_single_dependency_finds_annotation():
    """get_single_dependency_parameter_of_type finds deps in Annotated metadata."""

    async def task(
        customer_id: Annotated[int, ConcurrencyLimit(1)],
    ): ...  # pragma: no cover

    dep = get_single_dependency_parameter_of_type(task, ConcurrencyLimit)

    assert dep is not None
    assert isinstance(dep, ConcurrencyLimit)


# --- Two ConcurrencyLimit annotations rejected (single=True) ---


async def test_two_annotated_concurrency_limits_rejected(docket: Docket):
    """ConcurrencyLimit.single=True prevents two annotations on one function."""
    with pytest.raises(ValueError, match="Only one ConcurrencyLimit"):

        async def task(
            customer_id: Annotated[int, ConcurrencyLimit(1)],
            region: Annotated[str, ConcurrencyLimit(2)],
        ): ...  # pragma: no cover

        await docket.add(task)(customer_id=1, region="us")


# --- Validation ---


async def test_single_conflict_annotation_and_default(docket: Docket):
    """single=True conflict detected across annotation and default-param styles."""
    with pytest.raises(ValueError, match="Only one ConcurrencyLimit"):

        async def task(
            customer_id: Annotated[int, ConcurrencyLimit(1)],
            concurrency: ConcurrencyLimit = ConcurrencyLimit(max_concurrent=2),
        ): ...  # pragma: no cover

        await docket.add(task)(customer_id=1)


# --- Concurrency key cleanup ---


async def test_annotated_concurrency_keys_cleaned_up(docket: Docket, worker: Worker):
    """Concurrency keys from annotated deps are properly cleaned up."""

    async def task(customer_id: Annotated[int, ConcurrencyLimit(1)]):
        pass

    await docket.add(task)(customer_id=42)
    await worker.run_until_finished()

    async with docket.redis() as redis:
        key = f"{docket.name}:concurrency:customer_id:42"
        assert await redis.exists(key) == 0
