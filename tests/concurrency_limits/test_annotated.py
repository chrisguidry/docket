"""Tests for ConcurrencyLimit via Annotated-style type hints."""

from __future__ import annotations

import asyncio
import time
from typing import Annotated

import pytest

from docket import ConcurrencyLimit, Docket, Worker

from tests.concurrency_limits.overlap import assert_some_overlap


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


async def test_two_annotated_concurrency_limits_rejected(docket: Docket):
    """ConcurrencyLimit.single=True prevents two annotations on one function."""
    with pytest.raises(ValueError, match="Only one ConcurrencyLimit"):

        async def task(
            customer_id: Annotated[int, ConcurrencyLimit(1)],
            region: Annotated[str, ConcurrencyLimit(2)],
        ): ...  # pragma: no cover

        await docket.add(task)(customer_id=1, region="us")


async def test_single_conflict_annotation_and_default(docket: Docket):
    """single=True conflict detected across annotation and default-param styles."""
    with pytest.raises(ValueError, match="Only one ConcurrencyLimit"):

        async def task(
            customer_id: Annotated[int, ConcurrencyLimit(1)],
            concurrency: ConcurrencyLimit = ConcurrencyLimit(max_concurrent=2),
        ): ...  # pragma: no cover

        await docket.add(task)(customer_id=1)


async def test_annotated_concurrency_keys_cleaned_up(docket: Docket, worker: Worker):
    """Concurrency keys from annotated deps are properly cleaned up."""

    async def task(customer_id: Annotated[int, ConcurrencyLimit(1)]):
        pass

    await docket.add(task)(customer_id=42)
    await worker.run_until_finished()

    async with docket.redis() as redis:
        key = f"{docket.name}:concurrency:customer_id:42"
        assert await redis.exists(key) == 0
