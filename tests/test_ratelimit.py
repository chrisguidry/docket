"""Tests for RateLimit admission control dependency."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Annotated

from docket import ConcurrencyLimit, Docket, Worker
from docket.dependencies import RateLimit


async def test_task_level_rate_limit_drops_excess(docket: Docket, worker: Worker):
    """Task-level rate limit drops excess executions within the window."""
    results: list[str] = []

    async def rated_task(
        rate: RateLimit = RateLimit(2, per=timedelta(seconds=5), drop=True),
    ):
        results.append("executed")

    await docket.add(rated_task)()
    await docket.add(rated_task)()
    await docket.add(rated_task)()

    await worker.run_until_finished()

    assert len(results) == 2


async def test_task_level_rate_limit_allows_after_window(
    docket: Docket, worker: Worker
):
    """Task-level rate limit allows execution after the window expires."""
    results: list[str] = []

    async def rated_task(
        rate: RateLimit = RateLimit(1, per=timedelta(milliseconds=50), drop=True),
    ):
        results.append("executed")

    await docket.add(rated_task)()
    await worker.run_until_finished()
    assert results == ["executed"]

    await asyncio.sleep(0.06)

    await docket.add(rated_task)()
    await worker.run_until_finished()
    assert results == ["executed", "executed"]


async def test_per_parameter_rate_limit_independent_scopes(
    docket: Docket, worker: Worker
):
    """Per-parameter rate limit scopes independently per value."""
    results: list[int] = []

    async def rated_task(
        customer_id: Annotated[int, RateLimit(1, per=timedelta(seconds=5), drop=True)],
    ):
        results.append(customer_id)

    await docket.add(rated_task)(customer_id=1)
    await docket.add(rated_task)(customer_id=1)
    await docket.add(rated_task)(customer_id=2)

    worker.concurrency = 10
    await worker.run_until_finished()

    assert sorted(results) == [1, 2]
    assert results.count(1) == 1


async def test_drop_true_drops_excess(docket: Docket, worker: Worker):
    """With drop=True, excess tasks are quietly dropped instead of rescheduled."""
    results: list[str] = []

    async def rated_task(
        rate: RateLimit = RateLimit(1, per=timedelta(seconds=5), drop=True),
    ):
        results.append("executed")

    await docket.add(rated_task)()
    await docket.add(rated_task)()
    await docket.add(rated_task)()

    await worker.run_until_finished()

    assert results == ["executed"]


async def test_drop_false_excess_eventually_executes(docket: Docket, worker: Worker):
    """With drop=False (default), excess tasks reschedule and eventually execute."""
    results: list[str] = []

    async def rated_task(
        rate: RateLimit = RateLimit(1, per=timedelta(milliseconds=50)),
    ):
        results.append("executed")

    await docket.add(rated_task)()
    await docket.add(rated_task)()

    await worker.run_until_finished()

    assert len(results) == 2


async def test_multiple_rate_limits_on_different_parameters(
    docket: Docket, worker: Worker
):
    """Multiple RateLimit annotations on different parameters are independent."""
    results: list[tuple[int, str]] = []

    async def task(
        customer_id: Annotated[int, RateLimit(1, per=timedelta(seconds=5), drop=True)],
        region: Annotated[str, RateLimit(1, per=timedelta(seconds=5), drop=True)],
    ):
        results.append((customer_id, region))

    await docket.add(task)(customer_id=1, region="us")
    await worker.run_until_finished()

    await docket.add(task)(customer_id=1, region="eu")  # blocked by customer_id=1
    await docket.add(task)(customer_id=2, region="us")  # blocked by region="us"

    await worker.run_until_finished()

    assert results == [(1, "us")]


async def test_rate_limit_coexists_with_concurrency_limit(
    docket: Docket, worker: Worker
):
    """RateLimit + ConcurrencyLimit can coexist on the same task."""
    results: list[str] = []

    async def task(
        customer_id: Annotated[int, ConcurrencyLimit(1)],
        rate: RateLimit = RateLimit(10, per=timedelta(seconds=5)),
    ):
        results.append(f"executed_{customer_id}")

    await docket.add(task)(customer_id=1)
    await worker.run_until_finished()
    assert results == ["executed_1"]


async def test_rate_limit_key_cleaned_up_after_ttl(docket: Docket, worker: Worker):
    """Redis key is cleaned up after TTL expires."""

    async def rated_task(
        rate: RateLimit = RateLimit(10, per=timedelta(milliseconds=50)),
    ):
        pass

    await docket.add(rated_task)()
    await worker.run_until_finished()

    # Wait for TTL to expire (key TTL = window * 2)
    await asyncio.sleep(0.15)

    async with docket.redis() as redis:
        ratelimit_keys: list[str] = [
            key
            async for key in redis.scan_iter(  # type: ignore[union-attr]
                match=f"{docket.name}:ratelimit:*"
            )
        ]
        assert ratelimit_keys == []
