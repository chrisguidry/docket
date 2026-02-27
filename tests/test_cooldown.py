"""Tests for Cooldown dependency."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Annotated

import pytest

from docket import ConcurrencyLimit, Docket, Worker
from docket.dependencies import Cooldown


async def test_task_level_cooldown_blocks_rapid_reexecution(
    docket: Docket, worker: Worker
):
    """Task-level cooldown drops duplicate execution within the window."""
    results: list[str] = []

    async def cooled_task(
        cooldown: Cooldown = Cooldown(timedelta(seconds=5)),
    ):
        results.append("executed")

    await docket.add(cooled_task)()
    await docket.add(cooled_task)()

    await worker.run_until_finished()

    assert results == ["executed"]


async def test_task_level_cooldown_allows_after_window(docket: Docket, worker: Worker):
    """Task-level cooldown allows execution after the window expires."""
    results: list[str] = []

    async def cooled_task(
        cooldown: Cooldown = Cooldown(timedelta(milliseconds=50)),
    ):
        results.append("executed")

    await docket.add(cooled_task)()
    await worker.run_until_finished()
    assert results == ["executed"]

    await asyncio.sleep(0.06)

    await docket.add(cooled_task)()
    await worker.run_until_finished()
    assert results == ["executed", "executed"]


async def test_per_parameter_cooldown_blocks_same_value(docket: Docket, worker: Worker):
    """Per-parameter cooldown blocks same value, allows different values."""
    results: list[int] = []

    async def cooled_task(
        customer_id: Annotated[int, Cooldown(timedelta(seconds=5))],
    ):
        results.append(customer_id)

    await docket.add(cooled_task)(customer_id=1)
    await docket.add(cooled_task)(customer_id=1)
    await docket.add(cooled_task)(customer_id=2)

    worker.concurrency = 10
    await worker.run_until_finished()

    assert sorted(results) == [1, 2]
    assert results.count(1) == 1


async def test_cooldown_single_rejects_two(docket: Docket):
    """single=True rejects two Cooldown on the same task."""
    with pytest.raises(ValueError, match="Only one Cooldown"):

        async def task(
            a: Annotated[int, Cooldown(timedelta(seconds=1))],
            b: Annotated[str, Cooldown(timedelta(seconds=2))],
        ): ...  # pragma: no cover

        await docket.add(task)(a=1, b="x")


async def test_cooldown_coexists_with_concurrency_limit(docket: Docket, worker: Worker):
    """Cooldown + ConcurrencyLimit can coexist on the same task."""
    results: list[str] = []

    async def task(
        customer_id: Annotated[int, ConcurrencyLimit(1)],
        cooldown: Cooldown = Cooldown(timedelta(milliseconds=50)),
    ):
        results.append(f"executed_{customer_id}")

    await docket.add(task)(customer_id=1)
    await worker.run_until_finished()
    assert results == ["executed_1"]


async def test_cooldown_key_cleaned_up_after_ttl(docket: Docket, worker: Worker):
    """Redis key is cleaned up after TTL expires."""

    async def cooled_task(
        cooldown: Cooldown = Cooldown(timedelta(milliseconds=50)),
    ):
        pass

    await docket.add(cooled_task)()
    await worker.run_until_finished()

    # Wait for TTL to expire
    await asyncio.sleep(0.1)

    async with docket.redis() as redis:
        # Scan for any cooldown keys — should all be expired
        cooldown_keys: list[str] = [
            key
            async for key in redis.scan_iter(  # type: ignore[union-attr]
                match=f"{docket.name}:cooldown:*"
            )
        ]
        assert cooldown_keys == []
