"""Tests for Debounce dependency (true debounce: settle then fire)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Annotated

import pytest

from docket import ConcurrencyLimit, Docket, Worker
from docket.dependencies import Debounce


async def test_single_submission_fires_after_settle(docket: Docket, worker: Worker):
    """A single submission fires after the settle window, not immediately."""
    results: list[str] = []

    async def debounced_task(
        debounce: Debounce = Debounce(timedelta(milliseconds=50)),
    ):
        results.append("executed")

    await docket.add(debounced_task)()

    # The winner gets rescheduled; run_until_finished processes it
    await worker.run_until_finished()
    assert results == ["executed"]


async def test_rapid_submissions_only_one_execution(docket: Docket, worker: Worker):
    """Rapid submissions result in only one execution after settling."""
    results: list[str] = []

    async def debounced_task(
        debounce: Debounce = Debounce(timedelta(milliseconds=100)),
    ):
        results.append("executed")

    await docket.add(debounced_task)()
    await docket.add(debounced_task)()
    await docket.add(debounced_task)()

    await worker.run_until_finished()
    assert results == ["executed"]


async def test_per_parameter_debounce_independent_windows(
    docket: Docket, worker: Worker
):
    """Per-parameter debounce has independent settle windows per value."""
    results: list[int] = []

    async def debounced_task(
        customer_id: Annotated[int, Debounce(timedelta(milliseconds=50))],
    ):
        results.append(customer_id)

    await docket.add(debounced_task)(customer_id=1)
    await docket.add(debounced_task)(customer_id=2)
    await docket.add(debounced_task)(customer_id=1)

    await worker.run_until_finished()

    assert sorted(results) == [1, 2]
    assert results.count(1) == 1


async def test_debounce_single_rejects_two(docket: Docket):
    """single=True rejects two Debounce on the same task."""
    with pytest.raises(ValueError, match="Only one Debounce"):

        async def task(
            a: Annotated[int, Debounce(timedelta(seconds=1))],
            b: Annotated[str, Debounce(timedelta(seconds=2))],
        ): ...  # pragma: no cover

        await docket.add(task)(a=1, b="x")


async def test_debounce_coexists_with_concurrency_limit(docket: Docket, worker: Worker):
    """Debounce + ConcurrencyLimit can coexist on the same task."""
    results: list[str] = []

    async def task(
        customer_id: Annotated[int, ConcurrencyLimit(1)],
        debounce: Debounce = Debounce(timedelta(milliseconds=50)),
    ):
        results.append(f"executed_{customer_id}")

    await docket.add(task)(customer_id=1)
    await worker.run_until_finished()
    assert results == ["executed_1"]


async def test_debounce_keys_cleaned_up_after_execution(docket: Docket, worker: Worker):
    """Winner and last_seen keys are cleaned up after the task proceeds."""

    async def debounced_task(
        debounce: Debounce = Debounce(timedelta(milliseconds=50)),
    ):
        pass

    await docket.add(debounced_task)()
    await worker.run_until_finished()

    # Wait for any residual TTLs
    await asyncio.sleep(0.1)

    async with docket.redis() as redis:
        debounce_keys: list[str] = [
            key
            async for key in redis.scan_iter(  # type: ignore[union-attr]
                match=f"{docket.name}:debounce:*"
            )
        ]
        assert debounce_keys == []
