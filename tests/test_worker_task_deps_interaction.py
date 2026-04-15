"""Regression tests: task-level ``single=True`` deps keep working when the
worker also has non-single worker-level deps registered (v1 interaction)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Annotated

from docket import Docket, Worker
from docket.dependencies import (
    ConcurrencyLimit,
    Perpetual,
    Retry,
    Timeout,
)


FAST = timedelta(milliseconds=5)


async def test_task_timeout_still_applies_with_worker_deps(docket: Docket):
    events: list[str] = []

    async def noop() -> None:
        events.append("worker_setup")

    async def the_task(timeout: Timeout = Timeout(timedelta(milliseconds=50))) -> None:
        events.append("task_start")
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            events.append("cancelled")
            raise

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"n": noop},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert "worker_setup" in events
    assert "task_start" in events
    assert "cancelled" in events


async def test_task_retry_still_applies_with_worker_deps(docket: Docket):
    worker_setups: list[int] = []
    task_attempts: list[int] = []

    async def setup() -> None:
        worker_setups.append(1)

    async def the_task(retry: Retry = Retry(attempts=3)) -> None:
        task_attempts.append(retry.attempt)
        if retry.attempt < 3:
            raise RuntimeError("fail")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"s": setup},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert task_attempts == [1, 2, 3]
    assert len(worker_setups) == 3


async def test_task_perpetual_still_applies_with_worker_deps(docket: Docket):
    worker_setups: list[int] = []
    runs: list[int] = []

    async def setup() -> None:
        worker_setups.append(1)

    async def the_task(
        perpetual: Perpetual = Perpetual(
            every=timedelta(milliseconds=10), automatic=False
        ),
    ) -> None:
        runs.append(len(runs) + 1)
        if len(runs) >= 3:
            perpetual.cancel()

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"s": setup},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert len(runs) == 3
    assert len(worker_setups) == 3


async def test_task_concurrency_limit_still_enforced_with_worker_deps(docket: Docket):
    seen: list[str] = []

    async def setup() -> None:
        seen.append("setup")

    async def the_task(
        customer_id: Annotated[str, ConcurrencyLimit(max_concurrent=1)],
    ) -> None:
        seen.append(f"task:{customer_id}")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"s": setup},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)("acme")
        await worker.run_until_finished()

    assert "task:acme" in seen
    assert "setup" in seen
