"""Tests for worker-level ``single=True`` dependencies.

Worker-level factories that return ``Runtime`` / ``FailureHandler`` /
``CompletionHandler`` / ``ConcurrencyLimit`` / ``Debounce`` instances act
as defaults for every task the worker executes.  When a task declares one
of the same type, we raise a ``ValueError`` at resolution time rather than
silently picking one.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Annotated

import pytest

from docket import Docket, Worker
from docket.dependencies import (
    ConcurrencyLimit,
    Debounce,
    Depends,
    Perpetual,
    RateLimit,
    Retry,
    Timeout,
)


FAST = timedelta(milliseconds=5)


async def test_worker_level_timeout_applies_when_task_has_none(docket: Docket):
    events: list[str] = []

    async def the_task() -> None:
        events.append("task_start")
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            events.append("cancelled")
            raise

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"timeout": Timeout(timedelta(milliseconds=50))},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert "task_start" in events
    assert "cancelled" in events


async def test_worker_level_retry_applies_when_task_has_none(docket: Docket):
    attempts: list[int] = []

    async def the_task() -> None:
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("fail")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"retry": Retry(attempts=3)},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert len(attempts) == 3


async def test_worker_level_perpetual_applies_when_task_has_none(docket: Docket):
    runs: list[int] = []
    live: dict[str, Perpetual] = {}

    class CapturingPerpetual(Perpetual):
        async def __aenter__(self) -> Perpetual:
            entered = await super().__aenter__()
            live["current"] = entered
            return entered

    def make_perpetual() -> Perpetual:
        return CapturingPerpetual(every=timedelta(milliseconds=10), automatic=False)

    async def the_task() -> None:
        runs.append(len(runs) + 1)
        if len(runs) >= 3:
            live["current"].cancel()

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"perp": make_perpetual()},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert len(runs) == 3


async def test_worker_level_concurrency_limit_applies_when_task_has_none(
    docket: Docket,
):
    seen: list[str] = []

    async def the_task(marker: str) -> None:
        seen.append(marker)

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"limit": ConcurrencyLimit(max_concurrent=1)},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)("a")
        await worker.run_until_finished()

    assert seen == ["a"]


async def test_worker_level_debounce_applies_when_task_has_none(docket: Docket):
    runs: list[str] = []

    async def the_task() -> None:
        runs.append("ran")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"deb": Debounce(timedelta(milliseconds=10))},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert runs == ["ran"]


async def test_conflicting_timeout_raises_and_task_fails(
    docket: Docket, caplog: pytest.LogCaptureFixture
):
    import logging

    async def the_task(
        timeout: Timeout = Timeout(timedelta(seconds=5)),
    ) -> None:  # pragma: no cover - body must not run
        pass

    docket.register(the_task)

    with caplog.at_level(logging.ERROR, logger="docket"):
        async with Worker(
            docket,
            dependencies={"timeout": Timeout(timedelta(seconds=30))},
            minimum_check_interval=FAST,
            scheduling_resolution=FAST,
        ) as worker:
            await docket.add(the_task)()
            await worker.run_until_finished()

    assert "Only one Timeout dependency is allowed" in caplog.text


async def test_conflicting_retry_raises_and_task_fails(
    docket: Docket, caplog: pytest.LogCaptureFixture
):
    import logging

    async def the_task(
        retry: Retry = Retry(attempts=2),
    ) -> None:  # pragma: no cover - body must not run
        pass

    docket.register(the_task)

    with caplog.at_level(logging.ERROR, logger="docket"):
        async with Worker(
            docket,
            dependencies={"retry": Retry(attempts=5)},
            minimum_check_interval=FAST,
            scheduling_resolution=FAST,
        ) as worker:
            await docket.add(the_task)()
            await worker.run_until_finished()

    assert "Only one Retry dependency is allowed" in caplog.text


async def test_conflicting_concurrency_limit_raises_and_task_fails(
    docket: Docket, caplog: pytest.LogCaptureFixture
):
    import logging

    async def the_task(
        customer_id: Annotated[str, ConcurrencyLimit(max_concurrent=1)],
    ) -> None:  # pragma: no cover - body must not run
        pass

    docket.register(the_task)

    with caplog.at_level(logging.ERROR, logger="docket"):
        async with Worker(
            docket,
            dependencies={"global_limit": ConcurrencyLimit(max_concurrent=3)},
            minimum_check_interval=FAST,
            scheduling_resolution=FAST,
        ) as worker:
            await docket.add(the_task)("acme")
            await worker.run_until_finished()

    assert "Only one ConcurrencyLimit dependency is allowed" in caplog.text


async def test_task_level_timeout_without_worker_conflict_still_works(
    docket: Docket,
):
    """Regression guard: task-only single deps behave as before."""
    events: list[str] = []

    async def setup() -> None:
        events.append("setup")

    async def the_task(
        timeout: Timeout = Timeout(timedelta(milliseconds=50)),
    ) -> None:
        events.append("task_start")
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            events.append("cancelled")
            raise

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"setup": Depends(setup)},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert "setup" in events
    assert "cancelled" in events


async def test_non_single_admission_dep_at_worker_level_no_conflict(docket: Docket):
    """``RateLimit`` isn't single=True, so worker-level + task-level coexist."""
    runs: list[str] = []

    async def the_task(
        rate: RateLimit = RateLimit(100, per=timedelta(seconds=1)),
    ) -> None:
        runs.append("ran")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"worker_rate": RateLimit(50, per=timedelta(seconds=1))},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert runs == ["ran"]
