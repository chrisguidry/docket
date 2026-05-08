"""Tests proving the Perpetual chain survives every plausible crash point.

Once a Perpetual has been scheduled at least once, the chain continues as
long as Redis is durable and a worker is eventually running. None of these
tests configure ``Retry`` — the property holds without it.

Two existing mechanisms work together to guarantee this:

1. **Generation-counter supersession** (``runs:{key}`` hash, incremented on
   every ``replace``): stale executions are rejected at ``claim()``, and
   stale terminal writes are no-ops.
2. **Consumer-group redelivery** (``XAUTOCLAIM`` after ``redelivery_timeout``):
   any message whose handling didn't reach ``XACK`` is reclaimed by a healthy
   consumer.

The two structural recovery cases the tests below exercise:

- **Pre-``on_complete`` failure** (no successor yet scheduled) — redelivery
  brings the original message back, the task body re-runs, ``on_complete``
  eventually succeeds, chain continues.
- **Post-``on_complete`` failure** (successor already in Redis with a higher
  generation) — redelivery brings the original back, ``claim()`` sees it
  superseded, the message is ACKed cleanly, the successor runs as scheduled.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable
from unittest.mock import AsyncMock, patch

import pytest

if sys.version_info < (3, 11):  # pragma: no cover
    from exceptiongroup import ExceptionGroup
from redis.exceptions import ConnectionError

from docket import Docket, Perpetual, Worker
from docket.dependencies import TaskOutcome
from docket.execution import Execution, TaskFunction


async def test_chain_survives_worker_death_during_task_body(docket: Docket):
    """A worker that dies before a Perpetual's task body runs is recovered via
    XAUTOCLAIM redelivery: a fresh worker reclaims the message and the chain
    proceeds normally."""
    executions: list[int] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=20)),
    ):
        executions.append(1)

    await docket.add(perpetual_task, key="perpetual")()

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_a:
        worker_a._execute = AsyncMock(side_effect=Exception("simulated crash"))  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(ExceptionGroup) as exc_info:
            await worker_a.run_until_finished()
        assert any("simulated crash" in str(e) for e in exc_info.value.exceptions)
        assert executions == []  # body never ran on worker_a

    await asyncio.sleep(0.25)  # > redelivery_timeout

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_b:
        await worker_b.run_at_most({"perpetual": 3})
        assert len(executions) >= 3  # chain ran on worker_b after redelivery


async def test_chain_survives_on_complete_failure_in_success_path(docket: Docket):
    """When ``Perpetual.on_complete`` raises after a successful task body
    (and the in-``_execute`` recovery's repeat call also fails — simulating a
    Redis outage long enough to defeat the in-place recovery), the worker
    dies. The message stays in the consumer-group pending list; a fresh
    worker reclaims it via XAUTOCLAIM, the body runs again, ``on_complete``
    now succeeds, the chain continues."""
    executions: list[int] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=20)),
    ):
        executions.append(1)

    await docket.add(perpetual_task, key="perpetual")()

    async def crashing_on_complete(
        self: Perpetual, execution: Execution, outcome: TaskOutcome
    ) -> bool:
        raise ConnectionError("simulated Redis blip during on_complete")

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_a:
        with patch.object(Perpetual, "on_complete", crashing_on_complete):
            with pytest.raises(ExceptionGroup):
                await worker_a.run_until_finished()
        assert len(executions) == 1  # body ran once on worker_a before on_complete

    await asyncio.sleep(0.25)

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_b:
        await worker_b.run_at_most({"perpetual": 3})
        assert len(executions) >= 3  # body ran again on worker_b, chain continued


async def test_chain_survives_on_complete_failure_in_failure_path_no_retry(
    docket: Docket,
):
    """When the task body raises (no ``Retry`` configured) and
    ``Perpetual.on_complete`` raises while being called from the
    failure-handling branch, the worker dies. Redelivery saves the chain:
    a fresh worker reclaims, the body raises again, ``on_complete`` runs from
    the failure path with the patch gone, and the chain continues.

    This directly refutes the claim that ``Retry`` is needed for reliability:
    a Perpetual whose body fails AND whose ``on_complete`` fails still
    survives every iteration without ``Retry``."""
    executions: list[int] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=20)),
    ):
        executions.append(1)
        raise ValueError("simulated body failure")

    await docket.add(perpetual_task, key="perpetual")()

    async def crashing_on_complete(
        self: Perpetual, execution: Execution, outcome: TaskOutcome
    ) -> bool:
        raise ConnectionError("simulated Redis blip during on_complete")

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_a:
        with patch.object(Perpetual, "on_complete", crashing_on_complete):
            with pytest.raises(ExceptionGroup):
                await worker_a.run_until_finished()
        assert len(executions) == 1

    await asyncio.sleep(0.25)

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_b:
        await worker_b.run_at_most({"perpetual": 3})
        assert len(executions) >= 3  # chain continued despite repeated body failures


async def test_chain_survives_replace_failure_inside_on_complete(docket: Docket):
    """A more targeted failure than patching ``on_complete`` itself: simulate
    a Redis outage on the ``Docket.replace`` call that ``Perpetual.on_complete``
    makes at ``_perpetual.py:132``. Because ``replace`` fails on every call
    during worker_a's run, both the success-path ``on_complete`` and the
    in-place recovery's repeat call fail, the worker dies; redelivery brings
    the message back; once the patch is gone, ``replace`` works and the chain
    continues."""
    executions: list[int] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=20)),
    ):
        executions.append(1)

    await docket.add(perpetual_task, key="perpetual")()

    def crashing_replace(
        self: Docket,
        function: TaskFunction | str,
        when: datetime,
        key: str,
    ) -> Callable[..., Awaitable[Execution]]:
        raise ConnectionError("simulated Redis blip during docket.replace")

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_a:
        with patch.object(Docket, "replace", crashing_replace):
            with pytest.raises(ExceptionGroup):
                await worker_a.run_until_finished()
        assert len(executions) == 1

    await asyncio.sleep(0.25)

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_b:
        await worker_b.run_at_most({"perpetual": 3})
        assert len(executions) >= 3


async def test_chain_survives_mark_as_completed_failure_via_in_execute_recovery(
    docket: Docket,
):
    """When ``mark_as_completed`` raises after ``on_complete`` already scheduled
    the successor, the existing ``except Exception`` block in ``_execute``
    catches it and re-runs the completion handler, which idempotently rewrites
    the successor via ``replace=True``. The worker does NOT die; the chain
    continues in place. This demonstrates that in-place recovery in ``_execute``
    is also a layer of defense, separate from redelivery."""
    executions: list[int] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=20)),
    ):
        executions.append(1)

    await docket.add(perpetual_task, key="perpetual")()

    original = Execution.mark_as_completed
    failed_once = False

    async def flaky_mark_as_completed(
        self: Execution, *args: Any, **kwargs: Any
    ) -> None:
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise ConnectionError("simulated Redis blip during mark_as_completed")
        return await original(self, *args, **kwargs)

    async with Worker(docket, redelivery_timeout=timedelta(milliseconds=200)) as worker:
        with patch.object(Execution, "mark_as_completed", flaky_mark_as_completed):
            await worker.run_at_most({"perpetual": 3})
        assert failed_once is True
        assert len(executions) >= 3  # in-place recovery kept the chain going


async def test_chain_survives_terminal_failure_after_on_complete_via_supersession(
    docket: Docket,
):
    """Worker dies AFTER ``on_complete`` already scheduled the successor:
    ``mark_as_completed`` raises in the success path, the in-place recovery's
    ``mark_as_failed`` also raises, and the worker exits. The successor is
    already in Redis with an incremented generation, so when redelivery brings
    the original back to a fresh worker, ``claim()`` sees it superseded and
    ACKs cleanly without re-running the body. The successor then runs as
    scheduled and the chain continues. We use ``RuntimeError`` (not
    ``ConnectionError``) because ``Worker._run`` reconnects on
    ``ConnectionError`` and would mask the worker death."""
    executions: list[int] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=20)),
    ):
        executions.append(1)

    await docket.add(perpetual_task, key="perpetual")()

    async def crashing_mark_as_completed(
        self: Execution, *args: Any, **kwargs: Any
    ) -> None:
        raise RuntimeError("simulated blip in mark_as_completed")

    async def crashing_mark_as_failed(
        self: Execution, *args: Any, **kwargs: Any
    ) -> None:
        raise RuntimeError("simulated blip in mark_as_failed")

    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=200),
        # Slow scheduler so the successor doesn't get pulled into the stream
        # before the worker has a chance to crash from this iteration.
        scheduling_resolution=timedelta(seconds=5),
    ) as worker_a:
        with (
            patch.object(Execution, "mark_as_completed", crashing_mark_as_completed),
            patch.object(Execution, "mark_as_failed", crashing_mark_as_failed),
        ):
            with pytest.raises((ExceptionGroup, RuntimeError)):
                await worker_a.run_until_finished()
        assert len(executions) == 1  # body ran once before worker_a died

    await asyncio.sleep(0.25)

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=200)
    ) as worker_b:
        await worker_b.run_at_most({"perpetual": 3})
        # Worker_b reclaims the original via XAUTOCLAIM, sees claim() return
        # SUPERSEDED (generation was incremented when worker_a's on_complete ran),
        # ACKs without re-running the body. The successor that on_complete already
        # scheduled then runs. Chain continues.
        assert len(executions) >= 3


async def test_perpetual_without_retry_survives_repeated_body_failures(
    docket: Docket, worker: Worker
):
    """A Perpetual whose body raises every iteration still gets perpetuated
    via the failure-path call to ``on_complete`` (no ``Retry`` involved).
    This is the most basic refutation of "needs Retry": the chain runs as
    many times as ``run_at_most`` permits, every iteration failing."""
    executions: list[int] = []

    async def always_fails(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=20)),
    ):
        executions.append(1)
        raise ValueError("body always fails")

    await docket.add(always_fails, key="perpetual")()

    await worker.run_at_most({"perpetual": 5})

    assert len(executions) == 5
