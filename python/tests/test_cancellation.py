"""Tests for cancellation of running tasks."""

import asyncio
from datetime import timedelta

import pytest

from typing import AsyncGenerator

from docket import Docket, ExecutionCancelled, ExecutionState, Worker
from docket.execution import ProgressEvent, StateEvent


async def test_cancel_running_task(docket: Docket, worker: Worker):
    """A running task can be cancelled via docket.cancel()."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow_task():
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    docket.register(slow_task)
    execution = await docket.add(slow_task)()

    async def run_worker():
        await worker.run_until_finished()

    worker_task = asyncio.create_task(run_worker())

    await asyncio.wait_for(started.wait(), timeout=5.0)

    await docket.cancel(execution.key)

    await asyncio.wait_for(cancelled.wait(), timeout=5.0)

    await asyncio.wait_for(worker_task, timeout=5.0)


async def test_double_cancel_does_not_overwrite_runs_hash(
    docket: Docket, worker: Worker
):
    """A second ``docket.cancel()`` after the task has fully settled into
    ``state=cancelled`` must not modify the runs hash -- ``state`` stays
    ``cancelled``, and ``completed_at`` stays at the first cancel's
    timestamp.

    The Lua's terminal-state guard at the bottom of ``_cancel_task``
    (``if current_state ~= 'completed' and current_state ~= 'failed'
    and current_state ~= 'cancelled' then ...``) is what enforces this.
    Locks it in so a future refactor that drops the guard would be
    caught here rather than at someone's audit log noticing two
    different ``completed_at`` timestamps for the same cancellation.

    NB: the snapshot baseline is taken AFTER the worker has finished
    processing the cancellation -- otherwise we'd race against the
    worker's own ``mark_as_cancelled`` (via ``_terminal``), which
    legitimately rewrites ``completed_at`` from the CancelledError
    handler in ``_execute``.  The test is purely about ``_cancel_task``
    Lua's idempotency, not about any worker-side cancel handling.
    """
    started = asyncio.Event()

    async def slow_task():
        started.set()
        await asyncio.sleep(60)

    docket.register(slow_task)
    execution = await docket.add(slow_task)()

    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(started.wait(), timeout=5.0)

    # First cancel: triggers both the Lua tombstone AND the worker's
    # CancelledError → mark_as_cancelled path.  Wait for the worker to
    # finish so both paths have run and the runs hash is fully settled.
    await docket.cancel(execution.key)
    await asyncio.wait_for(worker_task, timeout=5.0)

    # Snapshot the settled state.  This is the baseline the second cancel
    # must not perturb.
    async with docket.redis() as redis:
        runs_key = docket.runs_key(execution.key)
        baseline_state = await redis.hget(runs_key, "state")
        baseline_completed_at = await redis.hget(runs_key, "completed_at")

    assert baseline_state == b"cancelled", (
        f"setup: runs hash should be 'cancelled' after first cancel + "
        f"worker drain; got: {baseline_state!r}"
    )
    assert baseline_completed_at is not None

    # Second cancel against a fully-settled CANCELLED task.  The
    # ``_cancel_task`` Lua's terminal-state guard should short-circuit
    # the HSET and leave the runs hash untouched.
    await docket.cancel(execution.key)

    async with docket.redis() as redis:
        state_after = await redis.hget(runs_key, "state")
        completed_at_after = await redis.hget(runs_key, "completed_at")

    assert state_after == baseline_state, (
        f"second cancel must not change state; "
        f"baseline={baseline_state!r}, after={state_after!r}"
    )
    assert completed_at_after == baseline_completed_at, (
        f"second cancel must not overwrite the first cancel's "
        f"completed_at; baseline={baseline_completed_at!r}, "
        f"after={completed_at_after!r}"
    )


async def test_cancel_running_task_state(docket: Docket, worker: Worker):
    """A cancelled running task transitions to CANCELLED state."""
    started = asyncio.Event()

    async def slow_task():
        started.set()
        await asyncio.sleep(60)

    docket.register(slow_task)
    execution = await docket.add(slow_task)()

    async def run_worker():
        await worker.run_until_finished()

    worker_task = asyncio.create_task(run_worker())

    await asyncio.wait_for(started.wait(), timeout=5.0)

    await docket.cancel(execution.key)

    await asyncio.wait_for(worker_task, timeout=5.0)

    await execution.sync()
    assert execution.state == ExecutionState.CANCELLED


async def test_cancel_running_task_with_cleanup(docket: Docket, worker: Worker):
    """A task can catch CancelledError to perform cleanup."""
    started = asyncio.Event()
    cleanup_done = asyncio.Event()

    async def task_with_cleanup():
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cleanup_done.set()
            raise

    docket.register(task_with_cleanup)
    execution = await docket.add(task_with_cleanup)()

    async def run_worker():
        await worker.run_until_finished()

    worker_task = asyncio.create_task(run_worker())

    await asyncio.wait_for(started.wait(), timeout=5.0)

    await docket.cancel(execution.key)

    await asyncio.wait_for(cleanup_done.wait(), timeout=5.0)

    await asyncio.wait_for(worker_task, timeout=5.0)


async def test_cancel_task_that_ignores_cancellation(docket: Docket, worker: Worker):
    """A task that catches and swallows CancelledError continues to completion."""
    started = asyncio.Event()
    cancellation_caught = asyncio.Event()
    completed = asyncio.Event()

    async def stubborn_task():
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancellation_caught.set()
        completed.set()

    docket.register(stubborn_task)
    execution = await docket.add(stubborn_task)()

    async def run_worker():
        await worker.run_until_finished()

    worker_task = asyncio.create_task(run_worker())

    await asyncio.wait_for(started.wait(), timeout=5.0)

    await docket.cancel(execution.key)

    await asyncio.wait_for(cancellation_caught.wait(), timeout=5.0)
    await asyncio.wait_for(completed.wait(), timeout=5.0)

    await asyncio.wait_for(worker_task, timeout=5.0)

    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED


async def test_cancel_already_completed_is_noop(docket: Docket, worker: Worker):
    """Cancelling a task that has already completed is a no-op."""

    async def quick_task():
        pass

    docket.register(quick_task)
    execution = await docket.add(quick_task)()

    await worker.run_until_finished()

    await docket.cancel(execution.key)

    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED


async def test_cancel_publishes_state_event(docket: Docket, worker: Worker):
    """Cancelling a running task publishes a CANCELLED state event."""
    started = asyncio.Event()
    state_events: list[StateEvent | ProgressEvent] = []

    async def slow_task():
        started.set()
        await asyncio.sleep(60)

    docket.register(slow_task)
    execution = await docket.add(slow_task)()

    async def collect_state_events():
        async for event in execution.subscribe():  # pragma: no branch
            state_events.append(event)
            if (
                event.get("type") == "state"
                and event.get("state") == ExecutionState.CANCELLED
            ):
                break

    collector_task = asyncio.create_task(collect_state_events())

    async def run_worker():
        await worker.run_until_finished()

    worker_task = asyncio.create_task(run_worker())

    await asyncio.wait_for(started.wait(), timeout=5.0)

    await docket.cancel(execution.key)

    await asyncio.wait_for(collector_task, timeout=5.0)

    await asyncio.wait_for(worker_task, timeout=5.0)

    cancelled_events = [
        e
        for e in state_events
        if e.get("type") == "state" and e.get("state") == ExecutionState.CANCELLED
    ]
    assert len(cancelled_events) == 1


@pytest.fixture
async def second_worker(docket: Docket) -> AsyncGenerator[Worker, None]:
    """A second worker to test multi-worker scenarios."""
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as w:
        yield w


async def test_cancel_only_affects_running_worker(
    docket: Docket, worker: Worker, second_worker: Worker
):
    """A cancellation signal only affects the worker running the task."""
    started_on_worker = asyncio.Event()

    async def slow_task():
        started_on_worker.set()
        await asyncio.sleep(60)

    docket.register(slow_task)
    execution = await docket.add(slow_task)()

    async def run_first_worker():
        await worker.run_until_finished()

    worker_task = asyncio.create_task(run_first_worker())

    await asyncio.wait_for(started_on_worker.wait(), timeout=5.0)

    await docket.cancel(execution.key)

    await asyncio.wait_for(worker_task, timeout=5.0)

    await execution.sync()
    assert execution.state == ExecutionState.CANCELLED


async def test_cancel_running_task_with_zero_execution_ttl(zero_ttl_docket: Docket):
    """Cancellation with execution_ttl=0 deletes the execution record immediately."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow_task():
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    zero_ttl_docket.register(slow_task)
    execution = await zero_ttl_docket.add(slow_task)()

    async with Worker(
        zero_ttl_docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:

        async def run_worker():
            await worker.run_until_finished()

        worker_task = asyncio.create_task(run_worker())

        await asyncio.wait_for(started.wait(), timeout=5.0)

        await zero_ttl_docket.cancel(execution.key)

        await asyncio.wait_for(cancelled.wait(), timeout=5.0)

        await asyncio.wait_for(worker_task, timeout=5.0)

    # With execution_ttl=0, execution data is deleted after terminal state
    # Verify the task was cancelled and execution record was cleaned up
    assert cancelled.is_set()
    async with zero_ttl_docket.redis() as redis:
        exists = await redis.exists(f"{zero_ttl_docket.name}:runs:{execution.key}")
    assert not exists, "execution record should be deleted with execution_ttl=0"


async def test_cancelled_task_with_retry_does_not_retry(docket: Docket, worker: Worker):
    """A cancelled task should NOT retry, even if it has a Retry dependency."""
    from docket.dependencies import Retry

    started = asyncio.Event()
    execution_count = 0

    async def retryable_task(retry: Retry = Retry(attempts=3)):
        nonlocal execution_count
        execution_count += 1
        started.set()
        await asyncio.sleep(60)

    docket.register(retryable_task)
    execution = await docket.add(retryable_task)()

    async def run_worker():
        await worker.run_until_finished()

    worker_task = asyncio.create_task(run_worker())

    await asyncio.wait_for(started.wait(), timeout=5.0)

    await docket.cancel(execution.key)

    await asyncio.wait_for(worker_task, timeout=5.0)

    await execution.sync()
    assert execution.state == ExecutionState.CANCELLED
    assert execution_count == 1, "cancelled task should not retry"


async def test_cancelled_perpetual_task_does_not_perpetuate(
    docket: Docket, worker: Worker
):
    """A cancelled Perpetual task should NOT reschedule itself."""
    from docket.dependencies import Perpetual

    started = asyncio.Event()
    execution_count = 0

    async def perpetual_task(perpetual: Perpetual = Perpetual()):
        nonlocal execution_count
        execution_count += 1
        started.set()
        await asyncio.sleep(60)

    docket.register(perpetual_task)
    execution = await docket.add(perpetual_task)()

    async def run_worker():
        await worker.run_until_finished()

    worker_task = asyncio.create_task(run_worker())

    await asyncio.wait_for(started.wait(), timeout=5.0)

    await docket.cancel(execution.key)

    await asyncio.wait_for(worker_task, timeout=5.0)

    await execution.sync()
    assert execution.state == ExecutionState.CANCELLED
    assert execution_count == 1, "cancelled perpetual task should not reschedule"

    # Verify nothing was rescheduled in the queue
    async with docket.redis() as redis:
        queue_count = await redis.zcard(docket.queue_key)
        stream_len = await redis.xlen(docket.stream_key)
    assert queue_count == 0, "no tasks should be scheduled"
    assert stream_len == 0, "no tasks should be in the stream"


async def test_cancel_running_task_with_timeout(docket: Docket, worker: Worker):
    """A running task with Timeout can be cancelled via docket.cancel().

    Regression test for bug where _run_function_with_timeout converted
    ALL CancelledError to TimeoutError, breaking external cancellation.
    """
    from docket.dependencies import Timeout

    started = asyncio.Event()

    async def slow_task_with_timeout(timeout: Timeout = Timeout(timedelta(seconds=60))):
        started.set()
        await asyncio.sleep(60)

    docket.register(slow_task_with_timeout)
    execution = await docket.add(slow_task_with_timeout)()

    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(started.wait(), timeout=5.0)

    await docket.cancel(execution.key)
    await asyncio.wait_for(worker_task, timeout=5.0)

    await execution.sync()
    assert execution.state == ExecutionState.CANCELLED  # NOT FAILED


async def test_get_result_raises_execution_cancelled_for_cancelled_task(
    docket: Docket, worker: Worker
):
    """get_result() should raise ExecutionCancelled for cancelled tasks."""
    started = asyncio.Event()

    async def cancellable_task():
        started.set()
        await asyncio.sleep(60)

    docket.register(cancellable_task)
    execution = await docket.add(cancellable_task)()

    worker_task = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(started.wait(), timeout=5.0)

    await docket.cancel(execution.key)
    await asyncio.wait_for(worker_task, timeout=5.0)

    with pytest.raises(ExecutionCancelled):
        await execution.get_result()
