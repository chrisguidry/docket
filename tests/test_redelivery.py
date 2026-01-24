"""Tests for redelivery behavior and lease renewal.

These tests verify that:
1. Tasks are redelivered when workers crash/abandon them
2. Lease renewal prevents duplicate execution when tasks run longer than redelivery_timeout
"""

import asyncio
import inspect
import sys
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest

if sys.version_info < (3, 11):  # pragma: no cover
    from exceptiongroup import ExceptionGroup
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster
from redis.exceptions import ConnectionError

from docket import Docket, Perpetual, Retry, Timeout, Worker


@pytest.fixture
def the_task() -> AsyncMock:
    task = AsyncMock()
    task.__name__ = "the_task"
    task.__signature__ = inspect.signature(lambda *args, **kwargs: None)
    task.return_value = None
    return task


async def test_redelivery_from_abandoned_worker(docket: Docket, the_task: AsyncMock):
    """Tasks should be redelivered when a worker crashes or abandons them."""
    await docket.add(the_task)()

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_a:
        worker_a._execute = AsyncMock(side_effect=Exception("Nope"))  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(ExceptionGroup) as exc_info:
            await worker_a.run_until_finished()
        assert any("Nope" in str(e) for e in exc_info.value.exceptions)

    the_task.assert_not_called()

    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_b:
        async with docket.redis() as redis:
            pending_info = await redis.xpending(
                docket.stream_key,
                docket.worker_group_name,
            )
            assert pending_info["pending"] == 1, (
                "Expected one pending task in the stream"
            )

        await asyncio.sleep(0.125)  # longer than the redelivery timeout

        await worker_b.run_until_finished()

    the_task.assert_awaited_once_with()


async def test_long_running_task_not_duplicated(docket: Docket):
    """Test that lease renewal prevents duplicate execution when task exceeds redelivery_timeout.

    This test runs a task that takes 500ms with a 200ms redelivery_timeout.
    Without lease renewal, XAUTOCLAIM would reclaim the message after 200ms,
    causing duplicate execution. With lease renewal (every 50ms), the message
    stays claimed and no duplicates occur.
    """
    executions: list[int] = []

    async def slow_task(task_id: int):
        executions.append(task_id)
        await asyncio.sleep(0.5)

    await docket.add(slow_task, key="slow-1")(task_id=1)
    await docket.add(slow_task, key="slow-2")(task_id=2)

    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    ) as worker:
        await worker.run_until_finished()

    assert sorted(executions) == [1, 2], f"Expected [1, 2], got {executions}"


async def test_retry_with_long_running_task(docket: Docket):
    """Test that retries work correctly with lease renewal.

    A task that fails and retries should still benefit from lease renewal.
    Each attempt should be a distinct execution without duplicates.
    """
    attempts: list[tuple[str, int]] = []

    async def flaky_task(
        task_id: str,
        retry: Retry = Retry(attempts=3, delay=timedelta(milliseconds=50)),
    ):
        attempts.append((task_id, retry.attempt))
        await asyncio.sleep(0.3)

        if retry.attempt < 3:
            raise ValueError("Temporary failure")

    await docket.add(flaky_task, key="flaky")(task_id="test")

    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    ) as worker:
        await worker.run_until_finished()

    assert attempts == [
        ("test", 1),
        ("test", 2),
        ("test", 3),
    ], f"Expected 3 distinct attempts, got {attempts}"


async def test_multiple_workers_no_duplicate_execution(docket: Docket):
    """Test that lease renewal prevents duplicates across multiple competing workers.

    With multiple workers and tasks that run longer than redelivery_timeout,
    XAUTOCLAIM could reclaim a message from one worker and deliver it to another.
    Lease renewal prevents this by keeping messages "fresh" while being processed.
    """
    executions: list[tuple[str, int]] = []
    lock = asyncio.Lock()

    async def slow_task(task_id: int, worker_name: str = ""):
        async with lock:
            executions.append((worker_name, task_id))
        await asyncio.sleep(0.5)

    # Schedule several tasks
    for i in range(6):
        await docket.add(slow_task, key=f"task-{i}")(task_id=i)

    # Run multiple workers concurrently with short redelivery_timeout
    workers = [
        Worker(
            docket,
            name=f"worker-{i}",
            redelivery_timeout=timedelta(milliseconds=200),
            minimum_check_interval=timedelta(milliseconds=10),
            scheduling_resolution=timedelta(milliseconds=10),
            concurrency=2,
        )
        for i in range(3)
    ]

    async def run_worker(worker: Worker):
        async with worker:
            await worker.run_until_finished()

    await asyncio.gather(*[run_worker(w) for w in workers])

    # Each task should execute exactly once
    task_ids = sorted([task_id for _, task_id in executions])
    assert task_ids == [0, 1, 2, 3, 4, 5], f"Expected each task once, got {executions}"


async def test_perpetual_task_with_lease_renewal(docket: Docket):
    """Perpetual tasks that run longer than redelivery_timeout should reschedule correctly.

    Without lease renewal, a perpetual task running longer than redelivery_timeout
    could be reclaimed by XAUTOCLAIM, causing duplicate execution.
    """
    executions: list[int] = []

    async def slow_perpetual(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=100)),
    ):
        executions.append(len(executions) + 1)
        await asyncio.sleep(0.4)  # Longer than redelivery_timeout

    await docket.add(slow_perpetual, key="perpetual")()

    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    ) as worker:
        await worker.run_at_most({"perpetual": 3})

    assert executions == [1, 2, 3], (
        f"Expected 3 sequential executions, got {executions}"
    )


async def test_user_timeout_longer_than_redelivery(docket: Docket):
    """User-specified Timeout > redelivery_timeout should work with lease renewal.

    Lease renewal allows tasks to run longer than redelivery_timeout without
    being reclaimed by XAUTOCLAIM.
    """
    task_completed = False

    async def long_task_with_timeout(
        timeout: Timeout = Timeout(timedelta(seconds=2)),
    ):
        nonlocal task_completed
        await asyncio.sleep(0.5)  # Longer than redelivery_timeout
        task_completed = True

    await docket.add(long_task_with_timeout)()

    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    ) as worker:
        await worker.run_until_finished()

    assert task_completed, "Task should complete even with timeout > redelivery_timeout"


async def test_workers_with_same_redelivery_timeout(docket: Docket):
    """Workers with consistent redelivery_timeouts should coexist correctly.

    When workers share the same redelivery_timeout, their lease renewal intervals
    are synchronized, preventing incorrect task reclamation via XAUTOCLAIM.

    Note: Workers with different redelivery_timeouts can cause issues - a worker
    with a shorter timeout may reclaim tasks from a worker with a longer timeout.
    Use consistent timeouts across workers in a cluster.
    """
    executions: list[tuple[str, int]] = []
    lock = asyncio.Lock()

    async def tracked_task(task_id: int):
        async with lock:
            executions.append(("started", task_id))
        await asyncio.sleep(0.5)  # Longer than redelivery_timeout
        async with lock:
            executions.append(("completed", task_id))

    for i in range(4):
        await docket.add(tracked_task, key=f"task-{i}")(task_id=i)

    # Both workers use the same redelivery_timeout
    worker_a = Worker(
        docket,
        name="worker-a",
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    )
    worker_b = Worker(
        docket,
        name="worker-b",
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    )

    async def run_worker(w: Worker):
        async with w:
            await w.run_until_finished()

    await asyncio.gather(run_worker(worker_a), run_worker(worker_b))

    # All tasks should complete exactly once
    completed = [tid for event, tid in executions if event == "completed"]
    assert sorted(completed) == [0, 1, 2, 3], (
        f"Expected 4 completions, got {executions}"
    )


async def test_worker_joining_doesnt_steal_renewed_lease(docket: Docket):
    """A new worker joining shouldn't steal tasks that are actively being renewed.

    Worker A starts a task and renews its lease. Worker B joins later and
    runs XAUTOCLAIM, but shouldn't reclaim A's actively-renewed task.
    """
    executions: list[tuple[str, int]] = []
    task_started = asyncio.Event()

    async def slow_task(task_id: int):
        executions.append(("start", task_id))
        task_started.set()
        await asyncio.sleep(0.6)  # Long task
        executions.append(("end", task_id))

    await docket.add(slow_task, key="task")(task_id=1)

    worker_a = Worker(
        docket,
        name="worker-a",
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    )

    async def run_a():
        async with worker_a:
            await worker_a.run_until_finished()

    # Start worker A
    a_task = asyncio.create_task(run_a())

    # Wait for task to start
    await asyncio.wait_for(task_started.wait(), timeout=2.0)

    # Small delay to ensure A is actively renewing
    await asyncio.sleep(0.1)

    # Start worker B - it should NOT steal the task
    worker_b = Worker(
        docket,
        name="worker-b",
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=10),
        scheduling_resolution=timedelta(milliseconds=10),
    )

    async def run_b():
        async with worker_b:
            await worker_b.run_until_finished()

    await asyncio.gather(a_task, run_b())

    # Task should execute exactly once (start and end)
    assert executions == [("start", 1), ("end", 1)], (
        f"Task stolen or duplicated: {executions}"
    )


async def test_lease_renewal_recovers_from_redis_error(
    docket: Docket, caplog: pytest.LogCaptureFixture
):
    """Lease renewal should recover from transient Redis errors.

    If XCLAIM fails, the worker should log a warning and continue.
    The task should still complete successfully.
    """
    task_completed = False

    async def slow_task():
        nonlocal task_completed
        await asyncio.sleep(0.5)
        task_completed = True

    await docket.add(slow_task)()

    # Track XCLAIM calls to inject error on first call only
    xclaim_calls = 0
    original_redis_xclaim = Redis.xclaim
    original_cluster_xclaim = RedisCluster.xclaim

    async def mock_redis_xclaim(  # pragma: no cover
        self: Redis,  # type: ignore[type-arg]
        *args: object,
        **kwargs: object,
    ) -> object:
        nonlocal xclaim_calls
        xclaim_calls += 1
        if xclaim_calls == 1:
            raise ConnectionError("Simulated Redis error")
        return await original_redis_xclaim(self, *args, **kwargs)  # type: ignore[arg-type]

    async def mock_cluster_xclaim(  # pragma: no cover
        self: RedisCluster,  # type: ignore[type-arg]
        *args: object,
        **kwargs: object,
    ) -> object:
        nonlocal xclaim_calls
        xclaim_calls += 1
        if xclaim_calls == 1:
            raise ConnectionError("Simulated Redis error")
        return await original_cluster_xclaim(self, *args, **kwargs)  # type: ignore[arg-type]

    with (
        patch.object(Redis, "xclaim", mock_redis_xclaim),
        patch.object(RedisCluster, "xclaim", mock_cluster_xclaim),
    ):
        async with Worker(
            docket,
            redelivery_timeout=timedelta(milliseconds=200),
            minimum_check_interval=timedelta(milliseconds=10),
            scheduling_resolution=timedelta(milliseconds=10),
        ) as worker:
            await worker.run_until_finished()

    assert task_completed, "Task should complete despite renewal error"
    assert xclaim_calls >= 2, "Should have retried renewal after error"
    assert "Failed to renew leases" in caplog.text


async def test_lease_renewal_exits_cleanly_with_no_active_tasks(docket: Docket):
    """Lease renewal loop should exit cleanly when worker stops with no active tasks."""
    async with Worker(
        docket,
        redelivery_timeout=timedelta(milliseconds=40),
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        # Worker with no tasks exits immediately
        await worker.run_until_finished()
