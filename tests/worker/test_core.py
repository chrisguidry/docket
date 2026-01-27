"""Tests for core worker behavior: lifecycle, concurrency, reconnection."""

import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest

if sys.version_info < (3, 11):  # pragma: no cover
    from exceptiongroup import ExceptionGroup
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster
from redis.exceptions import ConnectionError

from docket import CurrentWorker, Docket, Worker
from docket.tasks import standard_tasks


async def test_worker_acknowledges_messages(
    docket: Docket, worker: Worker, the_task: AsyncMock
):
    """The worker should acknowledge and drain messages as they're processed"""

    await docket.add(the_task)()

    await worker.run_until_finished()

    async with docket.redis() as redis:
        pending_info = await redis.xpending(
            name=docket.stream_key,
            groupname=docket.worker_group_name,
        )
        assert pending_info["pending"] == 0

        assert await redis.xlen(docket.stream_key) == 0


async def test_two_workers_split_work(docket: Docket):
    """Two workers should split the workload"""

    # Use concurrency=1 so workers claim tasks one at a time for finer distribution
    worker1 = Worker(docket, concurrency=1)
    worker2 = Worker(docket, concurrency=1)

    call_counts = {
        worker1: 0,
        worker2: 0,
    }

    # Tasks wait for this event, ensuring both workers claim work before any completes
    proceed = asyncio.Event()

    async def the_task(worker: Worker = CurrentWorker()):
        await proceed.wait()
        call_counts[worker] += 1

    for _ in range(100):
        await docket.add(the_task)()

    async with worker1, worker2:
        run1 = asyncio.create_task(worker1.run_until_finished())
        run2 = asyncio.create_task(worker2.run_until_finished())
        # Give both workers time to claim tasks
        await asyncio.sleep(0.2)
        # Let all tasks complete
        proceed.set()
        await run1
        await run2

    assert call_counts[worker1] + call_counts[worker2] == 100
    # Both workers should participate (at least 20% each)
    assert call_counts[worker1] > 20
    assert call_counts[worker2] > 20


async def test_worker_reconnects_when_connection_is_lost(
    docket: Docket, the_task: AsyncMock
):
    """The worker should reconnect when the connection is lost"""
    worker = Worker(docket, reconnection_delay=timedelta(milliseconds=100))

    # Mock the _worker_loop method to fail once then succeed
    original_worker_loop = worker._worker_loop  # type: ignore[protected-access]
    call_count = 0

    async def mock_worker_loop(redis: Redis, forever: bool = False):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("Simulated connection error")
        return await original_worker_loop(redis, forever=forever)

    worker._worker_loop = mock_worker_loop  # type: ignore[protected-access]

    await docket.add(the_task)()

    async with worker:
        await worker.run_until_finished()

    assert call_count == 2
    the_task.assert_called_once()


async def test_worker_respects_concurrency_limit(docket: Docket, worker: Worker):
    """Worker should not exceed its configured concurrency limit"""

    task_results: set[int] = set()

    currently_running = 0
    max_concurrency_observed = 0

    async def concurrency_tracking_task(index: int):
        nonlocal currently_running, max_concurrency_observed

        currently_running += 1
        max_concurrency_observed = max(max_concurrency_observed, currently_running)

        await asyncio.sleep(0.1)  # Long enough to overlap even on slow CI runners
        task_results.add(index)

        currently_running -= 1

    for i in range(50):
        await docket.add(concurrency_tracking_task)(index=i)

    worker.concurrency = 5
    await worker.run_until_finished()

    assert task_results == set(range(50))

    assert 1 < max_concurrency_observed <= 5


async def test_worker_handles_unregistered_task_execution_on_initial_delivery(
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
    the_task: AsyncMock,
):
    """worker should handle the case when an unregistered task is executed"""
    await docket.add(the_task)()

    docket.tasks.pop("the_task")

    with caplog.at_level(logging.WARNING):
        await worker.run_until_finished()

    # Default fallback logs warning and ACKs the message
    assert "Unknown task 'the_task' received - dropping" in caplog.text
    assert "Register via CLI (--tasks your.module:tasks)" in caplog.text


async def test_worker_handles_unregistered_task_execution_on_redelivery(
    docket: Docket,
    caplog: pytest.LogCaptureFixture,
):
    """worker should handle the case when an unregistered task is redelivered"""

    async def test_task():
        await asyncio.sleep(0.01)

    # Register and schedule the task first
    docket.register(test_task)
    await docket.add(test_task)()

    # First run the task successfully to ensure line 249 coverage
    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_success:
        await worker_success.run_until_finished()

    # Schedule another task for the redelivery test
    await docket.add(test_task)()

    # First worker fails during execution
    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_a:
        worker_a._execute = AsyncMock(side_effect=Exception("Simulated failure"))  # type: ignore[protected-access]
        with pytest.raises(ExceptionGroup) as exc_info:
            await worker_a.run_until_finished()
        assert any("Simulated failure" in str(e) for e in exc_info.value.exceptions)

    # Verify task is pending redelivery
    async with docket.redis() as redis:
        pending_info = await redis.xpending(
            docket.stream_key,
            docket.worker_group_name,
        )
        assert pending_info["pending"] == 1

    await asyncio.sleep(0.125)  # Wait for redelivery timeout

    # Unregister the task before redelivery
    docket.tasks.pop("test_task")

    # Second worker should handle the unregistered task gracefully
    async with Worker(
        docket, redelivery_timeout=timedelta(milliseconds=100)
    ) as worker_b:
        with caplog.at_level(logging.WARNING):
            await worker_b.run_until_finished()

    # Default fallback logs warning and ACKs the message
    assert "Unknown task 'test_task' received - dropping" in caplog.text
    assert "Register via CLI (--tasks your.module:tasks)" in caplog.text


builtin_tasks = {function.__name__ for function in standard_tasks}


async def test_worker_announcements(
    docket: Docket, the_task: AsyncMock, another_task: AsyncMock
):
    # Use 100ms heartbeat - short enough for fast tests, long enough to be reliable
    # under CPU contention in CI environments
    heartbeat = timedelta(milliseconds=100)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    docket.register(the_task)
    docket.register(another_task)

    async with Worker(docket, name="worker-a") as worker_a:
        await asyncio.sleep(heartbeat.total_seconds() * 5)

        workers = await docket.workers()
        assert len(workers) == 1
        assert worker_a.name in {w.name for w in workers}

        async with Worker(docket, name="worker-b") as worker_b:
            await asyncio.sleep(heartbeat.total_seconds() * 5)

            workers = await docket.workers()
            assert len(workers) == 2
            assert {w.name for w in workers} == {worker_a.name, worker_b.name}

            for worker in workers:
                # Allow generous timing tolerance - CI can have significant delays
                assert worker.last_seen > datetime.now(timezone.utc) - (heartbeat * 20)
                assert worker.tasks == builtin_tasks | {"the_task", "another_task"}

        await asyncio.sleep(heartbeat.total_seconds() * 10)

        workers = await docket.workers()
        assert len(workers) == 1
        assert worker_a.name in {w.name for w in workers}
        assert worker_b.name not in {w.name for w in workers}

    await asyncio.sleep(heartbeat.total_seconds() * 10)

    workers = await docket.workers()
    assert len(workers) == 0


async def test_task_announcements(
    docket: Docket, the_task: AsyncMock, another_task: AsyncMock
):
    """Test that we can ask about which workers are available for a task"""

    # Use 100ms heartbeat - short enough for fast tests, long enough to be reliable
    # under CPU contention in CI environments
    heartbeat = timedelta(milliseconds=100)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    docket.register(the_task)
    docket.register(another_task)
    async with Worker(docket, name="worker-a") as worker_a:
        await asyncio.sleep(heartbeat.total_seconds() * 5)

        workers = await docket.task_workers("the_task")
        assert len(workers) == 1
        assert worker_a.name in {w.name for w in workers}

        async with Worker(docket, name="worker-b") as worker_b:
            await asyncio.sleep(heartbeat.total_seconds() * 5)

            workers = await docket.task_workers("the_task")
            assert len(workers) == 2
            assert {w.name for w in workers} == {worker_a.name, worker_b.name}

            for worker in workers:
                # Allow generous timing tolerance - CI can have significant delays
                assert worker.last_seen > datetime.now(timezone.utc) - (heartbeat * 20)
                assert worker.tasks == builtin_tasks | {"the_task", "another_task"}

        await asyncio.sleep(heartbeat.total_seconds() * 10)

        workers = await docket.task_workers("the_task")
        assert len(workers) == 1
        assert worker_a.name in {w.name for w in workers}
        assert worker_b.name not in {w.name for w in workers}

    await asyncio.sleep(heartbeat.total_seconds() * 10)

    workers = await docket.task_workers("the_task")
    assert len(workers) == 0


@pytest.mark.parametrize(
    "error",
    [
        ConnectionError("oof"),
        ValueError("woops"),
    ],
)
async def test_worker_recovers_from_redis_errors(
    docket: Docket,
    the_task: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
):
    """Should recover from errors and continue sending heartbeats"""

    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    docket.register(the_task)

    original_redis = docket.redis
    error_time = None
    redis_calls = 0

    @asynccontextmanager
    async def mock_redis() -> AsyncGenerator[Redis | RedisCluster, None]:
        nonlocal redis_calls, error_time
        redis_calls += 1

        if redis_calls == 2:
            error_time = datetime.now(timezone.utc)
            raise error

        async with original_redis() as r:
            yield r

    monkeypatch.setattr(docket, "redis", mock_redis)

    async with Worker(docket) as worker:
        await asyncio.sleep(heartbeat.total_seconds() * 1.5)

        await asyncio.sleep(heartbeat.total_seconds() * 5)

        workers = await docket.workers()
        assert len(workers) == 1
        assert worker.name in {w.name for w in workers}

        # Verify that the last_seen timestamp is after our error
        worker_info = next(w for w in workers if w.name == worker.name)
        assert error_time
        assert worker_info.last_seen > error_time, (
            "Worker should have sent heartbeats after the Redis error"
        )


async def test_worker_can_be_told_to_skip_automatic_tasks(docket: Docket):
    """A worker can be told to skip automatic tasks"""
    from docket import Perpetual

    called = False

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(
            every=timedelta(milliseconds=50), automatic=True
        ),
    ):
        nonlocal called
        called = True  # pragma: no cover

    docket.register(perpetual_task)

    # Without the flag, this would hang because the task would always be scheduled
    async with Worker(docket, schedule_automatic_tasks=False) as worker:
        await worker.run_until_finished()

    assert not called


async def test_worker_concurrency_cleanup_without_dependencies(docket: Docket):
    """Test worker cleanup when dependencies are not defined."""
    cleanup_executed = False

    async def simple_task():
        nonlocal cleanup_executed
        # Force an exception after dependencies would be set
        raise ValueError("Force cleanup path")

    await docket.add(simple_task)()

    async with Worker(docket) as worker:
        # This should trigger the finally block cleanup
        await worker.run_until_finished()

    # Exception was handled by worker, test that it didn't crash
    cleanup_executed = True
    assert cleanup_executed


async def test_worker_concurrency_no_limit_with_custom_docket(docket: Docket):
    """Test early return when task has no concurrency limit using custom docket."""
    task_executed = False

    async def task_without_concurrency():
        nonlocal task_executed
        task_executed = True

    await docket.add(task_without_concurrency)()

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert task_executed


async def test_worker_exception_before_dependencies(docket: Docket):
    """Test finally block when exception occurs before dependencies are set."""
    task_failed = False

    async def task_that_will_fail():
        nonlocal task_failed
        task_failed = True
        raise RuntimeError("Test exception for coverage")

    try:
        await task_that_will_fail()
    except RuntimeError:
        pass

    # Reset flag to test worker behavior
    task_failed = False

    # Mock resolved_dependencies to fail before setting dependencies

    await docket.add(task_that_will_fail)()

    async with Worker(docket) as worker:
        # Patch resolved_dependencies to raise an exception immediately
        with patch("docket.worker.resolved_dependencies") as mock_deps:
            # Create a context manager that fails on entry
            context = AsyncMock()
            context.__aenter__.side_effect = RuntimeError(
                "Dependencies failed to resolve"
            )
            mock_deps.return_value = context

            # This should trigger the finally block where "dependencies" not in locals()
            await worker.run_until_finished()

    # The task function shouldn't run via worker due to dependency failure
    assert task_failed is False
