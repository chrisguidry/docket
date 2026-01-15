"""Tests for worker Redis cleanup, consumer groups, and bootstrap behavior."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Callable

import pytest

from docket import CurrentWorker, Docket, Worker
from tests._key_leak_checker import KeyCountChecker


async def test_redis_key_cleanup_successful_task(
    docket: Docket, worker: Worker, key_leak_checker: KeyCountChecker
) -> None:
    """Test that Redis keys are properly cleaned up after successful task execution.

    After execution, a tombstone (runs hash) with COMPLETED state remains with TTL.
    The autouse key_leak_checker fixture verifies no leaks automatically.
    """
    # Create and register a simple task
    task_executed = False

    async def successful_task():
        nonlocal task_executed
        task_executed = True
        await asyncio.sleep(0.01)  # Small delay to ensure proper execution flow

    docket.register(successful_task)

    # Schedule and execute the task
    await docket.add(successful_task)()
    await worker.run_until_finished()

    # Verify task executed successfully
    assert task_executed, "Task should have executed successfully"

    # The autouse key_leak_checker fixture will verify no leaks on teardown


async def test_redis_key_cleanup_failed_task(
    docket: Docket, worker: Worker, key_leak_checker: KeyCountChecker
) -> None:
    """Test that Redis keys are properly cleaned up after failed task execution.

    After failure, a tombstone (runs hash) with FAILED state remains with TTL.
    The autouse key_leak_checker fixture verifies no leaks automatically.
    """
    # Create a task that will fail
    task_attempted = False

    async def failing_task():
        nonlocal task_attempted
        task_attempted = True
        raise ValueError("Intentional test failure")

    docket.register(failing_task)

    # Schedule and execute the task (should fail)
    await docket.add(failing_task)()
    await worker.run_until_finished()

    # Verify task was attempted
    assert task_attempted, "Task should have been attempted"

    # The autouse key_leak_checker fixture will verify no leaks on teardown


async def test_redis_key_cleanup_cancelled_task(
    docket: Docket, worker: Worker, key_leak_checker: KeyCountChecker
) -> None:
    """Test that Redis keys are properly cleaned up after task cancellation.

    After cancellation, a tombstone (runs hash) with CANCELLED state remains with TTL
    to support the claim check pattern via get_execution(). All other keys (queue,
    parked data, etc.) are cleaned up. The autouse key_leak_checker fixture verifies
    no leaks automatically.
    """
    from docket.execution import ExecutionState

    # Create a task that won't be executed
    task_executed = False

    async def task_to_cancel():
        nonlocal task_executed
        task_executed = True  # pragma: no cover

    docket.register(task_to_cancel)

    # Schedule the task for future execution
    future_time = datetime.now(timezone.utc) + timedelta(seconds=10)
    execution = await docket.add(task_to_cancel, future_time)()

    # Cancel the task
    await docket.cancel(execution.key)

    # Run worker to process any cleanup
    await worker.run_until_finished()

    # Verify task was not executed
    assert not task_executed, "Task should not have been executed after cancellation"

    # Verify tombstone exists with CANCELLED state
    retrieved = await docket.get_execution(execution.key)
    assert retrieved is not None, "Tombstone should exist after cancellation"
    assert retrieved.state == ExecutionState.CANCELLED

    # The autouse key_leak_checker fixture will verify no leaks on teardown


async def test_verify_remaining_keys_have_ttl_detects_leaks(
    redis_url: str, docket: Docket, worker: Worker, key_leak_checker: KeyCountChecker
) -> None:
    """Test that verify_remaining_keys_have_ttl properly detects keys without TTL."""
    leak_key = docket.key("test-leak")

    # Exempt the leak from autouse checker
    key_leak_checker.add_exemption(leak_key)

    async with docket.redis() as redis:
        # Intentionally create a key without TTL (simulating a memory leak)
        await redis.set(leak_key, "leaked-value")

        # Remove exemption and manually verify it would detect the leak
        key_leak_checker.exemptions.remove(leak_key)
        with pytest.raises(AssertionError, match="Memory leak detected"):
            await key_leak_checker.verify_remaining_keys_have_ttl()

        # Clean up
        await redis.delete(leak_key)


async def test_consumer_group_created_on_first_worker_read(
    redis_url: str, make_docket_name: Callable[[], str]
):
    """Consumer group should be created when worker first tries to read.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    docket = Docket(name=make_docket_name(), url=redis_url)

    async def dummy_task():
        pass

    async with docket:
        docket.register(dummy_task)

        await docket.add(dummy_task)()

        async with docket.redis() as redis:
            assert await redis.exists(docket.stream_key)
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 0, "Consumer group should not exist before worker"

        async with Worker(
            docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            await worker.run_until_finished()

        async with docket.redis() as redis:
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 1
            assert groups[0]["name"] == docket.worker_group_name.encode()


async def test_multiple_workers_racing_to_create_group(
    redis_url: str, make_docket_name: Callable[[], str]
):
    """Multiple workers starting simultaneously should all succeed.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    docket = Docket(name=make_docket_name(), url=redis_url)
    call_counts: dict[str, int] = {}

    async def counting_task(worker: Worker = CurrentWorker()):
        call_counts[worker.name] = call_counts.get(worker.name, 0) + 1

    async with docket:
        docket.register(counting_task)

        for _ in range(20):
            await docket.add(counting_task)()

        workers = [
            Worker(
                docket,
                minimum_check_interval=timedelta(milliseconds=5),
                scheduling_resolution=timedelta(milliseconds=5),
            )
            for _ in range(5)
        ]

        for w in workers:
            await w.__aenter__()

        await asyncio.gather(*[w.run_until_finished() for w in workers])

        for w in workers:
            await w.__aexit__(None, None, None)

        total_calls = sum(call_counts.values())
        assert total_calls == 20

        async with docket.redis() as redis:
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 1


async def test_worker_handles_nogroup_error_gracefully(
    redis_url: str, make_docket_name: Callable[[], str]
):
    """Worker should handle NOGROUP error and create group automatically.

    Issue #206: Lazy stream/consumer group bootstrap.
    """
    docket = Docket(name=make_docket_name(), url=redis_url)
    task_executed = False

    async def simple_task():
        nonlocal task_executed
        task_executed = True

    async with docket:
        docket.register(simple_task)

        await docket.add(simple_task)()

        async with docket.redis() as redis:
            groups = await redis.xinfo_groups(docket.stream_key)
            assert len(groups) == 0

        async with Worker(
            docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            await worker.run_until_finished()

        assert task_executed, "Task should have been executed"


async def test_worker_handles_nogroup_in_xreadgroup(
    redis_url: str,
    make_docket_name: Callable[[], str],
    caplog: pytest.LogCaptureFixture,
):
    """Worker should handle NOGROUP error in xreadgroup and retry.

    Issue #206: Lazy stream/consumer group bootstrap.

    This tests the rare case where xautoclaim succeeds but then xreadgroup
    gets NOGROUP (e.g., if the group was deleted between the two calls).
    """
    from unittest.mock import patch

    import redis.asyncio
    from redis.exceptions import ResponseError

    docket = Docket(name=make_docket_name(), url=redis_url)
    task_executed = False

    async def simple_task():
        nonlocal task_executed
        task_executed = True

    async with docket:
        docket.register(simple_task)

        # Add a task so the worker has something to process
        await docket.add(simple_task)()

        # Ensure group exists first so xautoclaim won't hit NOGROUP
        await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]

        # Track how many times xreadgroup is called
        call_count = 0
        original_redis_xreadgroup = redis.asyncio.Redis.xreadgroup
        original_cluster_xreadgroup = redis.asyncio.RedisCluster.xreadgroup

        async def mock_redis_xreadgroup(  # pragma: no cover  # pyright: ignore[reportUnknownParameterType]
            self: redis.asyncio.Redis,  # type: ignore[type-arg]
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ResponseError("NOGROUP No such key or consumer group")
            return await original_redis_xreadgroup(self, *args, **kwargs)  # type: ignore[arg-type]

        async def mock_cluster_xreadgroup(  # pragma: no cover  # pyright: ignore[reportUnknownParameterType]
            self: redis.asyncio.RedisCluster,  # type: ignore[type-arg]
            *args: object,
            **kwargs: object,
        ) -> object:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ResponseError("NOGROUP No such key or consumer group")
            return await original_cluster_xreadgroup(self, *args, **kwargs)  # type: ignore[arg-type]

        with (
            patch.object(redis.asyncio.Redis, "xreadgroup", mock_redis_xreadgroup),
            patch.object(
                redis.asyncio.RedisCluster, "xreadgroup", mock_cluster_xreadgroup
            ),
            caplog.at_level(logging.DEBUG),
        ):
            async with Worker(
                docket,
                minimum_check_interval=timedelta(milliseconds=5),
                scheduling_resolution=timedelta(milliseconds=5),
            ) as worker:
                await worker.run_until_finished()

        # Task should have executed after NOGROUP was handled
        assert task_executed
        # Should have called xreadgroup at least twice (once NOGROUP, then success)
        assert call_count >= 2
