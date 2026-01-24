"""Tests for worker lifecycle, shutdown, and cancellation behavior."""

import asyncio
import random
import sys
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Callable
from unittest.mock import AsyncMock, patch

from redis.exceptions import ConnectionError

from docket import Docket, Worker, testing

if sys.version_info >= (3, 11):  # pragma: no cover
    from asyncio import timeout as async_timeout
else:  # pragma: no cover
    from async_timeout import timeout as async_timeout


async def test_run_forever_cancels_promptly_with_future_tasks(
    docket: Docket, the_task: AsyncMock, now: Callable[[], datetime]
):
    """run_forever() should cancel promptly even with future-scheduled tasks.

    Issue #260: Perpetual tasks block worker shutdown.
    """
    execution = await docket.add(the_task, now() + timedelta(seconds=15))()

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        worker_task = asyncio.create_task(worker.run_forever())
        await asyncio.sleep(0.05)
        worker_task.cancel()
        with suppress(asyncio.CancelledError):  # pragma: no branch
            async with async_timeout(1.0):  # pragma: no branch
                await worker_task

    the_task.assert_not_called()
    await testing.assert_task_scheduled(docket, the_task, key=execution.key)


async def test_run_until_finished_exits_promptly_with_future_tasks(
    docket: Docket, the_task: AsyncMock, now: Callable[[], datetime]
):
    """run_until_finished() should exit promptly when only future tasks exist.

    Issue #260: Perpetual tasks block worker shutdown.
    """
    execution = await docket.add(the_task, now() + timedelta(seconds=15))()

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        async with async_timeout(1.0):  # pragma: no branch
            await worker.run_until_finished()

    the_task.assert_not_called()
    await testing.assert_task_scheduled(docket, the_task, key=execution.key)


async def test_run_at_most_cancels_promptly_with_future_tasks(
    docket: Docket, the_task: AsyncMock, now: Callable[[], datetime]
):
    """run_at_most() should cancel promptly even with future-scheduled tasks.

    Issue #260: Perpetual tasks block worker shutdown.
    """
    execution = await docket.add(the_task, now() + timedelta(seconds=15))()

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        worker_task = asyncio.create_task(worker.run_at_most({execution.key: 1}))
        await asyncio.sleep(0.05)
        worker_task.cancel()
        with suppress(asyncio.CancelledError):  # pragma: no branch
            async with async_timeout(1.0):  # pragma: no branch
                await worker_task

    the_task.assert_not_called()
    await testing.assert_task_scheduled(docket, the_task, key=execution.key)


async def test_worker_aexit_completes_on_immediate_cancellation(docket: Docket):
    """Verify __aexit__ doesn't hang when worker is cancelled before setup completes.

    This tests the fix for a race condition where CancelledError during async setup
    would leave _worker_done cleared, causing __aexit__ to hang forever.
    """
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        worker_task = asyncio.create_task(worker.run_forever())
        # Cancel immediately - before setup completes
        worker_task.cancel()

        # __aexit__ should complete promptly (within 1 second)
        # Without the fix, this would hang forever
        with suppress(asyncio.CancelledError):
            async with async_timeout(1.0):  # pragma: no branch
                await worker_task


async def test_worker_done_set_after_early_cancellation(docket: Docket):
    """Verify _worker_done is set even when cancelled during setup phase."""
    worker = Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    )
    await worker.__aenter__()

    # Start worker and cancel before it can process any tasks
    worker_task = asyncio.create_task(worker.run_forever())
    await asyncio.sleep(0.01)  # Brief yield
    worker_task.cancel()

    with suppress(asyncio.CancelledError):
        await worker_task

    # Verify the event is set after the worker loop finishes
    # (must check before __aexit__ which deletes the attribute)
    assert worker._worker_done.is_set()  # pyright: ignore[reportPrivateUsage]

    # __aexit__ should complete promptly because _worker_done should be set
    async with async_timeout(1.0):  # pragma: no branch
        await worker.__aexit__(None, None, None)


async def test_worker_rapid_start_cancel_cycles(docket: Docket):
    """Verify worker handles rapid start/cancel cycles without hanging."""
    for _ in range(10):
        async with Worker(
            docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            worker_task = asyncio.create_task(worker.run_forever())
            # Random delay before cancelling
            await asyncio.sleep(random.uniform(0, 0.02))
            worker_task.cancel()

            with suppress(asyncio.CancelledError):
                async with async_timeout(1.0):  # pragma: no branch
                    await worker_task


async def test_worker_cancellation_during_setup_before_scheduler_created(
    docket: Docket,
):
    """Test cancellation during _cancellation_ready.wait() before scheduler/lease tasks exist.

    This hits the None branches in the finally block for scheduler_task and lease_renewal_task.
    """
    worker = Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    )
    await worker.__aenter__()

    # Patch _cancellation_listener to block indefinitely on _cancellation_ready
    # This ensures scheduler_task and lease_renewal_task are never created
    async def slow_listener() -> None:
        # Don't set _cancellation_ready, just wait forever
        await asyncio.Event().wait()

    with patch.object(worker, "_cancellation_listener", slow_listener):
        worker_task = asyncio.create_task(worker.run_forever())
        # Give time for the task to start and reach _cancellation_ready.wait()
        await asyncio.sleep(0.01)
        worker_task.cancel()

        with suppress(asyncio.CancelledError):
            async with async_timeout(1.0):  # pragma: no branch
                await worker_task

    # Cleanup
    async with async_timeout(1.0):  # pragma: no branch
        await worker.__aexit__(None, None, None)


async def test_cancellation_listener_handles_connection_error(docket: Docket):
    """Test that _cancellation_listener handles ConnectionError and reconnects."""
    error_handled = asyncio.Event()
    error_count = 0
    original_pubsub = docket._pubsub  # pyright: ignore[reportPrivateUsage]

    @asynccontextmanager
    async def failing_pubsub() -> AsyncGenerator[Any, None]:
        nonlocal error_count
        async with original_pubsub() as pubsub:
            original_get_message = pubsub.get_message

            async def failing_get_message(
                **kwargs: Any,
            ) -> dict[str, Any] | None:
                nonlocal error_count
                error_count += 1
                if error_count == 1:
                    raise ConnectionError("Test connection error")
                # Signal that we got past the error handler
                error_handled.set()
                return await original_get_message(**kwargs)  # pyright: ignore[reportUnknownVariableType]

            pubsub.get_message = failing_get_message  # type: ignore[method-assign]
            yield pubsub

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        with patch.object(docket, "_pubsub", failing_pubsub):
            worker_task = asyncio.create_task(worker.run_forever())
            # Wait for the error to be handled and reconnection to succeed
            async with async_timeout(5.0):  # pragma: no branch
                await error_handled.wait()
            worker_task.cancel()
            with suppress(asyncio.CancelledError):  # pragma: no branch
                await worker_task

    assert error_count >= 2  # First call raised error, second succeeded


async def test_cancellation_listener_handles_generic_exception(docket: Docket):
    """Test that _cancellation_listener handles generic Exception and continues."""
    error_handled = asyncio.Event()
    error_count = 0
    original_pubsub = docket._pubsub  # pyright: ignore[reportPrivateUsage]

    @asynccontextmanager
    async def failing_pubsub() -> AsyncGenerator[Any, None]:
        nonlocal error_count
        async with original_pubsub() as pubsub:
            original_get_message = pubsub.get_message

            async def failing_get_message(
                **kwargs: Any,
            ) -> dict[str, Any] | None:
                nonlocal error_count
                error_count += 1
                if error_count == 1:
                    raise RuntimeError("Test generic error")
                # Signal that we got past the error handler
                error_handled.set()
                return await original_get_message(**kwargs)  # pyright: ignore[reportUnknownVariableType]

            pubsub.get_message = failing_get_message  # type: ignore[method-assign]
            yield pubsub

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        with patch.object(docket, "_pubsub", failing_pubsub):
            worker_task = asyncio.create_task(worker.run_forever())
            # Wait for the error to be handled and reconnection to succeed
            async with async_timeout(5.0):  # pragma: no branch
                await error_handled.wait()
            worker_task.cancel()
            with suppress(asyncio.CancelledError):  # pragma: no branch
                await worker_task

    assert error_count >= 2  # First call raised error, second succeeded
