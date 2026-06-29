"""Tests for worker lifecycle, shutdown, and cancellation behavior."""

import asyncio
import sys
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Callable, cast
from unittest.mock import AsyncMock, patch

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError

from docket import Docket, Worker, testing
from docket.worker import _WorkerPollStopped  # pyright: ignore[reportPrivateUsage]
from tests._container import BASE_VERSION, CLUSTER_ENABLED

if sys.version_info >= (3, 11):  # pragma: no cover
    from asyncio import timeout as async_timeout
else:  # pragma: no cover
    from async_timeout import timeout as async_timeout


WORKER_SHUTDOWN_TIMEOUT_SECONDS = 10.0


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
            async with async_timeout(  # pragma: no branch
                WORKER_SHUTDOWN_TIMEOUT_SECONDS
            ):
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
        async with async_timeout(WORKER_SHUTDOWN_TIMEOUT_SECONDS):  # pragma: no branch
            await worker.run_until_finished()

    the_task.assert_not_called()
    await testing.assert_task_scheduled(docket, the_task, key=execution.key)


async def test_poll_wait_stops_when_worker_stop_event_wins(docket: Docket):
    async def never_finishes() -> None:
        await asyncio.Event().wait()

    async with Worker(docket) as worker:
        stop_event = asyncio.Event()
        stop_event.set()

        with pytest.raises(_WorkerPollStopped):
            await worker._await_poll_result(  # pyright: ignore[reportPrivateUsage]
                never_finishes(),
                cast(Any, object()),
                stop_event,
            )


async def test_blocked_polling_client_close_path_is_exercised(docket: Docket):
    closed = False

    class ClosingRedis(Redis):
        async def aclose(
            self, close_connection_pool: bool | None = None
        ) -> None:  # pragma: no cover
            nonlocal closed
            closed = True
            await super().aclose(close_connection_pool=close_connection_pool)

    async with Worker(docket) as worker:
        await worker._close_blocked_polling_client(  # pyright: ignore[reportPrivateUsage]
            ClosingRedis(host="localhost")
        )

    assert closed


async def test_worker_loop_exits_when_poll_stop_is_reported(docket: Docket):
    async def registered_task() -> None: ...

    docket.register(registered_task)

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:

        async def stop_poll(
            awaitable: Any,
            redis: Any,
            stop_event: asyncio.Event,
        ) -> Any:
            del redis, stop_event
            close = getattr(awaitable, "close", None)
            if close is not None:  # pragma: no branch
                close()
            raise _WorkerPollStopped()

        with patch.object(worker, "_await_poll_result", stop_poll):
            await asyncio.wait_for(worker.run_forever(), timeout=2.0)


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
            async with async_timeout(  # pragma: no branch
                WORKER_SHUTDOWN_TIMEOUT_SECONDS
            ):
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

        # __aexit__ should complete promptly.
        # Without the fix, this would hang forever
        with suppress(asyncio.CancelledError):
            async with async_timeout(  # pragma: no branch
                WORKER_SHUTDOWN_TIMEOUT_SECONDS
            ):
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
        async with async_timeout(WORKER_SHUTDOWN_TIMEOUT_SECONDS):  # pragma: no branch
            await worker_task

    # Verify the event is set after the worker loop finishes
    # (must check before __aexit__ which deletes the attribute)
    assert worker._worker_done.is_set()  # pyright: ignore[reportPrivateUsage]

    # __aexit__ should complete promptly because _worker_done should be set
    async with async_timeout(WORKER_SHUTDOWN_TIMEOUT_SECONDS):  # pragma: no branch
        await worker.__aexit__(None, None, None)


@pytest.mark.skipif(
    CLUSTER_ENABLED or BASE_VERSION == "memory",
    reason="requires real non-cluster Redis polling cancellation behavior",
)
async def test_worker_rapid_start_cancel_cycles(docket: Docket):
    """Verify worker handles rapid start/cancel cycles without hanging."""
    for cancel_delay in (0, 0.001, 0.005, 0.02):  # pragma: no branch
        async with Worker(
            docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            worker_task = asyncio.create_task(worker.run_forever())
            await asyncio.sleep(cancel_delay)
            worker_task.cancel()

            with suppress(asyncio.CancelledError):
                await asyncio.wait_for(
                    worker_task,
                    timeout=WORKER_SHUTDOWN_TIMEOUT_SECONDS,
                )


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
            async with async_timeout(
                WORKER_SHUTDOWN_TIMEOUT_SECONDS
            ):  # pragma: no branch
                await worker_task

    # Cleanup
    async with async_timeout(WORKER_SHUTDOWN_TIMEOUT_SECONDS):  # pragma: no branch
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
                return await original_get_message(**kwargs)

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

        assert error_count >= 2


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
                return await original_get_message(**kwargs)

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

        assert error_count >= 2


async def test_worker_drains_active_tasks_on_shutdown(docket: Docket):
    """Active tasks are gathered and processed in the finally block at shutdown.

    Uses an event handshake so the task is guaranteed to still be running
    when the worker is cancelled. The finally block's asyncio.gather waits
    for the task, and we release it from a separate coroutine.
    """
    task_started = asyncio.Event()
    task_can_finish = asyncio.Event()
    task_drained = False

    async def blocking_task():
        nonlocal task_drained
        task_started.set()
        await task_can_finish.wait()
        task_drained = True

    await docket.add(blocking_task)()

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        worker_task = asyncio.create_task(worker.run_forever())
        await task_started.wait()

        worker_task.cancel()

        # Release the task after the cancel propagates — the finally
        # block's gather is waiting for this task to complete
        async def release_task():
            await asyncio.sleep(0.05)
            task_can_finish.set()

        release = asyncio.create_task(release_task())
        with suppress(asyncio.CancelledError):  # pragma: no branch
            async with async_timeout(5.0):  # pragma: no branch
                await worker_task
        await release

        assert task_drained
