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
from docket.dependencies import Dependency
from docket.worker import _ProcessingSession  # pyright: ignore[reportPrivateUsage]

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


async def test_context_exit_interrupts_reconnection_delay(docket: Docket):
    reconnecting = asyncio.Event()

    async with Worker(
        docket,
        reconnection_delay=timedelta(seconds=30),
    ) as worker:

        async def failing_loop(redis: Any, forever: bool = False) -> None:
            reconnecting.set()
            raise ConnectionError("simulated reconnect")

        with patch.object(worker, "_worker_loop", failing_loop):
            worker_task = asyncio.create_task(worker.run_forever())
            await asyncio.wait_for(reconnecting.wait(), timeout=1.0)
            await asyncio.sleep(0.01)

    await asyncio.wait_for(worker_task, timeout=1.0)


async def test_worker_stopped_disconnect_returns_without_retry(docket: Docket):
    async with Worker(docket) as worker:

        async def stopped_loop(redis: Any, forever: bool = False) -> None:
            worker._worker_stopping.set()  # pyright: ignore[reportPrivateUsage]
            raise ConnectionError("simulated stopped reconnect")

        with patch.object(worker, "_worker_loop", stopped_loop):
            await worker.run_forever()


async def test_worker_lifecycle_skips_dependency_without_context(docket: Docket):
    events: list[str] = []

    class NoLifecycleContext(Dependency[None]):
        @classmethod
        def worker_lifecycle(cls, docket: Docket, worker: Worker) -> None:
            events.append("skipped-context")
            return None

        async def __aenter__(self) -> None:
            return None

    class TrackedLifecycleContext(Dependency[None]):
        @classmethod
        @asynccontextmanager
        async def worker_lifecycle(
            cls, docket: Docket, worker: Worker
        ) -> AsyncGenerator[None, None]:
            events.append("enter-context")
            try:
                yield
            finally:
                events.append("exit-context")

        async def __aenter__(self) -> None:
            return None

    async def the_task(
        no_context: None = NoLifecycleContext(),  # type: ignore[assignment]
        tracked: None = TrackedLifecycleContext(),  # type: ignore[assignment]
    ) -> None:
        events.append("task")

    await docket.add(the_task)()

    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    assert events == [
        "skipped-context",
        "enter-context",
        "task",
        "exit-context",
    ]


async def test_worker_rapid_start_cancel_cycles(docket: Docket):
    """Verify worker handles rapid start/cancel cycles without hanging."""
    for _ in range(10):  # pragma: no branch
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
    """Test cancellation before scheduler/lease tasks are created.

    This keeps the processing attempt before its heartbeat, scheduler, and
    lease-renewal tasks start.
    """
    worker = Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    )
    await worker.__aenter__()

    # Patch _cancellation_listener to block indefinitely before readiness.
    # This ensures scheduler_task and lease_renewal_task are never created.
    async def slow_listener() -> None:
        await asyncio.Event().wait()

    with patch.object(worker, "_cancellation_listener", slow_listener):
        worker_task = asyncio.create_task(worker.run_forever())
        # Give time for the task to start and wait on cancellation readiness.
        await asyncio.sleep(0.01)
        worker_task.cancel()

        with suppress(asyncio.CancelledError):
            async with async_timeout(1.0):  # pragma: no branch
                await worker_task

    # Cleanup
    async with async_timeout(1.0):  # pragma: no branch
        await worker.__aexit__(None, None, None)


async def test_context_exit_while_waiting_for_cancellation_readiness(docket: Docket):
    async def exercise() -> None:
        async with Worker(
            docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:

            async def slow_listener() -> None:
                session = worker._processing_session  # pyright: ignore[reportPrivateUsage]
                assert session is not None
                await session.stopping.wait()

            with patch.object(worker, "_cancellation_listener", slow_listener):
                asyncio.create_task(worker.run_forever())
                await asyncio.sleep(0.01)

    await asyncio.wait_for(exercise(), timeout=1.0)


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


async def test_cancellation_listener_stops_during_error_backoff(docket: Docket):
    async with Worker(docket) as worker:
        for error in [ConnectionError("redis down"), RuntimeError("listener error")]:
            session = _ProcessingSession(
                stopping=asyncio.Event(),
                cancellation_ready=asyncio.Event(),
            )
            worker._processing_session = session  # pyright: ignore[reportPrivateUsage]

            @asynccontextmanager
            async def failing_pubsub() -> AsyncGenerator[Any, None]:
                raise error
                yield  # pragma: no cover

            with patch.object(docket, "_pubsub", failing_pubsub):
                try:
                    listener = asyncio.create_task(
                        worker._cancellation_listener()  # pyright: ignore[reportPrivateUsage]
                    )
                    await asyncio.sleep(0.01)
                    session.stopping.set()
                    await asyncio.wait_for(listener, timeout=1.0)
                finally:
                    session.stopping.set()
                    worker._processing_session = None  # pyright: ignore[reportPrivateUsage]


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
