import asyncio
from datetime import timedelta

from docket import CurrentExecution, Docket, Worker
from docket.dependencies import Retry
from docket.execution import Execution


async def test_retry_does_not_duplicate_task_near_redelivery_timeout(docket: Docket):
    """Test that tasks timing out at redelivery_timeout don't cause duplicate executions.

    When a task exceeds the implicit timeout (derived from redelivery_timeout), Docket
    cancels it and schedules a retry. Without the fix, there was a race condition
    (https://github.com/chrisguidry/docket/issues/246):

    1. Task times out at implicit_timeout (slightly before redelivery_timeout)
    2. _retry_if_requested() schedules retry via schedule(replace=True)
    3. This adds a NEW message to the stream
    4. Original message not yet ACKed (happens after _execute returns)
    5. XAUTOCLAIM sees original as idle and redelivers it
    6. Result: duplicate execution of attempt 1

    The fix has two parts:
    1. Retry uses atomic reschedule_message to ACK+reschedule in one operation
    2. Implicit timeout is set slightly below redelivery_timeout to give margin
       for cleanup before XAUTOCLAIM can fire
    """
    executions: list[tuple[int, str]] = []  # (attempt, task_key)

    async def task_that_times_out_then_succeeds(
        execution: Execution = CurrentExecution(),
        retry: Retry = Retry(attempts=3, delay=timedelta(milliseconds=50)),
    ) -> None:
        executions.append((execution.attempt, execution.key))

        # First attempt: sleep longer than redelivery_timeout to trigger implicit timeout
        if execution.attempt == 1:
            await asyncio.sleep(1.0)  # Much longer than 200ms redelivery_timeout

        # Subsequent attempts succeed quickly
        await asyncio.sleep(0.01)

    # Schedule multiple tasks to increase chances of hitting the race
    for _ in range(5):
        await docket.add(task_that_times_out_then_succeeds)()

    # Short redelivery_timeout to make the implicit timeout fire quickly
    async with Worker(
        docket,
        concurrency=5,
        redelivery_timeout=timedelta(milliseconds=200),
        minimum_check_interval=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    # Group executions by task key
    by_key: dict[str, list[int]] = {}
    for attempt, key in executions:
        by_key.setdefault(key, []).append(attempt)

    # Should have exactly 5 unique task keys
    assert len(by_key) == 5, (
        f"Expected 5 unique task keys, got {len(by_key)}: {list(by_key.keys())}"
    )

    # Each task should have exactly 2 attempts: [1, 2]
    # If we see [1, 1, 2] or similar, that indicates duplicate delivery from the race
    for key, attempts in by_key.items():
        assert attempts == [1, 2], (
            f"Task {key}: expected attempts [1, 2], got {attempts}. "
            f"If duplicate attempt 1s, race condition caused duplicate delivery."
        )

    # Verify cleanup
    async with docket.redis() as redis:
        pending_info = await redis.xpending(
            name=docket.stream_key,
            groupname=docket.worker_group_name,
        )
        assert pending_info["pending"] == 0, "Found unacknowledged messages"
        assert await redis.xlen(docket.stream_key) == 0, "Stream not empty"
