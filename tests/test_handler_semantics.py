"""Tests for FailureHandler and CompletionHandler behavior."""

from datetime import datetime, timedelta, timezone

from docket import Docket, ExecutionState, Worker
from docket.dependencies import Perpetual, Retry


async def test_retrying_task_is_not_marked_as_failed(docket: Docket, worker: Worker):
    """When FailureHandler schedules a retry, the task state should not be FAILED."""
    attempts = 0

    async def the_task(retry: Retry = Retry(attempts=3)):
        nonlocal attempts
        attempts += 1
        raise ValueError("fail")

    execution = await docket.add(the_task)()

    # Run just the first attempt
    await worker.run_at_most({execution.key: 1})

    # Task should be cancelled (by run_at_most), not failed
    # The key point is that during retry, state was SCHEDULED, not FAILED
    await execution.sync()
    assert execution.state == ExecutionState.CANCELLED
    assert attempts == 1


async def test_exhausted_retries_marks_task_as_failed(docket: Docket, worker: Worker):
    """When all retries are exhausted, the task state should be FAILED."""
    attempts = 0

    async def the_task(retry: Retry = Retry(attempts=2)):
        nonlocal attempts
        attempts += 1
        raise ValueError("fail")

    execution = await docket.add(the_task)()

    await worker.run_until_finished()

    await execution.sync()
    assert execution.state == ExecutionState.FAILED
    assert attempts == 2


async def test_failed_perpetual_task_is_rescheduled(docket: Docket, worker: Worker):
    """A Perpetual task that fails should still be rescheduled for next execution."""
    attempts = 0

    async def the_task(
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=10)),
    ):
        nonlocal attempts
        attempts += 1
        raise ValueError("fail")

    execution = await docket.add(the_task)()

    # Run 3 executions (all failures, but rescheduled each time)
    await worker.run_at_most({execution.key: 3})

    # Task ran 3 times despite failing each time - proves rescheduling worked
    assert attempts == 3

    # State is FAILED from the 3rd execution (run_at_most stops worker before
    # claiming the 4th execution that Perpetual scheduled)
    await execution.sync()
    assert execution.state == ExecutionState.FAILED


async def test_retry_and_perpetual_work_together(docket: Docket, worker: Worker):
    """A task can have both Retry and Perpetual - Retry handles failures first."""
    # Track: (perpetual_run, retry_attempt, succeeded)
    runs: list[tuple[int, int, bool]] = []
    perpetual_run = 0

    async def task_with_both(
        retry: Retry = Retry(attempts=2),
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=10)),
    ):
        nonlocal perpetual_run

        # First perpetual run: fail twice (exhaust retries), then perpetual reschedules
        # Second perpetual run: succeed on first attempt
        if perpetual_run == 0:
            perpetual_run = 1
        elif retry.attempt == 1 and len([r for r in runs if r[0] == 2]) == 0:
            perpetual_run = 2

        should_fail = perpetual_run == 1
        runs.append((perpetual_run, retry.attempt, not should_fail))

        if should_fail:
            raise ValueError("failing first perpetual run")

    execution = await docket.add(task_with_both)()

    # Run: 2 retries for first perpetual + 1 success for second perpetual = 3 runs
    await worker.run_at_most({execution.key: 3})

    # First perpetual run: 2 attempts, both failed
    # Second perpetual run: 1 attempt, succeeded
    assert runs == [
        (1, 1, False),  # perpetual run 1, retry 1, failed
        (1, 2, False),  # perpetual run 1, retry 2, failed (exhausted)
        (2, 1, True),  # perpetual run 2, retry 1, succeeded
    ]


async def test_perpetual_after_is_respected_on_failure(docket: Docket, worker: Worker):
    """Perpetual.after() delay is used even when the task fails."""
    run_times: list[datetime] = []

    async def failing_task(perpetual: Perpetual = Perpetual()):
        run_times.append(datetime.now(timezone.utc))
        perpetual.after(timedelta(milliseconds=100))
        raise ValueError("intentional failure")

    execution = await docket.add(failing_task)()

    await worker.run_at_most({execution.key: 2})

    assert len(run_times) == 2
    delay = run_times[1] - run_times[0]
    # Should have waited ~100ms between runs
    assert delay >= timedelta(milliseconds=50)
