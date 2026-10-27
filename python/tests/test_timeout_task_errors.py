"""Tests for exceptions a task raises while a Timeout dependency runs it."""

import asyncio
import logging
from datetime import timedelta

import pytest

from docket import Docket, Retry, Timeout, Worker
from docket.execution import ExecutionState


def error_logged(caplog: pytest.LogCaptureFixture, message: str) -> bool:
    return any(
        record.levelno == logging.ERROR
        and record.exc_info is not None
        and str(record.exc_info[1]) == message
        for record in caplog.records
    )


@pytest.mark.parametrize("error_type", [ValueError, TimeoutError])
async def test_an_error_from_the_task_body_fails_the_task(
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
    error_type: type[Exception],
):
    """A TimeoutError the task raises itself fails the task, the same as a ValueError."""

    async def raises_its_own_error(
        timeout: Timeout = Timeout(timedelta(seconds=5)),
    ):
        raise error_type("the task's own error")

    execution = await docket.add(raises_its_own_error)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    await execution.sync()
    assert execution.state == ExecutionState.FAILED
    with pytest.raises(error_type, match="the task's own error"):
        await execution.get_result()
    assert error_logged(caplog, "the task's own error")


async def test_a_wait_for_timeout_inside_the_task_fails_the_task(
    docket: Docket, worker: Worker
):
    """A timed-out asyncio.wait_for inside the task fails the task.

    On Python 3.10, asyncio.TimeoutError is a separate class from the builtin
    TimeoutError, so this covers the case the builtin TimeoutError test does not.
    """

    async def waits_too_long_for_something(
        timeout: Timeout = Timeout(timedelta(seconds=5)),
    ):
        await asyncio.wait_for(asyncio.sleep(5), timeout=0.01)

    execution = await docket.add(waits_too_long_for_something)()

    await worker.run_until_finished()

    await execution.sync()
    assert execution.state == ExecutionState.FAILED
    with pytest.raises(asyncio.TimeoutError):
        await execution.get_result()


async def test_a_timeout_error_from_the_task_body_is_retried(
    docket: Docket, worker: Worker
):
    """A task's own TimeoutError triggers its Retry, the same as any other failure."""

    attempts: list[int] = []

    async def raises_timeout_error(
        retry: Retry = Retry(attempts=3),
        timeout: Timeout = Timeout(timedelta(seconds=5)),
    ):
        attempts.append(retry.attempt)
        raise TimeoutError("the task's own error")

    await docket.add(raises_timeout_error)()

    await worker.run_until_finished()

    assert attempts == [1, 2, 3]


async def test_overrunning_the_timeout_fails_the_task(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A task that runs past its Timeout is cancelled and fails with docket's timeout."""

    async def runs_too_long(
        timeout: Timeout = Timeout(timedelta(milliseconds=100)),
    ):
        await asyncio.sleep(5)

    execution = await docket.add(runs_too_long)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    await execution.sync()
    assert execution.state == ExecutionState.FAILED
    with pytest.raises(asyncio.TimeoutError, match="exceeded timeout of 0.1s"):
        await execution.get_result()
    assert error_logged(caplog, f"Docket task {execution.key} exceeded timeout of 0.1s")
