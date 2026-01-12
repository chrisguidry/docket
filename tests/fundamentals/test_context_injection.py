"""Tests for CurrentDocket, CurrentWorker, CurrentExecution, and TaskKey injection."""

from datetime import datetime
from typing import Callable

from docket import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    Docket,
    Execution,
    TaskKey,
    Worker,
)


async def test_supports_requesting_current_docket(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support providing the current docket to a task"""

    called = False

    async def the_task(a: str, b: str, this_docket: Docket = CurrentDocket()):
        assert a == "a"
        assert b == "c"
        assert this_docket is docket

        nonlocal called
        called = True

    await docket.add(the_task)("a", b="c")

    await worker.run_until_finished()

    assert called


async def test_supports_requesting_current_worker(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support providing the current worker to a task"""

    called = False

    async def the_task(a: str, b: str, this_worker: Worker = CurrentWorker()):
        assert a == "a"
        assert b == "c"
        assert this_worker is worker

        nonlocal called
        called = True

    await docket.add(the_task)("a", b="c")

    await worker.run_until_finished()

    assert called


async def test_supports_requesting_current_execution(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support providing the current execution to a task"""

    called = False

    async def the_task(a: str, b: str, this_execution: Execution = CurrentExecution()):
        assert a == "a"
        assert b == "c"

        assert isinstance(this_execution, Execution)
        assert this_execution.key == "my-cool-task:123"

        nonlocal called
        called = True

    await docket.add(the_task, key="my-cool-task:123")("a", b="c")

    await worker.run_until_finished()

    assert called


async def test_supports_requesting_current_task_key(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support providing the current task key to a task"""

    called = False

    async def the_task(a: str, b: str, this_key: str = TaskKey()):
        assert a == "a"
        assert b == "c"
        assert this_key == "my-cool-task:123"

        nonlocal called
        called = True

    await docket.add(the_task, key="my-cool-task:123")("a", b="c")

    await worker.run_until_finished()

    assert called
