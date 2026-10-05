"""Tests for self-rescheduling tasks."""

from datetime import datetime, timedelta
from typing import Callable

from docket import Docket, TaskKey, Worker


async def test_self_perpetuating_immediate_tasks(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support self-perpetuating tasks"""

    calls: dict[str, list[int]] = {
        "first": [],
        "second": [],
    }

    async def the_task(start: int, iteration: int, key: str = TaskKey()):
        calls[key].append(start + iteration)
        if iteration < 3:
            # Use replace() for self-perpetuating to allow rescheduling while running
            await docket.replace(the_task, now(), key=key)(start, iteration + 1)

    await docket.add(the_task, key="first")(10, 1)
    await docket.add(the_task, key="second")(20, 1)

    await worker.run_until_finished()

    assert calls["first"] == [11, 12, 13]
    assert calls["second"] == [21, 22, 23]


async def test_self_perpetuating_scheduled_tasks(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support self-perpetuating tasks"""

    calls: dict[str, list[int]] = {
        "first": [],
        "second": [],
    }

    async def the_task(start: int, iteration: int, key: str = TaskKey()):
        calls[key].append(start + iteration)
        if iteration < 3:
            soon = now() + timedelta(milliseconds=100)
            # Use replace() for self-perpetuating to allow rescheduling while running
            await docket.replace(the_task, key=key, when=soon)(start, iteration + 1)

    await docket.add(the_task, key="first")(10, 1)
    await docket.add(the_task, key="second")(20, 1)

    await worker.run_until_finished()

    assert calls["first"] == [11, 12, 13]
    assert calls["second"] == [21, 22, 23]


async def test_infinitely_self_perpetuating_tasks(
    docket: Docket, worker: Worker, now: Callable[[], datetime]
):
    """docket should support testing use cases for infinitely self-perpetuating tasks"""

    calls: dict[str, list[int]] = {
        "first": [],
        "second": [],
        "unaffected": [],
    }

    async def the_task(start: int, iteration: int, key: str = TaskKey()):
        calls[key].append(start + iteration)
        soon = now() + timedelta(milliseconds=100)
        # Use replace() for self-perpetuating to allow rescheduling while running
        await docket.replace(the_task, key=key, when=soon)(start, iteration + 1)

    async def unaffected_task(start: int, iteration: int, key: str = TaskKey()):
        calls[key].append(start + iteration)
        if iteration < 3:
            # Use replace() for self-perpetuating to allow rescheduling while running
            await docket.replace(unaffected_task, now(), key=key)(start, iteration + 1)

    await docket.add(the_task, key="first")(10, 1)
    await docket.add(the_task, key="second")(20, 1)
    await docket.add(unaffected_task, key="unaffected")(30, 1)

    # Using worker.run_until_finished() would hang here because the task is always
    # queueing up a future run of itself.  With worker.run_at_most(),
    # we can specify tasks keys that will only be allowed to run a limited number of
    # times, thus allowing the worker to exist cleanly.
    await worker.run_at_most({"first": 4, "second": 2})

    assert calls["first"] == [11, 12, 13, 14]
    assert calls["second"] == [21, 22]
    assert calls["unaffected"] == [31, 32, 33]
