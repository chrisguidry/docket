"""The worker creates each task's asyncio task through the loop's task factory."""

import asyncio
from typing import Any, AsyncGenerator

import pytest

from docket import Docket, Worker


@pytest.fixture
async def tasks_from_factory() -> AsyncGenerator[set[asyncio.Task[Any]], None]:
    """Install a task factory on the running loop that records each task it makes.

    Python 3.10 calls the factory with only the loop and the coroutine.  Later
    versions can also pass keyword arguments, such as the task's name and
    context, so the factory hands them on to the Task.
    """
    made: set[asyncio.Task[Any]] = set()

    def factory(
        loop: asyncio.AbstractEventLoop, coro: Any, **kwargs: Any
    ) -> asyncio.Task[Any]:
        task = asyncio.Task(coro, loop=loop, **kwargs)
        made.add(task)
        return task

    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    loop.set_task_factory(factory)
    yield made
    loop.set_task_factory(previous)


async def test_tasks_run_in_asyncio_tasks_from_the_loop_task_factory(
    docket: Docket, worker: Worker, tasks_from_factory: set[asyncio.Task[Any]]
):
    """Each task runs in an asyncio task that the loop's task factory made.

    Tools such as Sentry and aiomonitor install a task factory to track every
    task.  A task that the worker creates another way bypasses that factory.
    """
    ran_in: list[asyncio.Task[Any] | None] = []

    async def the_task() -> None:
        ran_in.append(asyncio.current_task())

    await docket.add(the_task)()
    await worker.run_until_finished()

    assert ran_in[0] in tasks_from_factory
