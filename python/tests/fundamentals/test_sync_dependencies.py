"""Tests for synchronous dependencies."""

from contextlib import contextmanager
from typing import Generator
from uuid import uuid4

from docket import CurrentDocket, CurrentWorker, Depends, Docket, Worker


async def test_sync_function_dependencies(docket: Docket, worker: Worker):
    """A task can depend on the return value of sync functions"""

    def dependency_one() -> str:
        return f"one-{uuid4()}"

    def dependency_two() -> str:
        return f"two-{uuid4()}"

    called = 0

    async def dependent_task(
        one_a: str = Depends(dependency_one),
        one_b: str = Depends(dependency_one),
        two: str = Depends(dependency_two),
    ):
        assert one_a.startswith("one-")
        assert one_b == one_a

        assert two.startswith("two-")

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 1


async def test_sync_contextual_dependencies(docket: Docket, worker: Worker):
    """A task can depend on the return value of sync context managers"""

    stages: list[str] = []

    @contextmanager
    def dependency_one() -> Generator[str, None, None]:
        stages.append("one-before")
        yield f"one-{uuid4()}"
        stages.append("one-after")

    def dependency_two() -> str:
        return f"two-{uuid4()}"

    called = 0

    async def dependent_task(
        one_a: str = Depends(dependency_one),
        one_b: str = Depends(dependency_one),
        two: str = Depends(dependency_two),
    ):
        assert one_a.startswith("one-")
        assert one_b == one_a

        assert two.startswith("two-")

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 1
    assert stages == ["one-before", "one-after"]


async def test_mixed_sync_and_async_dependencies(docket: Docket, worker: Worker):
    """A task can depend on both sync and async dependencies"""

    def sync_dependency() -> str:
        return f"sync-{uuid4()}"

    async def async_dependency() -> str:
        return f"async-{uuid4()}"

    called = 0

    async def dependent_task(
        sync_val: str = Depends(sync_dependency),
        async_val: str = Depends(async_dependency),
    ):
        assert sync_val.startswith("sync-")
        assert async_val.startswith("async-")

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 1


async def test_sync_dependencies_of_dependencies(docket: Docket, worker: Worker):
    """A sync task dependency can depend on other sync dependencies"""
    counter = 0

    def dependency_one() -> list[str]:
        nonlocal counter
        counter += 1
        return [f"one-{counter}"]

    def dependency_two(my_one: list[str] = Depends(dependency_one)) -> list[str]:
        nonlocal counter
        counter += 1
        return my_one + [f"two-{counter}"]

    def dependency_three(
        my_one: list[str] = Depends(dependency_one),
        my_two: list[str] = Depends(dependency_two),
    ) -> list[str]:
        nonlocal counter
        counter += 1
        return my_one + my_two + [f"three-{counter}"]

    async def dependent_task(
        one_a: list[str] = Depends(dependency_one),
        one_b: list[str] = Depends(dependency_one),
        two: list[str] = Depends(dependency_two),
        three: list[str] = Depends(dependency_three),
    ):
        assert one_a is one_b

        assert one_a == ["one-1"]
        assert two == ["one-1", "two-2"]
        assert three == ["one-1", "two-2", "three-3"]

    await docket.add(dependent_task)()

    await worker.run_until_finished()


async def test_sync_dependencies_can_ask_for_docket_dependencies(
    docket: Docket, worker: Worker
):
    """A sync task dependency can ask for a docket dependency"""

    called = 0

    def dependency_one(this_docket: Docket = CurrentDocket()) -> str:
        assert this_docket is docket

        nonlocal called
        called += 1

        return f"one-{called}"

    def dependency_two(
        this_worker: Worker = CurrentWorker(),
        one: str = Depends(dependency_one),
    ) -> str:
        assert this_worker is worker

        assert one == "one-1"

        nonlocal called
        called += 1

        return f"two-{called}"

    async def dependent_task(
        one: str = Depends(dependency_one),
        two: str = Depends(dependency_two),
        this_docket: Docket = CurrentDocket(),
        this_worker: Worker = CurrentWorker(),
    ):
        assert one == "one-1"
        assert two == "two-2"

        assert this_docket is docket
        assert this_worker is worker

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 3


async def test_mixed_sync_async_nested_dependencies(docket: Docket, worker: Worker):
    """Dependencies can mix sync and async at different nesting levels"""
    counter = 0

    def sync_base() -> int:
        nonlocal counter
        counter += 1
        return counter

    async def async_multiplier(base: int = Depends(sync_base)) -> int:
        nonlocal counter
        counter += 1
        return base * 10

    def sync_adder(multiplied: int = Depends(async_multiplier)) -> int:
        nonlocal counter
        counter += 1
        return multiplied + 5

    async def dependent_task(result: int = Depends(sync_adder)):
        # 1 * 10 + 5 = 15
        assert result == 15

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert counter == 3
