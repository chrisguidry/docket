"""Tests for async function and context manager dependencies."""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from uuid import uuid4

import pytest

from docket import (
    CurrentDocket,
    CurrentWorker,
    Depends,
    Docket,
    TaskArgument,
    Worker,
)


async def test_simple_function_dependencies(docket: Docket, worker: Worker):
    """A task can depend on the return value of simple functions"""

    async def dependency_one() -> str:
        return f"one-{uuid4()}"

    async def dependency_two() -> str:
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


async def test_contextual_dependencies(docket: Docket, worker: Worker):
    """A task can depend on the return value of async context managers"""

    stages: list[str] = []

    @asynccontextmanager
    async def dependency_one() -> AsyncGenerator[str, None]:
        stages.append("one-before")
        yield f"one-{uuid4()}"
        stages.append("one-after")

    async def dependency_two() -> str:
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


async def test_dependencies_of_dependencies(docket: Docket, worker: Worker):
    """A task dependency can depend on other dependencies"""
    counter = 0

    async def dependency_one() -> list[str]:
        nonlocal counter
        counter += 1
        return [f"one-{counter}"]

    async def dependency_two(my_one: list[str] = Depends(dependency_one)) -> list[str]:
        nonlocal counter
        counter += 1
        return my_one + [f"two-{counter}"]

    async def dependency_three(
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


async def test_dependencies_can_ask_for_docket_dependencies(
    docket: Docket, worker: Worker
):
    """A task dependency can ask for a docket dependency"""

    called = 0

    async def dependency_one(this_docket: Docket = CurrentDocket()) -> str:
        assert this_docket is docket

        nonlocal called
        called += 1

        return f"one-{called}"

    async def dependency_two(
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


async def test_dependency_failures_are_task_failures(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A task dependency failure will cause the task to fail"""

    called: bool = False

    async def dependency_one() -> str:
        raise ValueError("this one is bad")

    async def dependency_two() -> str:
        raise ValueError("and so is this one")

    async def dependent_task(
        a: str = Depends(dependency_one),
        b: str = Depends(dependency_two),
    ) -> None:
        nonlocal called
        called = True  # pragma: no cover

    await docket.add(dependent_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert not called

    assert "Failed to resolve dependencies for parameter(s): a, b" in caplog.text
    assert "ValueError: this one is bad" in caplog.text
    assert "ValueError: and so is this one" in caplog.text


async def test_contextual_dependency_before_failures_are_task_failures(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A contextual task dependency failure will cause the task to fail"""

    called: int = 0

    @asynccontextmanager
    async def dependency_before() -> AsyncGenerator[str, None]:
        raise ValueError("this one is bad")
        yield "this won't be used"  # pragma: no cover

    async def dependent_task(
        a: str = Depends(dependency_before),
    ) -> None:
        nonlocal called
        called += 1  # pragma: no cover

    await docket.add(dependent_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert not called

    assert "Failed to resolve dependencies for parameter(s): a" in caplog.text
    assert "ValueError: this one is bad" in caplog.text


async def test_contextual_dependency_after_failures_are_task_failures(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A contextual task dependency failure will cause the task to fail"""

    called: int = 0

    @asynccontextmanager
    async def dependency_after() -> AsyncGenerator[str, None]:
        yield "this will be used"
        raise ValueError("this one is bad")

    async def dependent_task(
        a: str = Depends(dependency_after),
    ) -> None:
        assert a == "this will be used"

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert called == 1

    assert "ValueError: this one is bad" in caplog.text


async def test_dependencies_can_ask_for_task_arguments(docket: Docket, worker: Worker):
    """A task dependency can ask for a task argument"""

    called = 0

    async def dependency_one(a: list[str] = TaskArgument()) -> list[str]:
        return a

    async def dependency_two(another_name: list[str] = TaskArgument("a")) -> list[str]:
        return another_name

    async def dependent_task(
        a: list[str],
        b: list[str] = TaskArgument("a"),
        c: list[str] = Depends(dependency_one),
        d: list[str] = Depends(dependency_two),
    ) -> None:
        assert a is b
        assert a is c
        assert a is d

        nonlocal called
        called += 1

    await docket.add(dependent_task)(a=["hello", "world"])

    await worker.run_until_finished()

    assert called == 1


async def test_task_arguments_may_be_optional(docket: Docket, worker: Worker):
    """A task dependency can ask for a task argument optionally"""

    called = 0

    async def dependency_one(
        a: list[str] | None = TaskArgument(optional=True),
    ) -> list[str] | None:
        return a

    async def dependent_task(
        not_a: list[str],
        b: list[str] | None = Depends(dependency_one),
    ) -> None:
        assert not_a == ["hello", "world"]
        assert b is None

        nonlocal called
        called += 1

    await docket.add(dependent_task)(not_a=["hello", "world"])

    await worker.run_until_finished()

    assert called == 1
