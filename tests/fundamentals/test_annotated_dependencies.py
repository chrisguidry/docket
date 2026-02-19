"""Tests for Annotated[T, Depends(...)] style dependency injection."""

from contextlib import asynccontextmanager
from typing import Annotated, AsyncGenerator
from uuid import uuid4

from docket import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    Depends,
    Docket,
    Execution,
    Worker,
)


async def test_annotated_function_dependency(docket: Docket, worker: Worker):
    """A task can declare dependencies using Annotated[T, Depends(fn)] syntax."""

    async def get_greeting() -> str:
        return f"hello-{uuid4()}"

    called = False

    async def the_task(greeting: Annotated[str, Depends(get_greeting)]):
        assert greeting.startswith("hello-")

        nonlocal called
        called = True

    await docket.add(the_task)()  # pyright: ignore[reportCallIssue]
    await worker.run_until_finished()

    assert called


async def test_annotated_context_manager_dependency(docket: Docket, worker: Worker):
    """Annotated dependencies work with async context manager factories."""

    stages: list[str] = []

    @asynccontextmanager
    async def get_resource() -> AsyncGenerator[str, None]:
        stages.append("setup")
        yield "resource-value"
        stages.append("teardown")

    called = False

    async def the_task(resource: Annotated[str, Depends(get_resource)]):
        assert resource == "resource-value"

        nonlocal called
        called = True

    await docket.add(the_task)()  # pyright: ignore[reportCallIssue]
    await worker.run_until_finished()

    assert called
    assert stages == ["setup", "teardown"]


async def test_annotated_contextual_dependencies(docket: Docket, worker: Worker):
    """Annotated syntax works with contextual dependencies like CurrentDocket."""

    called = False

    async def the_task(
        a: str,
        this_docket: Annotated[Docket, CurrentDocket()],
        this_worker: Annotated[Worker, CurrentWorker()],
        this_execution: Annotated[Execution, CurrentExecution()],
    ):
        assert a == "hello"
        assert this_docket is docket
        assert this_worker is worker
        assert isinstance(this_execution, Execution)

        nonlocal called
        called = True

    await docket.add(the_task)("hello")  # pyright: ignore[reportCallIssue]
    await worker.run_until_finished()

    assert called


async def test_annotated_mixed_with_positional_args(docket: Docket, worker: Worker):
    """Annotated dependencies mix freely with regular positional arguments."""

    called = False

    async def get_config() -> dict[str, int]:
        return {"version": 2}

    async def the_task(
        name: str,
        count: int,
        config: Annotated[dict[str, int], Depends(get_config)],
    ):
        assert name == "test"
        assert count == 42
        assert config == {"version": 2}

        nonlocal called
        called = True

    await docket.add(the_task)("test", 42)  # pyright: ignore[reportCallIssue]
    await worker.run_until_finished()

    assert called


async def test_annotated_with_default_style_deps(docket: Docket, worker: Worker):
    """Annotated and default-style dependencies can coexist on the same task."""

    async def dep_a() -> str:
        return "from-annotated"

    async def dep_b() -> str:
        return "from-default"

    called = False

    async def the_task(
        a: Annotated[str, Depends(dep_a)],
        b: str = Depends(dep_b),
    ):
        assert a == "from-annotated"
        assert b == "from-default"

        nonlocal called
        called = True

    await docket.add(the_task)()  # pyright: ignore[reportCallIssue]
    await worker.run_until_finished()

    assert called


async def test_annotated_reusable_type_alias(docket: Docket, worker: Worker):
    """Annotated types can be reused as aliases across multiple tasks."""

    async def get_db() -> str:
        return "db-connection"

    DBConn = Annotated[str, Depends(get_db)]

    results: list[str] = []

    async def task_one(db: DBConn):  # pyright: ignore[reportInvalidTypeForm,reportUnknownParameterType]
        results.append(f"one:{db}")

    async def task_two(db: DBConn):  # pyright: ignore[reportInvalidTypeForm,reportUnknownParameterType]
        results.append(f"two:{db}")

    await docket.add(task_one)()  # pyright: ignore[reportCallIssue,reportUnknownArgumentType]
    await docket.add(task_two)()  # pyright: ignore[reportCallIssue,reportUnknownArgumentType]
    await worker.run_until_finished()

    assert sorted(results) == ["one:db-connection", "two:db-connection"]
