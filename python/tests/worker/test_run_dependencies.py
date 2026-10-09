"""Worker.run installs worker dependencies while loading task collections."""

import sys
from contextlib import asynccontextmanager
from contextvars import ContextVar
from types import ModuleType
from typing import AsyncGenerator
from uuid import uuid4

import pytest

from docket import CurrentExecution, Depends, Docket, Execution, Worker
from docket.execution import ExecutionState


@pytest.mark.parametrize("collection_kind", ["mapping", "list", "tuple"])
@pytest.mark.parametrize("by_path", [False, True])
async def test_run_dependencies_bracket_tasks(
    monkeypatch: pytest.MonkeyPatch, collection_kind: str, by_path: bool
) -> None:
    events: list[tuple[str, str]] = []
    task_context: ContextVar[str] = ContextVar("task_context")

    @asynccontextmanager
    async def logging_context(
        execution: Execution = CurrentExecution(),
    ) -> AsyncGenerator[None, None]:
        assert task_context.get(None) is None
        events.append(("enter", execution.key))
        token = task_context.set(execution.key)
        try:
            yield
        finally:
            events.append(("exit", execution.key))
            task_context.reset(token)

    async def task(fail: bool) -> None:
        events.append(("task", task_context.get()))
        if fail:
            raise ValueError("task failed")

    dependency = Depends(logging_context)
    if collection_kind == "mapping":
        dependencies = {"logging": dependency}
    elif collection_kind == "tuple":
        dependencies = (dependency,)
    else:
        dependencies = [dependency]

    module = ModuleType("run_dependencies_fixture")
    setattr(module, "tasks", [task])
    setattr(module, "dependencies", dependencies)
    monkeypatch.setitem(sys.modules, module.__name__, module)

    url = f"memory://{uuid4()}"
    async with Docket(name="run-dependencies", url=url) as docket:
        successful = await docket.add(task, key="successful")(False)
        failed = await docket.add(task, key="failed")(True)

        await Worker.run(
            docket_name=docket.name,
            url=url,
            tasks=[f"{module.__name__}:tasks"],
            dependencies=f"{module.__name__}:dependencies" if by_path else dependencies,
            concurrency=1,
            schedule_automatic_tasks=False,
            until_finished=True,
        )

        await successful.sync()
        await failed.sync()

    for key in ("successful", "failed"):
        assert [event for event in events if event[1] == key] == [
            ("enter", key),
            ("task", key),
            ("exit", key),
        ]
    assert task_context.get(None) is None
    assert successful.state == ExecutionState.COMPLETED
    assert failed.state == ExecutionState.FAILED


@pytest.mark.parametrize(
    ("path", "error"),
    [
        ("missing_worker_dependencies:dependencies", ModuleNotFoundError),
        ("docket.tasks:missing_dependencies", AttributeError),
    ],
)
async def test_run_dependency_import_errors(path: str, error: type[Exception]) -> None:
    with pytest.raises(error):
        await Worker.run(url="memory://", dependencies=path)


async def test_run_treats_an_empty_dependency_path_as_none() -> None:
    """An empty --dependencies or DOCKET_WORKER_DEPENDENCIES means no
    dependencies, as an empty --fallback-task means no fallback task."""
    await Worker.run(url="memory://", dependencies="", until_finished=True)


@pytest.mark.parametrize("by_path", [False, True])
async def test_run_rejects_bare_dependency_functions(
    monkeypatch: pytest.MonkeyPatch, by_path: bool
) -> None:
    def dependency() -> None: ...

    module = ModuleType("invalid_dependencies_fixture")
    setattr(module, "dependencies", [dependency])
    monkeypatch.setitem(sys.modules, module.__name__, module)

    with pytest.raises(TypeError, match="must be a Dependency instance"):
        await Worker.run(
            url=f"memory://{uuid4()}",
            tasks=[],
            dependencies=f"{module.__name__}:dependencies" if by_path else [dependency],
            schedule_automatic_tasks=False,
            until_finished=True,
        )
