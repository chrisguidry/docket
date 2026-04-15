"""Tests for Worker-level automatic task dependencies.

Worker-level dependencies are registered via ``Worker(dependencies={...})``
and run around every task that worker executes — the analogue of FastAPI
router dependencies / middleware. They use the same uncalled-for factory
shapes as ``Depends(...)``: sync fn, async fn, sync context manager, async
context manager. They can declare their own parameters (``TaskKey``,
``TaskArgument``, ``CurrentWorker``, ``Depends(...)``) which resolve
recursively against the same per-task cache as task-level dependencies.
"""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from datetime import timedelta
from typing import AsyncIterator, Iterator

import pytest

from docket import Docket, Worker
from docket.dependencies import (
    AdmissionBlocked,
    CurrentWorker,
    Depends,
    Retry,
    TaskArgument,
    TaskKey,
)


FAST = timedelta(milliseconds=5)


async def test_sync_function_worker_dependency_runs_before_task(docket: Docket):
    events: list[str] = []

    def setup_sync() -> str:
        events.append("sync_setup")
        return "sync_value"

    async def the_task() -> None:
        events.append("task")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"sync": setup_sync},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert events == ["sync_setup", "task"]


async def test_async_function_worker_dependency_runs_before_task(docket: Docket):
    events: list[str] = []

    async def setup_async() -> str:
        events.append("async_setup")
        return "async_value"

    async def the_task() -> None:
        events.append("task")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"a": setup_async},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert events == ["async_setup", "task"]


async def test_sync_context_manager_worker_dependency_brackets_task(docket: Docket):
    events: list[str] = []

    @contextmanager
    def bracket() -> Iterator[str]:
        events.append("enter")
        try:
            yield "value"
        finally:
            events.append("exit")

    async def the_task() -> None:
        events.append("task")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"b": bracket},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert events == ["enter", "task", "exit"]


async def test_async_context_manager_worker_dependency_brackets_task(docket: Docket):
    events: list[str] = []

    @asynccontextmanager
    async def bracket() -> AsyncIterator[str]:
        events.append("enter")
        try:
            yield "value"
        finally:
            events.append("exit")

    async def the_task() -> None:
        events.append("task")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"b": bracket},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert events == ["enter", "task", "exit"]


async def test_multiple_worker_dependencies_teardown_lifo(docket: Docket):
    events: list[str] = []

    def make(name: str):
        @asynccontextmanager
        async def dep() -> AsyncIterator[None]:
            events.append(f"enter_{name}")
            try:
                yield None
            finally:
                events.append(f"exit_{name}")

        return dep

    async def the_task() -> None:
        events.append("task")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"a": make("a"), "b": make("b"), "c": make("c")},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert events == [
        "enter_a",
        "enter_b",
        "enter_c",
        "task",
        "exit_c",
        "exit_b",
        "exit_a",
    ]


async def test_worker_dependency_can_consume_task_key_and_argument(docket: Docket):
    seen: dict[str, object] = {}

    async def capture(
        key: str = TaskKey(),
        name: str = TaskArgument("name"),
        current: Worker = CurrentWorker(),
    ) -> None:
        seen["key"] = key
        seen["name"] = name
        seen["worker_is_current"] = current

    async def the_task(name: str) -> None:
        seen["task_name"] = name

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"cap": capture},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task, key="my-key")("alice")
        await worker.run_until_finished()

    assert seen["key"] == "my-key"
    assert seen["name"] == "alice"
    assert seen["worker_is_current"] is worker
    assert seen["task_name"] == "alice"


async def test_worker_dependency_can_use_nested_depends(docket: Docket):
    seen: list[int] = []

    def provide_number() -> int:
        return 42

    async def consume(n: int = Depends(provide_number)) -> None:
        seen.append(n)

    async def the_task() -> None:
        seen.append(-1)

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"c": consume},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert seen == [42, -1]


async def test_worker_dep_and_task_depends_share_cache(docket: Docket):
    calls: list[str] = []

    def factory() -> object:
        calls.append("factory")
        return object()

    observed: dict[str, object] = {}

    async def worker_side(obj: object = Depends(factory)) -> None:
        observed["worker"] = obj

    async def the_task(obj: object = Depends(factory)) -> None:
        observed["task"] = obj

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"w": worker_side},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert len(calls) == 1
    assert observed["worker"] is observed["task"]


async def test_worker_dep_setup_failure_fails_task_without_running_body(
    docket: Docket,
):
    body_ran = False

    def broken() -> None:
        raise RuntimeError("worker dep broke")

    async def the_task(retry: Retry = Retry(attempts=2)) -> None:
        nonlocal body_ran  # pragma: no cover - body must not run
        body_ran = True  # pragma: no cover

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"db": broken},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert body_ran is False


async def test_worker_dep_failure_names_appear_in_exception_group(
    docket: Docket, caplog: pytest.LogCaptureFixture
):
    import logging

    def broken() -> None:
        raise RuntimeError("kaboom")

    async def the_task() -> None:  # pragma: no cover - never runs
        pass

    docket.register(the_task)

    with caplog.at_level(logging.ERROR, logger="docket"):
        async with Worker(
            docket,
            dependencies={"db_pool": broken},
            minimum_check_interval=FAST,
            scheduling_resolution=FAST,
        ) as worker:
            await docket.add(the_task)()
            await worker.run_until_finished()

    assert "db_pool" in caplog.text


async def test_worker_dep_admission_blocked_reschedules(docket: Docket):
    attempts: list[str] = []

    async def the_task(key: str = TaskKey()) -> None:
        attempts.append(key)

    docket.register(the_task)

    count = {"n": 0}

    async def gate(execution_key: str = TaskKey()) -> None:
        count["n"] += 1
        if count["n"] == 1:
            from docket.dependencies._base import current_execution

            raise AdmissionBlocked(
                current_execution.get(),
                reason="not yet",
                retry_delay=timedelta(milliseconds=5),
            )

    async with Worker(
        docket,
        dependencies={"gate": gate},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task, key="gated-task")()
        await worker.run_until_finished()

    assert attempts == ["gated-task"]
    assert count["n"] >= 2


async def test_worker_dep_teardown_error_fails_task(docket: Docket):
    events: list[str] = []

    @asynccontextmanager
    async def bad_exit() -> AsyncIterator[None]:
        events.append("enter")
        yield None
        events.append("exit")
        raise RuntimeError("teardown kaboom")

    async def the_task() -> None:
        events.append("task")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies={"b": bad_exit},
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert events[:3] == ["enter", "task", "exit"]


async def test_default_empty_dependencies_is_noop(docket: Docket):
    called = False

    async def the_task() -> None:
        nonlocal called
        called = True

    docket.register(the_task)

    async with Worker(
        docket, minimum_check_interval=FAST, scheduling_resolution=FAST
    ) as worker:
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert called


async def test_reserved_name_rejected_at_construction(docket: Docket):
    with pytest.raises(ValueError, match="reserved"):
        Worker(docket, dependencies={"__secret": lambda: None})


async def test_non_callable_factory_rejected_at_construction(docket: Docket):
    with pytest.raises(TypeError):
        Worker(docket, dependencies={"bad": "not a function"})  # type: ignore[dict-item]


async def test_list_form_runs_in_order_with_internal_names(docket: Docket):
    events: list[str] = []

    async def tracing() -> None:
        events.append("tracing")

    async def auditing() -> None:
        events.append("auditing")

    async def the_task() -> None:
        events.append("task")

    docket.register(the_task)

    async with Worker(
        docket,
        dependencies=[tracing, auditing],
        minimum_check_interval=FAST,
        scheduling_resolution=FAST,
    ) as worker:
        assert list(worker.dependencies.keys()) == [
            "__worker_dep_0__",
            "__worker_dep_1__",
        ]
        await docket.add(the_task)()
        await worker.run_until_finished()

    assert events == ["tracing", "auditing", "task"]
