"""Tests for Shared (worker-scoped) dependencies."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager, contextmanager
from datetime import timedelta
from typing import AsyncGenerator, Generator

import pytest

from docket import CurrentDocket, CurrentWorker, Depends, Docket, Shared, Worker


async def test_shared_dependency_is_initialized_once(docket: Docket, worker: Worker):
    """A Shared dependency initializes once at worker startup, not per-task."""
    init_count = 0

    @asynccontextmanager
    async def create_resource() -> AsyncGenerator[str, None]:
        nonlocal init_count
        init_count += 1
        yield f"resource-{init_count}"

    results: list[str] = []

    async def task_using_shared(r: str = Shared(create_resource)):
        results.append(r)

    docket.register(task_using_shared)

    await docket.add(task_using_shared)()
    await docket.add(task_using_shared)()
    await worker.run_until_finished()

    assert init_count == 1
    assert results == ["resource-1", "resource-1"]


async def test_shared_dependencies_are_same_instance(docket: Docket, worker: Worker):
    """Multiple tasks receive the exact same object instance."""

    @asynccontextmanager
    async def create_resource() -> AsyncGenerator[object, None]:
        yield object()

    instances: list[object] = []

    async def capture_instance(r: object = Shared(create_resource)):
        instances.append(r)

    docket.register(capture_instance)

    await docket.add(capture_instance)()
    await docket.add(capture_instance)()
    await worker.run_until_finished()

    assert len(instances) == 2
    assert instances[0] is instances[1]


async def test_shared_identity_is_factory_function(docket: Docket, worker: Worker):
    """Multiple Shared() calls with the same factory resolve to the same value."""
    init_count = 0

    @asynccontextmanager
    async def create_resource() -> AsyncGenerator[str, None]:
        nonlocal init_count
        init_count += 1
        yield f"resource-{init_count}"

    results: list[tuple[str, str]] = []

    async def task_a(r: str = Shared(create_resource)):
        results.append(("a", r))

    async def task_b(r: str = Shared(create_resource)):
        results.append(("b", r))

    docket.register(task_a)
    docket.register(task_b)

    await docket.add(task_a)()
    await docket.add(task_b)()
    await worker.run_until_finished()

    assert init_count == 1
    assert set(results) == {("a", "resource-1"), ("b", "resource-1")}


async def test_shared_cleanup_on_worker_exit(docket: Docket):
    """Shared resources are cleaned up when the worker exits."""
    stages: list[str] = []

    @asynccontextmanager
    async def create_resource() -> AsyncGenerator[str, None]:
        stages.append("startup")
        yield "resource"
        stages.append("shutdown")

    async def task_using_shared(r: str = Shared(create_resource)):
        stages.append("task-ran")

    async with Worker(
        docket, minimum_check_interval=timedelta(milliseconds=5)
    ) as worker:
        docket.register(task_using_shared)
        await docket.add(task_using_shared)()
        await worker.run_until_finished()
        assert stages == ["startup", "task-ran"]

    assert stages == ["startup", "task-ran", "shutdown"]


async def test_shared_depending_on_shared(docket: Docket, worker: Worker):
    """A Shared dependency can depend on another Shared dependency."""

    @asynccontextmanager
    async def create_config() -> AsyncGenerator[dict[str, str], None]:
        yield {"db_url": "postgres://localhost/test"}

    @asynccontextmanager
    async def create_pool(
        cfg: dict[str, str] = Shared(create_config),
    ) -> AsyncGenerator[str, None]:
        yield f"pool({cfg['db_url']})"

    results: list[str] = []

    async def task_using_pool(p: str = Shared(create_pool)):
        results.append(p)

    docket.register(task_using_pool)

    await docket.add(task_using_pool)()
    await worker.run_until_finished()

    assert results == ["pool(postgres://localhost/test)"]


async def test_shared_depending_on_depends(docket: Docket, worker: Worker):
    """A Shared can use Depends(), resolved once at worker scope."""
    call_count = 0

    def get_connection_string() -> str:
        nonlocal call_count
        call_count += 1
        return f"postgres://localhost/db{call_count}"

    @asynccontextmanager
    async def create_pool(
        url: str = Depends(get_connection_string),
    ) -> AsyncGenerator[str, None]:
        yield f"pool({url})"

    results: list[str] = []

    async def task_using_pool(p: str = Shared(create_pool)):
        results.append(p)

    docket.register(task_using_pool)

    await docket.add(task_using_pool)()
    await docket.add(task_using_pool)()
    await worker.run_until_finished()

    assert call_count == 1
    assert results == [
        "pool(postgres://localhost/db1)",
        "pool(postgres://localhost/db1)",
    ]


async def test_shared_can_access_current_docket_and_worker(docket: Docket):
    """Shared dependencies can use CurrentDocket and CurrentWorker."""
    captured: dict[str, str] = {}

    @asynccontextmanager
    async def create_resource(
        d: Docket = CurrentDocket(),
        w: Worker = CurrentWorker(),
    ) -> AsyncGenerator[str, None]:
        captured["docket_name"] = d.name
        captured["worker_name"] = w.name
        yield "ready"

    async def task_using_shared(r: str = Shared(create_resource)):
        pass

    async with Worker(
        docket, name="test-worker", minimum_check_interval=timedelta(milliseconds=5)
    ) as worker:
        docket.register(task_using_shared)
        await docket.add(task_using_shared)()
        await worker.run_until_finished()

    assert captured["docket_name"] == docket.name
    assert captured["worker_name"] == "test-worker"


async def test_late_registered_task_with_new_shared(docket: Docket):
    """A task registered after worker starts can introduce new Shared dependencies."""
    init_order: list[str] = []

    @asynccontextmanager
    async def create_early_resource() -> AsyncGenerator[str, None]:
        init_order.append("early")
        yield "early-resource"

    @asynccontextmanager
    async def create_late_resource() -> AsyncGenerator[str, None]:
        init_order.append("late")
        yield "late-resource"

    results: list[str] = []

    async def early_task(r: str = Shared(create_early_resource)):
        results.append(r)

    async def late_task(r: str = Shared(create_late_resource)):
        results.append(r)

    async with Worker(
        docket, minimum_check_interval=timedelta(milliseconds=5)
    ) as worker:
        docket.register(early_task)
        await docket.add(early_task)()
        await worker.run_until_finished()

        docket.register(late_task)
        await docket.add(late_task)()
        await worker.run_until_finished()

    assert "early" in init_order
    assert "late" in init_order
    assert results == ["early-resource", "late-resource"]


async def test_multiple_shared_cleanup_order(docket: Docket):
    """Multiple Shared dependencies clean up in reverse initialization order."""
    order: list[str] = []

    @asynccontextmanager
    async def create_first() -> AsyncGenerator[str, None]:
        order.append("first:start")
        yield "first"
        order.append("first:stop")

    @asynccontextmanager
    async def create_second(
        f: str = Shared(create_first),
    ) -> AsyncGenerator[str, None]:
        order.append("second:start")
        yield f"second(depends on {f})"
        order.append("second:stop")

    async def task_using_both(
        f: str = Shared(create_first),
        s: str = Shared(create_second),
    ):
        order.append("task-ran")

    async with Worker(
        docket, minimum_check_interval=timedelta(milliseconds=5)
    ) as worker:
        docket.register(task_using_both)
        await docket.add(task_using_both)()
        await worker.run_until_finished()

    assert order == [
        "first:start",
        "second:start",
        "task-ran",
        "second:stop",
        "first:stop",
    ]


async def test_shared_cleanup_on_init_failure(
    docket: Docket, caplog: pytest.LogCaptureFixture
):
    """If a Shared fails to initialize, earlier ones still clean up on worker exit.

    Since Shared dependencies are resolved lazily during task execution, init
    failures are handled as task failures (the task is marked failed but the
    worker continues). The key guarantee is that any Shared that DID initialize
    gets properly cleaned up when the worker exits. The error also appears in logs.
    """
    cleanup_called = False

    @asynccontextmanager
    async def create_good() -> AsyncGenerator[str, None]:
        try:
            yield "good"
        finally:
            nonlocal cleanup_called
            cleanup_called = True

    @asynccontextmanager
    async def create_bad(g: str = Shared(create_good)) -> AsyncGenerator[str, None]:
        raise ValueError("🦆 QUACK! The rubber duck factory exploded! 🦆")
        yield  # pragma: no cover

    async def task_using_bad(b: str = Shared(create_bad)): ...

    with caplog.at_level(logging.ERROR):
        async with Worker(
            docket, minimum_check_interval=timedelta(milliseconds=5)
        ) as worker:
            docket.register(task_using_bad)
            await docket.add(task_using_bad)()
            await worker.run_until_finished()

    # create_good was initialized before create_bad failed, and it should be
    # properly cleaned up when the worker exits
    assert cleanup_called

    # The error should appear in logs so operators can diagnose the issue
    assert "rubber duck factory exploded" in caplog.text


async def test_shared_async_function_factory(docket: Docket, worker: Worker):
    """Shared can use an async function that returns a value (not a context manager)."""
    init_count = 0

    async def load_config() -> dict[str, str]:
        nonlocal init_count
        init_count += 1
        return {"api_url": "https://api.example.com", "version": f"v{init_count}"}

    results: list[dict[str, str]] = []

    async def task_using_config(config: dict[str, str] = Shared(load_config)):
        results.append(config)

    docket.register(task_using_config)

    await docket.add(task_using_config)()
    await docket.add(task_using_config)()
    await worker.run_until_finished()

    assert init_count == 1
    assert len(results) == 2
    assert results[0] is results[1]
    assert results[0]["version"] == "v1"


async def test_shared_sync_function_factory(docket: Docket, worker: Worker):
    """Shared can use a sync function that returns a value (not a context manager)."""
    init_count = 0

    def create_config() -> dict[str, str]:
        nonlocal init_count
        init_count += 1
        return {"db_host": "localhost", "init": str(init_count)}

    results: list[dict[str, str]] = []

    async def task_using_config(config: dict[str, str] = Shared(create_config)):
        results.append(config)

    docket.register(task_using_config)

    await docket.add(task_using_config)()
    await docket.add(task_using_config)()
    await worker.run_until_finished()

    assert init_count == 1
    assert len(results) == 2
    assert results[0] is results[1]
    assert results[0]["init"] == "1"


async def test_shared_sync_context_manager_factory(docket: Docket):
    """Shared can use a sync context manager with cleanup."""
    stages: list[str] = []

    @contextmanager
    def create_resource() -> Generator[str, None, None]:
        stages.append("startup")
        yield "sync-resource"
        stages.append("shutdown")

    async def task_using_resource(r: str = Shared(create_resource)):
        stages.append(f"task-ran:{r}")

    async with Worker(
        docket, minimum_check_interval=timedelta(milliseconds=5)
    ) as worker:
        docket.register(task_using_resource)
        await docket.add(task_using_resource)()
        await worker.run_until_finished()
        assert stages == ["startup", "task-ran:sync-resource"]

    assert stages == ["startup", "task-ran:sync-resource", "shutdown"]
