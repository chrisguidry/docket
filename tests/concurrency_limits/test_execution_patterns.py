"""Tests for task execution patterns under concurrency limits.

This module tests how concurrency limits affect task execution patterns:
- Task queuing and scheduling behavior
- Different scopes and customers
- Quick vs slow tasks
- Dependency injection integration
- Successful slot acquisition
"""

import asyncio
from contextvars import ContextVar

from docket import ConcurrencyLimit, CurrentWorker, Docket, Worker


async def test_worker_concurrency_limits_task_queuing_behavior(docket: Docket):
    """Test that concurrency limits control task execution properly"""

    execution_log: ContextVar[list[tuple[str, int]]] = ContextVar("execution_log")
    execution_log.set([])

    async def task_with_concurrency(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=2
        ),
    ):
        log = execution_log.get()
        log.append(("start", customer_id))
        execution_log.set(log)

        await asyncio.sleep(0.01)

        log = execution_log.get()
        log.append(("end", customer_id))
        execution_log.set(log)

    await docket.add(task_with_concurrency)(customer_id=1)
    await docket.add(task_with_concurrency)(customer_id=2)
    await docket.add(task_with_concurrency)(customer_id=1)
    await docket.add(task_with_concurrency)(customer_id=2)
    await docket.add(task_with_concurrency)(customer_id=1)

    async with Worker(docket, concurrency=5) as worker:
        await worker.run_until_finished()

    log = execution_log.get()
    start_events = [event for event in log if event[0] == "start"]
    end_events = [event for event in log if event[0] == "end"]

    customer_1_starts = len([e for e in start_events if e[1] == 1])
    customer_2_starts = len([e for e in start_events if e[1] == 2])

    assert customer_1_starts == 3
    assert customer_2_starts == 2
    assert len(start_events) == len(end_events) == 5


async def test_worker_concurrency_different_customer_branches(docket: Docket):
    """Test that different customer IDs are handled in separate branches"""
    customers_executed: set[int] = set()

    async def track_customer_execution(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        customers_executed.add(customer_id)
        await asyncio.sleep(0.01)

    for customer_id in [1, 2, 3]:
        await docket.add(track_customer_execution)(customer_id=customer_id)

    async with Worker(docket, concurrency=5) as worker:
        await worker.run_until_finished()

    assert customers_executed == {1, 2, 3}


async def test_worker_concurrency_limits_different_scopes(docket: Docket):
    """Test that concurrency limits work correctly with different scopes"""
    task_executions: list[tuple[str, int]] = []

    # Use my-application: prefix for custom scopes (allowed by ACL for user-managed keys)
    async def scoped_task(
        customer_id: int,
        scope_name: str,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1, scope="my-application:custom"
        ),
    ):
        task_executions.append((scope_name, customer_id))
        await asyncio.sleep(0.01)

    async def default_scoped_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        task_executions.append(("default", customer_id))
        await asyncio.sleep(0.01)

    await docket.add(scoped_task)(customer_id=1, scope_name="custom")
    await docket.add(default_scoped_task)(customer_id=1)

    async with Worker(docket, concurrency=5) as worker:
        await worker.run_until_finished()

    assert len(task_executions) == 2
    assert ("custom", 1) in task_executions
    assert ("default", 1) in task_executions


async def test_worker_concurrency_refresh_mechanism_integration(docket: Docket):
    """Test that concurrency refresh mechanism works in practice"""
    long_running_started = False
    quick_task_completed = False

    async def long_running_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal long_running_started
        long_running_started = True
        await asyncio.sleep(0.1)

    async def quick_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal quick_task_completed
        quick_task_completed = True

    await docket.add(long_running_task)(customer_id=1)
    await docket.add(quick_task)(customer_id=1)

    worker = Worker(docket)

    async with worker:
        await worker.run_until_finished()

    assert long_running_started
    assert quick_task_completed


async def test_worker_concurrency_with_quick_tasks(docket: Docket):
    """Test that quick tasks complete without triggering complex cleanup paths"""
    completed_tasks = 0

    async def quick_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=2
        ),
    ):
        nonlocal completed_tasks
        completed_tasks += 1

    for _ in range(5):
        await docket.add(quick_task)(customer_id=1)

    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert completed_tasks == 5


async def test_worker_concurrency_with_dependencies_integration(docket: Docket):
    """Test that concurrency limits work correctly with dependency injection"""
    task_completed = False
    current_worker_name = None

    async def task_with_dependencies(
        customer_id: int,
        worker: Worker = CurrentWorker(),
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        nonlocal task_completed, current_worker_name
        current_worker_name = worker.name
        await asyncio.sleep(0.01)
        task_completed = True

    await docket.add(task_with_dependencies)(customer_id=1)

    async with Worker(docket, name="test-worker") as worker:
        await worker.run_until_finished()

    assert task_completed
    assert current_worker_name == "test-worker"


async def test_concurrency_limited_task_successfully_acquires_slot(docket: Docket):
    """Tasks with concurrency limits successfully acquire slots when available"""

    executed: list[int] = []

    async def limited_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id",
            max_concurrent=2,
        ),
    ) -> None:
        executed.append(customer_id)
        await asyncio.sleep(0.01)

    await docket.add(limited_task)(customer_id=1)

    async with Worker(docket, concurrency=5) as worker:
        await worker.run_until_finished()

    assert executed == [1]
