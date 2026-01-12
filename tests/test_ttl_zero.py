import asyncio
from datetime import datetime, timedelta, timezone
from typing import Callable

import pytest

from docket import Docket, Worker


async def test_task_executes_with_ttl_zero(zero_ttl_docket: Docket) -> None:
    """Tasks should execute successfully when execution_ttl is set to 0."""
    executed: list[int] = []

    async def simple_task(value: int) -> int:
        executed.append(value)
        return value * 2

    zero_ttl_docket.register(simple_task)

    async with Worker(docket=zero_ttl_docket) as worker:
        execution = await zero_ttl_docket.add(simple_task)(42)
        await worker.run_until_finished()

        assert executed == [42]
        assert execution.key is not None


async def test_state_record_expires_immediately_with_ttl_zero(
    zero_ttl_docket: Docket,
) -> None:
    """State records should be deleted immediately when execution_ttl is 0."""

    async def simple_task() -> str:
        return "done"

    zero_ttl_docket.register(simple_task)

    async with Worker(docket=zero_ttl_docket) as worker:
        execution = await zero_ttl_docket.add(simple_task)()
        await worker.run_until_finished()

        # Verify state record was deleted
        state = await execution.sync()
        assert state is None, "State should be None after TTL=0 deletion"

        # Verify no state records exist in Redis
        async with zero_ttl_docket.redis() as redis:  # pragma: no branch
            keys = await redis.keys(f"{zero_ttl_docket.name}:runs:*")  # type: ignore
            assert len(keys) == 0, f"Should have no state records, found {len(keys)}"


async def test_result_storage_with_ttl_zero(zero_ttl_docket: Docket) -> None:
    """Results should be stored with TTL of 0 when execution_ttl is 0."""

    async def task_with_result() -> str:
        return "result"

    zero_ttl_docket.register(task_with_result)

    async with Worker(docket=zero_ttl_docket) as worker:
        execution = await zero_ttl_docket.add(task_with_result)()
        await worker.run_until_finished()

        # With TTL=0, the result expires immediately
        # Attempting to get it should timeout
        deadline = datetime.now(timezone.utc) + timedelta(seconds=0.1)
        with pytest.raises(TimeoutError):  # pragma: no branch
            await execution.get_result(deadline=deadline)


async def test_failed_task_with_ttl_zero(zero_ttl_docket: Docket) -> None:
    """Failed tasks should handle TTL=0 correctly."""

    async def failing_task() -> None:
        raise ValueError("intentional failure")

    zero_ttl_docket.register(failing_task)

    async with Worker(docket=zero_ttl_docket) as worker:
        execution = await zero_ttl_docket.add(failing_task)()
        await worker.run_until_finished()

        # Task should have failed but not crashed
        await asyncio.sleep(0.1)

        # With TTL=0, exception data expires immediately
        deadline = datetime.now(timezone.utc) + timedelta(seconds=0.1)
        with pytest.raises(TimeoutError):  # pragma: no branch
            await execution.get_result(deadline=deadline)


async def test_mixed_ttl_workload(
    redis_url: str, make_docket_name: Callable[[], str]
) -> None:
    """Tasks with different TTL settings should not interfere with each other."""
    async with (  # pragma: no branch
        Docket(
            name=make_docket_name(),
            url=redis_url,
            execution_ttl=timedelta(seconds=60),
        ) as docket_with_ttl,
        Docket(
            name=make_docket_name(),
            url=redis_url,
            execution_ttl=timedelta(0),
        ) as docket_with_zero_ttl,
    ):
        results_with_ttl: list[int] = []
        results_zero_ttl: list[int] = []

        async def task_with_ttl(value: int) -> int:
            results_with_ttl.append(value)
            return value * 2

        async def task_zero_ttl(value: int) -> int:
            results_zero_ttl.append(value)
            return value * 3

        docket_with_ttl.register(task_with_ttl)
        docket_with_zero_ttl.register(task_zero_ttl)

        async with (  # pragma: no branch
            Worker(docket=docket_with_ttl) as worker_with_ttl,
            Worker(docket=docket_with_zero_ttl) as worker_with_zero_ttl,
        ):
            # Schedule tasks on both dockets
            exec_with_ttl = await docket_with_ttl.add(task_with_ttl)(10)
            exec_zero_ttl = await docket_with_zero_ttl.add(task_zero_ttl)(20)

            # Run both workers
            await asyncio.gather(
                worker_with_ttl.run_until_finished(),
                worker_with_zero_ttl.run_until_finished(),
            )

            assert results_with_ttl == [10]
            assert results_zero_ttl == [20]

            # Task with TTL should have retrievable result
            deadline = datetime.now(timezone.utc) + timedelta(seconds=1)
            result = await exec_with_ttl.get_result(deadline=deadline)
            assert result == 20

            # Task with zero TTL should have expired result
            deadline = datetime.now(timezone.utc) + timedelta(seconds=0.1)
            with pytest.raises(TimeoutError):  # pragma: no branch
                await exec_zero_ttl.get_result(deadline=deadline)
