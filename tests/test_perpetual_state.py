"""Tests for perpetual task state behavior with same-key cycles."""

import asyncio
from datetime import timedelta

from docket import Docket, Worker
from docket.dependencies import CurrentExecution, Perpetual
from docket.execution import Execution


async def test_perpetual_task_with_ttl_zero(docket: Docket, worker: Worker) -> None:
    """Perpetual tasks should work correctly with TTL of 0."""
    async with Docket(
        name="test-perpetual-ttl-zero",
        url=docket.url,
        execution_ttl=timedelta(0),
    ) as docket_with_zero_ttl:
        executions: list[str] = []

        async def perpetual_task(
            execution: Execution = CurrentExecution(),
            perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=10)),
        ) -> None:
            executions.append(execution.key)
            if len(executions) >= 3:
                perpetual.cancel()

        docket_with_zero_ttl.register(perpetual_task)

        async with Worker(docket=docket_with_zero_ttl) as worker_with_zero_ttl:
            execution = await docket_with_zero_ttl.add(perpetual_task)()
            await worker_with_zero_ttl.run_at_most({execution.key: 3})

            assert len(executions) == 3
            # All executions should have the SAME key
            assert len(set(executions)) == 1, "Perpetual task should reuse same key"


async def test_perpetual_task_state_isolation(docket: Docket, worker: Worker) -> None:
    """Perpetual tasks with the same key should execute independently."""
    executions: list[str] = []

    async def perpetual_task(
        execution: Execution = CurrentExecution(),
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=10)),
    ) -> None:
        executions.append(execution.key)
        if len(executions) >= 3:
            perpetual.cancel()

    docket.register(perpetual_task)
    execution = await docket.add(perpetual_task)()
    await worker.run_at_most({execution.key: 3})

    assert len(executions) == 3
    # Verify all executions use the same key
    assert len(set(executions)) == 1, "Perpetual executions should share the same key"


async def test_perpetual_task_no_state_accumulation_with_ttl_zero(
    docket: Docket, worker: Worker
) -> None:
    """Perpetual tasks with TTL=0 should not accumulate state records."""
    async with Docket(
        name="test-no-accumulation",
        url=docket.url,
        execution_ttl=timedelta(0),
    ) as docket_with_zero_ttl:
        executions: list[str] = []

        async def perpetual_task(
            execution: Execution = CurrentExecution(),
            perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=10)),
        ) -> None:
            executions.append(execution.key)
            if len(executions) >= 5:
                perpetual.cancel()

        docket_with_zero_ttl.register(perpetual_task)

        async with Worker(docket=docket_with_zero_ttl) as worker_with_zero_ttl:
            execution = await docket_with_zero_ttl.add(perpetual_task)()
            await worker_with_zero_ttl.run_at_most({execution.key: 5})

            assert len(executions) == 5

            # Small delay for Redis to process expirations
            await asyncio.sleep(0.2)

            # Check that we're not accumulating state records
            # With TTL=0, state records should be deleted immediately
            async with docket_with_zero_ttl.redis() as redis:  # pragma: no branch
                keys = await redis.keys(f"{docket_with_zero_ttl.name}:runs:*")  # type: ignore
                assert len(keys) == 0, (
                    f"Should have no state records, found {len(keys)}"
                )


async def test_rapid_perpetual_tasks_no_conflicts(
    docket: Docket, worker: Worker
) -> None:
    """Rapid perpetual tasks should not have state conflicts."""
    executions: list[str] = []

    async def rapid_perpetual(
        execution: Execution = CurrentExecution(),
        perpetual: Perpetual = Perpetual(every=timedelta(0)),
    ) -> None:
        executions.append(execution.key)
        if len(executions) >= 10:
            perpetual.cancel()

    docket.register(rapid_perpetual)
    execution = await docket.add(rapid_perpetual)()
    await worker.run_at_most({execution.key: 10})

    assert len(executions) == 10
    # All executions should have the SAME key (perpetual tasks reuse key)
    assert len(set(executions)) == 1, "Perpetual executions should share same key"


async def test_perpetual_same_key_no_state_accumulation(
    docket: Docket, worker: Worker
) -> None:
    """Multiple cycles of perpetual task with same key should not accumulate state records."""
    executions: list[str] = []

    async def perpetual_task(
        execution: Execution = CurrentExecution(),
        perpetual: Perpetual = Perpetual(every=timedelta(0)),
    ) -> None:
        executions.append(execution.key)
        if len(executions) >= 10:
            perpetual.cancel()

    docket.register(perpetual_task)
    execution = await docket.add(perpetual_task)()
    await worker.run_at_most({execution.key: 10})

    assert len(executions) == 10

    # All should use the same key
    assert len(set(executions)) == 1

    # Small delay for state TTL to take effect
    await asyncio.sleep(0.5)

    # Check state records - with default 15min TTL, the last completed state should exist
    async with docket.redis() as redis:
        # Since all executions share the same key, there should be exactly 1 state record
        keys = await redis.keys(f"{docket.name}:runs:*")  # type: ignore
        assert len(keys) == 1, (
            f"Should have exactly one state record, found {len(keys)}"
        )


async def test_perpetual_task_state_transitions_with_same_key(
    docket: Docket, worker: Worker
) -> None:
    """Each cycle of a perpetual task should use the same key."""
    executions: list[str] = []

    async def perpetual_tracking_keys(
        execution: Execution = CurrentExecution(),
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=20)),
    ) -> None:
        executions.append(execution.key)

        if len(executions) >= 5:
            perpetual.cancel()

    docket.register(perpetual_tracking_keys)
    execution = await docket.add(perpetual_tracking_keys)()
    await worker.run_at_most({execution.key: 5})

    assert len(executions) == 5

    # All should share the same key
    assert len(set(executions)) == 1, "All iterations should share the same key"
