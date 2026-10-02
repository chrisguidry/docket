"""Tests for perpetual task state behavior with same-key cycles."""

import asyncio
import contextlib
from datetime import timedelta
from typing import AsyncGenerator, Callable

import pytest
from docket import Docket, ExecutionState, Worker
from docket._execution_progress import StateEvent
from docket.dependencies import CurrentExecution, Perpetual
from docket.execution import Execution
from tests.conftest import wait_until


async def test_perpetual_task_with_ttl_zero(zero_ttl_docket: Docket) -> None:
    """Perpetual tasks should work correctly with TTL of 0."""
    executions: list[str] = []

    async def perpetual_task(
        execution: Execution = CurrentExecution(),
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=10)),
    ) -> None:
        executions.append(execution.key)
        if len(executions) >= 3:
            perpetual.cancel()

    zero_ttl_docket.register(perpetual_task)

    async with Worker(docket=zero_ttl_docket) as worker:
        execution = await zero_ttl_docket.add(perpetual_task)()
        await worker.run_at_most({execution.key: 3})

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
    zero_ttl_docket: Docket,
) -> None:
    """Perpetual tasks with TTL=0 should not accumulate state records."""
    executions: list[str] = []

    async def perpetual_task(
        execution: Execution = CurrentExecution(),
        perpetual: Perpetual = Perpetual(every=timedelta(milliseconds=10)),
    ) -> None:
        executions.append(execution.key)
        if len(executions) >= 5:
            perpetual.cancel()

    zero_ttl_docket.register(perpetual_task)

    async with Worker(docket=zero_ttl_docket) as worker:
        execution = await zero_ttl_docket.add(perpetual_task)()
        await worker.run_at_most({execution.key: 5})

        assert len(executions) == 5

        # With execution_ttl=0, state records should be DELed by the
        # final _terminal Lua.  Poll for the keys to clear instead of
        # racing a fixed 200 ms sleep -- the deletion is in-band with
        # the call we just awaited, so this almost always returns on
        # the first poll.
        async def no_state_records() -> bool:
            async with zero_ttl_docket.redis() as redis:
                keys = await redis.keys(f"{zero_ttl_docket.name}:runs:*")
                return len(keys) == 0

        await wait_until(no_state_records, description="all state records DELed")


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

    # Check state records - with default 15min TTL, the last completed state
    # should exist.  The HSET in _terminal is in-band with run_at_most's
    # last cycle, so this should be immediate -- poll instead of sleeping.
    async def exactly_one_state_record() -> bool:
        async with docket.redis() as redis:
            # SCAN, not KEYS, since hash-tagged patterns break in cluster mode.
            pattern = f"{docket.prefix}:runs:*"
            count = 0
            async for _ in redis.scan_iter(match=pattern):
                count += 1
            return count == 1

    await wait_until(
        exactly_one_state_record,
        description="exactly one runs:* state record",
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


@pytest.fixture(
    params=[
        pytest.param(timedelta(0), id="ttl_zero"),
        pytest.param(timedelta(seconds=60), id="default_ttl"),
    ],
)
async def pubsub_docket(
    request: pytest.FixtureRequest,
    redis_url: str,
    make_docket_name: Callable[[], str],
) -> AsyncGenerator[Docket, None]:
    async with Docket(
        name=make_docket_name(),
        url=redis_url,
        execution_ttl=request.param,
    ) as docket:
        yield docket


async def test_perpetual_publishes_completed_event(pubsub_docket: Docket):
    """Perpetual tasks must still publish a completed state event on pub/sub.

    _mark_as_terminal skips the runs hash write when the successor has already
    been scheduled, but the pub/sub notification must still fire so that
    callers waiting via execution.subscribe() or get_result() see completion.
    """

    async def simple_perpetual(perpetual: Perpetual = Perpetual()):
        perpetual.after(timedelta(hours=1))

    execution = await pubsub_docket.add(simple_perpetual, key="pubsub-completion")()

    state_events: list[StateEvent] = []

    async def collect_events():
        async for event in execution.subscribe():  # pragma: no cover
            if event["type"] == "state":
                state_events.append(event)
                if event["state"] == ExecutionState.COMPLETED:
                    return

    async with Worker(
        pubsub_docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        collector = asyncio.create_task(collect_events())
        worker_task = asyncio.create_task(worker.run_until_finished())

        await asyncio.wait_for(collector, timeout=10)

        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task

    states = [e["state"] for e in state_events]
    assert ExecutionState.COMPLETED in states, (
        f"Expected COMPLETED in pub/sub state events, got {states}"
    )
