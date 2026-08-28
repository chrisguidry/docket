import asyncio
from typing import Annotated

import pytest

from datetime import datetime, timedelta, timezone

from docket import Docket, Worker
from docket.annotations import Logged
from docket.dependencies import CurrentDocket, CurrentExecution, CurrentWorker, Depends
from docket.execution import (
    Execution,
    ExecutionState,
    TaskFunction,
    compact_signature,
    get_signature,
)


async def no_args() -> None: ...  # pragma: no cover


async def one_arg(a: str) -> None: ...  # pragma: no cover


async def two_args(a: str, b: str) -> None: ...  # pragma: no cover


async def optional_args(a: str, b: str, c: str = "c") -> None: ...  # pragma: no cover


async def logged_args(
    a: Annotated[str, Logged()],
    b: Annotated[str, Logged()] = "foo",
) -> None: ...  # pragma: no cover


async def a_dependency() -> str: ...  # pragma: no cover


async def dependencies(
    a: str,
    b: int = 42,
    c: str = Depends(a_dependency),
    docket: Docket = CurrentDocket(),
    worker: Worker = CurrentWorker(),
) -> None: ...  # pragma: no cover


async def only_dependencies(
    a: str = Depends(a_dependency),
    docket: Docket = CurrentDocket(),
    worker: Worker = CurrentWorker(),
) -> None: ...  # pragma: no cover


@pytest.mark.parametrize(
    "function, expected",
    [
        (no_args, ""),
        (one_arg, "a: str"),
        (two_args, "a: str, b: str"),
        (optional_args, "a: str, b: str, c: str = 'c'"),
        (logged_args, "a: str, b: str = 'foo'"),
        (dependencies, "a: str, b: int = 42, ..."),
        (only_dependencies, "..."),
    ],
)
async def test_compact_signature(
    docket: Docket, worker: Worker, function: TaskFunction, expected: str
):
    assert compact_signature(get_signature(function)) == expected


async def test_execution_function_is_immutable(docket: Docket):
    async def task(x: int) -> int:  # pragma: no cover
        return x * 2

    execution = Execution(
        docket=docket,
        function=task,
        args=(5,),
        kwargs={},
        when=datetime.now(timezone.utc),
        key="test-key",
        attempt=1,
    )

    assert execution.function == task

    with pytest.raises(AttributeError):
        execution.function = no_args  # type: ignore[misc]


async def test_execution_args_is_immutable(docket: Docket):
    async def task(x: int) -> int:  # pragma: no cover
        return x * 2

    execution = Execution(
        docket=docket,
        function=task,
        args=(5,),
        kwargs={},
        when=datetime.now(timezone.utc),
        key="test-key",
        attempt=1,
    )

    assert execution.args == (5,)

    with pytest.raises(AttributeError):
        execution.args = (10,)  # type: ignore[misc]


async def test_execution_kwargs_is_immutable(docket: Docket):
    async def task(x: int, y: int = 2) -> int:  # pragma: no cover
        return x * y

    execution = Execution(
        docket=docket,
        function=task,
        args=(5,),
        kwargs={"y": 3},
        when=datetime.now(timezone.utc),
        key="test-key",
        attempt=1,
    )

    assert execution.kwargs == {"y": 3}

    with pytest.raises(AttributeError):
        execution.kwargs = {"y": 10}  # type: ignore[misc]


async def test_execution_key_is_immutable(docket: Docket):
    async def task(x: int) -> int:  # pragma: no cover
        return x * 2

    execution = Execution(
        docket=docket,
        function=task,
        args=(5,),
        kwargs={},
        when=datetime.now(timezone.utc),
        key="test-key",
        attempt=1,
    )

    assert execution.key == "test-key"

    with pytest.raises(AttributeError):
        execution.key = "new-key"  # type: ignore[misc]


async def test_execution_from_message_without_fallback_raises_for_unknown_task(
    docket: Docket,
):
    """Execution.from_message should raise ValueError when task is unknown and no fallback."""
    import cloudpickle  # type: ignore[import-untyped]

    # Create a message for a task that isn't registered
    message = {
        b"function": b"unknown_task",
        b"args": cloudpickle.dumps(()),  # pyright: ignore[reportUnknownMemberType]
        b"kwargs": cloudpickle.dumps({}),  # pyright: ignore[reportUnknownMemberType]
        b"when": b"2024-01-01T00:00:00+00:00",
        b"key": b"test-key",
        b"attempt": b"1",
    }

    with pytest.raises(ValueError) as exc_info:
        await Execution.from_message(docket, message, redelivered=False)

    assert "unknown_task" in str(exc_info.value)
    assert "not registered" in str(exc_info.value)


async def test_schedule_does_not_wait_on_a_per_key_lock(docket: Docket):
    """schedule() takes no lock on the task key, so an outside holder cannot stall it.

    The `_schedule` script is atomic on its own, and nothing else in docket
    acquires a `{known}:lock` key.
    """
    when = datetime.now(timezone.utc) + timedelta(minutes=1)
    lock_key = f"{docket.known_task_key('unblocked')}:lock"

    async with docket.redis() as redis:
        async with redis.lock(lock_key, timeout=10):
            await asyncio.wait_for(
                docket.add(no_args, when, key="unblocked")(), timeout=2
            )

    snapshot = await docket.snapshot()
    assert [execution.key for execution in snapshot.future] == ["unblocked"]


def lifecycle_attributes(execution: Execution) -> dict[str, object]:
    """The attributes a claim or a sync fills in from Redis."""
    return {
        "state": execution.state,
        "worker": execution.worker,
        "started_at": execution.started_at,
        "completed_at": execution.completed_at,
        "error": execution.error,
        "result_key": execution.result_key,
        "current": execution.progress.current,
        "total": execution.progress.total,
        "message": execution.progress.message,
        "updated_at": execution.progress.updated_at,
    }


async def test_the_claimed_execution_matches_a_synced_one(
    docket: Docket, worker: Worker
):
    """The task body sees exactly what a sync of the same message would report."""
    seen: list[tuple[dict[str, object], dict[str, object]]] = []

    async def report(execution: Execution = CurrentExecution()) -> None:
        async with docket.redis() as redis:
            messages = await redis.xrange(docket.stream_key, count=1)
        _, message = messages[0]
        synced = await Execution.from_message(docket, message)
        seen.append((lifecycle_attributes(execution), lifecycle_attributes(synced)))

    await docket.add(report, key="claimed")()

    await worker.run_until_finished()

    claimed, synced = seen[0]
    assert claimed == synced


async def test_a_refused_claim_reports_what_redis_holds(docket: Docket):
    """A superseded execution shows the state of the key it lost, not stale defaults."""
    await docket.add(no_args, key="refused")()

    async with docket.redis() as redis:
        messages = await redis.xrange(docket.stream_key, count=1)
    _, message = messages[0]
    stale = await Execution.from_message(docket, message)

    await docket.replace(no_args, datetime.now(timezone.utc), "refused")()

    assert not await stale.claim("worker-1")
    assert lifecycle_attributes(stale) == {
        "state": ExecutionState.QUEUED,
        "worker": None,
        "started_at": None,
        "completed_at": None,
        "error": None,
        "result_key": None,
        "current": None,
        "total": 100,
        "message": None,
        "updated_at": None,
    }
