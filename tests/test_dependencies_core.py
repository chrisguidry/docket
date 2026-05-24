"""Tests for core dependency injection and retry strategies."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytest

from docket import CurrentDocket, CurrentWorker, Disposition, Docket, Worker
from docket.dependencies import (
    Depends,
    ExponentialRetry,
    FailureHandler,
    Retry,
    TaskArgument,
    TaskOutcome,
)
from docket.execution import Execution, ExecutionState


async def test_dependencies_may_be_duplicated(docket: Docket, worker: Worker):
    called = False

    async def the_task(
        a: str,
        b: str,
        docketA: Docket = CurrentDocket(),
        docketB: Docket = CurrentDocket(),
        workerA: Worker = CurrentWorker(),
        workerB: Worker = CurrentWorker(),
    ):
        assert a == "a"
        assert b == "b"
        assert docketA is docket
        assert docketB is docket
        assert workerA is worker
        assert workerB is worker

        nonlocal called
        called = True

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert called


async def test_users_can_provide_dependencies_directly(docket: Docket, worker: Worker):
    called = False

    async def the_task(
        a: str,
        b: str,
        retry: Retry = Retry(attempts=3),
    ):
        assert a == "a"
        assert b == "b"
        assert retry.attempts == 42

        nonlocal called
        called = True

    await docket.add(the_task)("a", "b", retry=Retry(attempts=42))

    await worker.run_until_finished()

    assert called


async def test_user_provide_retries_are_used(docket: Docket, worker: Worker):
    calls = 0

    async def the_task(
        a: str,
        b: str,
        retry: Retry = Retry(attempts=42),
    ):
        assert a == "a"
        assert b == "b"
        assert retry.attempts == 2

        nonlocal calls
        calls += 1

        raise Exception("womp womp")

    await docket.add(the_task)("a", "b", retry=Retry(attempts=2))

    await worker.run_until_finished()

    assert calls == 2


@pytest.mark.parametrize("retry_cls", [Retry, ExponentialRetry])
async def test_user_can_request_a_retry_after_a_delay(
    retry_cls: Retry, docket: Docket, worker: Worker
):
    calls = 0
    first_call_time = None
    second_call_time = None

    async def the_task(
        a: str,
        b: str,
        retry: Retry = retry_cls(attempts=2),  # type: ignore[reportCallIssue]
    ):
        assert a == "a"
        assert b == "b"

        nonlocal calls
        calls += 1

        nonlocal first_call_time
        if not first_call_time:
            first_call_time = datetime.now(timezone.utc)
            retry.after(timedelta(seconds=0.5))
        else:
            nonlocal second_call_time
            second_call_time = datetime.now(timezone.utc)

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert calls == 2

    assert isinstance(first_call_time, datetime)
    assert isinstance(second_call_time, datetime)

    delay = second_call_time - first_call_time
    assert delay.total_seconds() > 0 < 1


async def test_retry_in_is_backwards_compatible_alias_for_after(
    docket: Docket, worker: Worker
):
    """retry.in_() still works as an alias for retry.after()"""
    calls = 0

    async def the_task(retry: Retry = Retry(attempts=2)):
        nonlocal calls
        calls += 1
        if calls == 1:
            retry.in_(timedelta(seconds=0.1))

    await docket.add(the_task)()
    await worker.run_until_finished()

    assert calls == 2


@pytest.mark.parametrize("retry_cls", [Retry, ExponentialRetry])
async def test_user_can_request_a_retry_at_a_specific_time(
    retry_cls: Retry, docket: Docket, worker: Worker
):
    calls = 0
    first_call_time = None
    second_call_time = None

    async def the_task(
        a: str,
        b: str,
        retry: Retry = retry_cls(attempts=2),  # type: ignore[reportCallIssue]
    ):
        assert a == "a"
        assert b == "b"

        nonlocal calls
        calls += 1

        nonlocal first_call_time
        if not first_call_time:
            when = datetime.now(timezone.utc) + timedelta(seconds=0.5)
            first_call_time = datetime.now(timezone.utc)
            retry.at(when)
        else:
            nonlocal second_call_time
            second_call_time = datetime.now(timezone.utc)

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert calls == 2

    assert isinstance(first_call_time, datetime)
    assert isinstance(second_call_time, datetime)

    delay = second_call_time - first_call_time
    assert delay.total_seconds() > 0 < 1


async def test_user_can_request_a_retry_at_a_specific_time_in_the_past(
    docket: Docket, worker: Worker
):
    calls = 0
    first_call_time = None
    second_call_time = None

    async def the_task(
        a: str,
        b: str,
        retry: Retry = Retry(attempts=2),
    ):
        assert a == "a"
        assert b == "b"

        nonlocal calls
        calls += 1

        nonlocal first_call_time
        if not first_call_time:
            when = datetime.now(timezone.utc) - timedelta(days=1)
            first_call_time = datetime.now(timezone.utc)
            retry.at(when)
        else:
            nonlocal second_call_time
            second_call_time = datetime.now(timezone.utc)

    await docket.add(the_task)("a", "b")

    await worker.run_until_finished()

    assert calls == 2

    assert isinstance(first_call_time, datetime)
    assert isinstance(second_call_time, datetime)

    delay = second_call_time - first_call_time
    assert delay.total_seconds() > 0 < 1


async def test_concurrent_add_during_retry_wait_is_deduplicated(
    docket: Docket, worker: Worker
):
    """A docket.add() for the same key during a retry's wait window is
    deduplicated: only the original task runs, the intruder is rejected
    with ALREADY_SCHEDULED."""
    attempts: list[str] = []

    async def the_task(
        value: str,
        retry: Retry = Retry(attempts=2, delay=timedelta(milliseconds=500)),
    ):
        attempts.append(value)
        if len(attempts) == 1:
            raise RuntimeError("first attempt fails to trigger retry")

    first = await docket.add(the_task, key="dup-key")("original")
    assert first.disposition is Disposition.SCHEDULED

    # Kick off the worker; it will run the first attempt, fail, and reschedule
    # the retry ~500ms out.
    worker_task = asyncio.create_task(worker.run_until_finished())

    # Poll until the retry is parked (state=SCHEDULED) so we hit the dedup
    # window that exposed the missing 'known' field regression.
    async def await_retry_parked() -> None:
        deadline = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < deadline:
            current = await docket.get_execution("dup-key")
            if current and current.state is ExecutionState.SCHEDULED:
                return
            await asyncio.sleep(0.01)
        raise AssertionError(  # pragma: no cover
            "retry never reached SCHEDULED state"
        )

    await await_retry_parked()

    second = await docket.add(the_task, key="dup-key")("intruder")
    assert second.disposition is Disposition.ALREADY_SCHEDULED

    await worker_task

    assert attempts == ["original", "original"]


async def test_custom_failure_handler_returning_true_still_acks_message(
    docket: Docket, worker: Worker
):
    """A custom FailureHandler that returns True without rescheduling or
    marking a terminal state must still leave the stream message acked.
    The worker's safety net guarantees the message isn't left pending and
    redelivered indefinitely."""

    class JustLog(FailureHandler["JustLog"]):
        async def __aenter__(self) -> "JustLog":
            return self

        async def handle_failure(
            self, execution: Execution, outcome: TaskOutcome
        ) -> bool:
            # Pretend we logged + moved on, doing nothing with the message.
            return True

    async def the_task(value: str, handler: JustLog = JustLog()):
        raise RuntimeError(f"boom: {value}")

    await docket.add(the_task, key="leaky-key")("once")
    await worker.run_until_finished()

    # If the safety net works, the stream is empty -- nothing pending, no
    # message body left behind to redeliver.
    async with docket.redis() as redis:
        assert await redis.xlen(docket.stream_key) == 0
        pending = await redis.xpending(docket.stream_key, docket.worker_group_name)
        assert pending["pending"] == 0


async def test_retry_path_marks_execution_acked(docket: Docket):
    """A reschedule whose ``reschedule_message`` matches the execution's own
    ``message_id`` (the Retry path) must flip ``_acked`` to True locally.

    This is what stops the worker's safety net from firing a second
    ``mark_as_failed`` on a task that ``Retry`` has just re-scheduled --
    the safety net only checks ``execution._acked`` and the ``_schedule``
    Lua's reschedule branch is responsible for the actual XACK+XDEL.

    Pre-fix, ``schedule`` didn't touch ``_acked``, so the safety net
    would observe ``False`` and overwrite the just-scheduled retry with
    a terminal ``failed`` state.  Locking in this assignment guards
    against accidentally dropping the ``if reschedule_message and
    reschedule_message == self.message_id`` branch.
    """

    async def the_task() -> None: ...

    docket.register(the_task)
    await docket.add(the_task, key="retry-acked")()

    async with docket.redis() as redis:
        entries = await redis.xrange(docket.stream_key, count=10)
    msg_id, message = next(
        (mid, msg) for mid, msg in entries if msg[b"key"] == b"retry-acked"
    )
    execution = await Execution.from_message(docket, message, message_id=msg_id)
    assert execution._acked is False  # pyright: ignore[reportPrivateUsage]

    # Drive the same call shape Retry.handle_failure uses: replace=True,
    # reschedule_message=execution.message_id.
    await execution.schedule(replace=True, reschedule_message=execution.message_id)

    assert execution._acked is True, (  # pyright: ignore[reportPrivateUsage]
        "schedule() with reschedule_message == self.message_id must set "
        "_acked=True so the worker safety net does not double-fire "
        "mark_as_failed on a successfully-rescheduled retry"
    )


async def test_safety_net_publishes_failed_state_event_with_no_error(
    docket: Docket, worker: Worker
):
    """Lock in the observable shape of the safety-net's published event.

    When a custom ``FailureHandler`` returns True without taking action,
    the worker's safety net fires ``mark_as_failed(error=None)``.  The
    resulting state-channel event reports ``state=failed`` and carries
    NO ``error`` field (since none was supplied).  Anyone alerting on
    ``state=failed && !error`` needs to know this is the safety-net
    signature -- not a regression where error reporting got lost.
    """
    import contextlib
    import json

    class JustLog(FailureHandler["JustLog"]):
        async def __aenter__(self) -> "JustLog":
            return self

        async def handle_failure(
            self, execution: Execution, outcome: TaskOutcome
        ) -> bool:
            return True

    async def the_task(value: str, handler: JustLog = JustLog()):
        raise RuntimeError(f"boom: {value}")

    task_key = "safety-net-event"
    state_events: list[dict[str, object]] = []
    ready = asyncio.Event()

    async def collector() -> None:
        async with docket._pubsub() as pubsub:  # pyright: ignore[reportPrivateUsage]
            await pubsub.subscribe(docket.key(f"state:{task_key}"))
            ready.set()
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue  # pragma: no cover
                data = message["data"]
                payload = data.decode() if isinstance(data, bytes) else data
                state_events.append(json.loads(payload))

    collector_task = asyncio.create_task(collector())
    await ready.wait()

    await docket.add(the_task, key=task_key)("once")
    await worker.run_until_finished()
    await asyncio.sleep(0.05)

    collector_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await collector_task

    failed_events = [e for e in state_events if e.get("state") == "failed"]
    assert len(failed_events) == 1, (
        f"safety net should have published exactly one state=failed event; "
        f"got: {state_events!r}"
    )
    failed = failed_events[0]
    assert "error" not in failed, (
        f"safety-net mark_as_failed(error=None) must NOT include an "
        f"'error' field in the state event payload; got: {failed!r}"
    )
    assert "completed_at" in failed, (
        f"safety-net failed event must include 'completed_at'; got: {failed!r}"
    )
    assert failed.get("key") == task_key
    assert failed.get("type") == "state"


async def test_dependencies_error_for_missing_task_argument(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A task will fail when asking for a missing task argument"""

    async def dependency_one(nope: list[str] = TaskArgument()) -> list[str]:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    async def dependent_task(
        a: list[str],
        b: list[str] = TaskArgument("a"),
        c: list[str] = Depends(dependency_one),
    ) -> None:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    await docket.add(dependent_task)(a=["hello", "world"])

    await worker.run_until_finished()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert "Failed to resolve dependencies for parameter(s): c" in caplog.text
    assert "ExceptionGroup" in caplog.text
    assert "KeyError: 'nope'" in caplog.text


async def test_a_task_argument_cannot_ask_for_itself(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """A task argument cannot ask for itself"""

    # This task would be nonsense, because it's asking for itself.
    async def dependent_task(a: list[str] = TaskArgument()) -> None:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    await docket.add(dependent_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert "Failed to resolve dependencies for parameter(s): a" in caplog.text
    assert "ValueError: No parameter name specified" in caplog.text


def test_dependency_class_has_backwards_compatible_context_vars():
    """Dependency.execution/docket/worker are available for downstream consumers.

    Prior to 0.18, docket's Dependency class had class-level ContextVars.  Now
    that Dependency comes from uncalled-for, those ContextVars are module-level
    in docket.dependencies._base.  We monkeypatch them back onto the class so
    existing code (e.g. FastMCP's `Dependency.execution.get()`) keeps working.
    """
    from docket.dependencies import (
        Dependency,
        current_docket,
        current_execution,
        current_worker,
    )

    assert Dependency.execution is current_execution  # type: ignore[attr-defined]
    assert Dependency.docket is current_docket  # type: ignore[attr-defined]
    assert Dependency.worker is current_worker  # type: ignore[attr-defined]
