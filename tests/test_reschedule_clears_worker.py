"""Regression test: ``_schedule`` reschedule branch must clear ``worker``
and ``started_at`` left behind by a previous ``_claim``.

When a Retry-driven reschedule fires, the runs hash carries over the
previous attempt's ``worker`` and ``started_at`` fields from ``_claim``.
``sync()`` then reports a queued/scheduled task whose ``.worker`` and
``.started_at`` describe the last attempt, which is misleading.

The fix is to HDEL both fields inside the reschedule branch of
``_schedule``.
"""

from __future__ import annotations

import contextlib
from typing import AsyncGenerator

import pytest

from docket import Docket
from docket.execution import Execution


@pytest.fixture
async def cleanup_executions() -> AsyncGenerator[list[Execution], None]:
    """Executions whose runs+progress hashes should be closed out at
    teardown.  Ensures the key-leak checker sees TTLs on every key the
    test touched even when an assertion mid-test raises.
    """
    pending: list[Execution] = []
    yield pending
    for execution in pending:
        with contextlib.suppress(Exception):
            await execution.mark_as_completed()


async def test_reschedule_clears_worker_and_started_at(
    docket: Docket,
    cleanup_executions: list[Execution],
) -> None:
    async def the_task() -> None: ...

    docket.register(the_task)

    # 1. Add the task and read it back from the stream so we have an
    #    Execution whose ``message_id`` matches what's in the stream.
    await docket.add(the_task, key="reschedule-test")()
    async with docket.redis() as redis:
        stream_entries = await redis.xrange(docket.stream_key, count=10)
    message_id, message = next(
        (mid, msg) for mid, msg in stream_entries if msg[b"key"] == b"reschedule-test"
    )
    execution = await Execution.from_message(docket, message, message_id=message_id)

    # 2. Claim the task as a worker would.
    assert await execution.claim("worker-A")
    assert execution.worker == "worker-A"
    assert execution.started_at is not None

    # 3. Reschedule via the Retry-driven path: replace=True and the original
    #    message_id flow.
    await execution.schedule(replace=True, reschedule_message=message_id)

    # 4. A fresh Execution syncing from Redis should see no leftover worker
    #    or started_at fields.
    rehydrated = Execution(
        docket=docket,
        function=the_task,
        args=(),
        kwargs={},
        key="reschedule-test",
        when=execution.when,
        attempt=1,
    )
    cleanup_executions.append(rehydrated)
    await rehydrated.sync()

    assert rehydrated.worker is None, (
        f"after a reschedule, the runs hash should not carry over the "
        f"previous attempt's worker; got {rehydrated.worker!r}"
    )
    assert rehydrated.started_at is None, (
        f"after a reschedule, the runs hash should not carry over the "
        f"previous attempt's started_at; got {rehydrated.started_at!r}"
    )
