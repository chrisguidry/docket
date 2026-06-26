"""Regression test: SUPERSEDED ``_terminal`` must not clobber a successor's
in-flight progress.

When a stale generation finishes after the successor has been claimed,
they share the same progress hash (keyed by task key, not by generation).
The pre-fix ``_terminal`` SUPERSEDED branch unconditionally ``DEL``ed the
progress hash, wiping the successor's ``current``/``total``/``message``
even though the successor was still actively reporting against it.

The fix tags the progress hash with ``generation`` at ``_claim`` time,
and the SUPERSEDED branch only DELs when its own generation owns the
tag.  Missing tag (pre-fix data or pre-tracking) defaults to the prior
behavior: always DEL.
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from typing import AsyncGenerator

import pytest

from docket import Docket
from docket.execution import Execution


@pytest.fixture
async def cleanup_executions() -> AsyncGenerator[list[Execution], None]:
    """Executions whose runs+progress hashes should be closed out at
    teardown.  The tests below race generations directly through Redis
    without going through the worker, so nothing else completes these
    executions for us; running the cleanup as a fixture ensures it fires
    even if an assertion mid-test raises.
    """
    pending: list[Execution] = []
    yield pending
    for execution in pending:
        with contextlib.suppress(Exception):
            await execution.mark_as_completed()


async def test_superseded_terminal_preserves_successor_progress(
    docket: Docket,
    cleanup_executions: list[Execution],
) -> None:
    """A stale gen=1's ``_terminal`` must not clobber gen=2's progress hash."""

    async def noop() -> None: ...

    docket.register(noop)

    key = "clobber-test"
    # Schedule once and grab the stream message so we have a gen=1 Execution
    # with a real message_id.
    await docket.add(noop, key=key)()
    async with docket.redis() as redis:
        messages = await redis.xrange(docket.stream_key, count=10)
    msg_id_gen1, message_gen1 = next(
        (mid, msg) for mid, msg in messages if msg[b"key"] == key.encode()
    )
    stale = await Execution.from_message(docket, message_gen1, message_id=msg_id_gen1)
    assert stale.generation == 1

    # gen=1 worker claims the task and initializes the progress hash.
    assert await stale.claim("worker-A")

    # Now create gen=2 in place: bump generation in the runs hash and
    # re-claim from a fresh Execution.  This models the race where the
    # successor was claimed (by another worker, after a replace()) before
    # the stale gen=1 finishes.
    async with docket.redis() as redis:
        await redis.hincrby(stale._redis_key, "generation", 1)  # pyright: ignore[reportPrivateUsage]
    successor = Execution(
        docket=docket,
        function=noop,
        args=(),
        kwargs={},
        key=key,
        when=datetime.now(timezone.utc),
        attempt=1,
        generation=2,
    )
    assert await successor.claim("worker-B")
    cleanup_executions.append(successor)
    # Successor reports some progress.
    await successor.progress.set_total(7)
    await successor.progress.increment(3)
    await successor.progress.set_message("halfway-ish")

    # Snapshot the successor's progress hash before stale's termination.
    async with docket.redis() as redis:
        before = await redis.hgetall(successor.progress._redis_key)  # pyright: ignore[reportPrivateUsage]
    assert before.get(b"current") == b"3"
    assert before.get(b"total") == b"7"
    assert before.get(b"message") == b"halfway-ish"

    # Now stale gen=1 finishes.  Its `_terminal` should hit SUPERSEDED.
    # Pre-fix, this DELed the progress hash and the successor's tracking
    # vanished.
    await stale.mark_as_completed()

    async with docket.redis() as redis:
        after = await redis.hgetall(successor.progress._redis_key)  # pyright: ignore[reportPrivateUsage]

    assert after, (
        "SUPERSEDED termination should not have DELed the progress hash "
        "owned by a newer generation"
    )
    assert after.get(b"current") == b"3", (
        f"successor's `current` should still be 3, got: {after!r}"
    )
    assert after.get(b"total") == b"7", (
        f"successor's `total` should still be 7, got: {after!r}"
    )
    assert after.get(b"message") == b"halfway-ish", (
        f"successor's `message` should still be 'halfway-ish', got: {after!r}"
    )


async def test_successor_claim_clears_stale_message_and_updated_at(
    docket: Docket,
    cleanup_executions: list[Execution],
) -> None:
    """A successor's ``_claim`` must wipe ``message``/``updated_at`` left
    behind by the previous generation's progress reports.

    ``_claim`` only ``HSET``s ``current``/``total``/``generation`` on the
    shared progress hash.  Without an explicit ``HDEL`` of the optional
    fields, a stale ``message`` and ``updated_at`` from the predecessor's
    last ``set_message`` call survive into the successor's view, and
    ``sync()``/state subscribers report metadata that doesn't belong to
    the new attempt.
    """

    async def noop() -> None: ...

    docket.register(noop)

    key = "claim-clears-stale"
    await docket.add(noop, key=key)()
    async with docket.redis() as redis:
        messages = await redis.xrange(docket.stream_key, count=10)
    msg_id, message = next(
        (mid, msg) for mid, msg in messages if msg[b"key"] == key.encode()
    )
    stale = await Execution.from_message(docket, message, message_id=msg_id)
    assert await stale.claim("worker-A")

    # Predecessor reports a message and bumps progress.
    await stale.progress.set_total(10)
    await stale.progress.increment(4)
    await stale.progress.set_message("from the old attempt")

    # Successor takes over.  Bump generation directly so we can re-claim
    # against the same runs hash without going through the worker.
    async with docket.redis() as redis:
        await redis.hincrby(stale._redis_key, "generation", 1)  # pyright: ignore[reportPrivateUsage]
    successor = Execution(
        docket=docket,
        function=noop,
        args=(),
        kwargs={},
        key=key,
        when=datetime.now(timezone.utc),
        attempt=1,
        generation=2,
    )
    assert await successor.claim("worker-B")
    cleanup_executions.append(successor)

    # The successor's view should report a clean slate -- no leftover
    # ``message`` or ``updated_at`` from the predecessor.
    await successor.progress.sync()
    assert successor.progress.current == 0
    assert successor.progress.total == 100
    assert successor.progress.message is None, (
        f"successor saw stale message from predecessor: {successor.progress.message!r}"
    )
    assert successor.progress.updated_at is None, (
        f"successor saw stale updated_at from predecessor: "
        f"{successor.progress.updated_at!r}"
    )


async def test_superseded_terminal_dels_progress_when_no_generation_tag(
    docket: Docket,
    cleanup_executions: list[Execution],
) -> None:
    """Pre-fix progress hashes (no generation tag) keep being DELed on
    SUPERSEDED, preserving backwards-compat during a mixed-version upgrade.
    """

    async def noop() -> None: ...

    docket.register(noop)

    key = "clobber-legacy"
    await docket.add(noop, key=key)()
    async with docket.redis() as redis:
        messages = await redis.xrange(docket.stream_key, count=10)
    msg_id, message = next(
        (mid, msg) for mid, msg in messages if msg[b"key"] == key.encode()
    )
    stale = await Execution.from_message(docket, message, message_id=msg_id)
    assert await stale.claim("worker-A")

    # Simulate "successor was claimed by old code that didn't tag the
    # progress hash with generation": bump generation in the runs hash,
    # rewrite the progress hash *without* a generation field.
    async with docket.redis() as redis:
        await redis.hincrby(stale._redis_key, "generation", 1)  # pyright: ignore[reportPrivateUsage]
        await redis.delete(stale.progress._redis_key)  # pyright: ignore[reportPrivateUsage]
        await redis.hset(
            stale.progress._redis_key,  # pyright: ignore[reportPrivateUsage]
            mapping={"current": "1", "total": "5"},
        )

    await stale.mark_as_completed()

    async with docket.redis() as redis:
        after = await redis.hgetall(stale.progress._redis_key)  # pyright: ignore[reportPrivateUsage]
    assert after == {}, (
        f"untagged progress hash should be DELed on SUPERSEDED for "
        f"backwards-compat, got: {after!r}"
    )

    # Register a synthetic gen=2 successor for cleanup so the key-leak
    # checker doesn't flag the runs hash this test left behind.
    cleanup_executions.append(
        Execution(
            docket=docket,
            function=noop,
            args=(),
            kwargs={},
            key=key,
            when=datetime.now(timezone.utc),
            attempt=1,
            generation=2,
        )
    )
