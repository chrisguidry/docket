"""Regression tests for ``Execution.claim`` SUPERSEDED-path behavior.

When ``_claim`` detects a stale generation it returns ``SUPERSEDED``
and (a) must clean up the stale stream message and (b) must always
fire for any source of supersession -- ``replace()``, Perpetual
successors, retries that crossed a successor's claim, etc.

This is not strictly a concurrency-limit concern, so it lives at the
top level rather than under ``tests/concurrency_limits/``.
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timedelta, timezone

from docket import Docket
from docket.execution import Execution


async def test_claim_superseded_acks_stale_stream_message(docket: Docket) -> None:
    """``_claim`` SUPERSEDED must XACK+XDEL the stale stream entry.

    Build a real Execution at generation=1, bump the runs hash directly to
    generation=2 (simulating a successor already in flight), then call
    ``claim`` on the stale execution.  The Lua's supersession check fires,
    and the new behaviour requires the stream message and the consumer
    group's PEL to both be empty afterward.
    """

    async def noop() -> None:
        pass  # pragma: no cover

    docket.register(noop)
    await docket.add(noop, key="claim-supersedes")()

    # Pull the stream message so we have a real message_id to claim against.
    async with docket.redis() as redis:
        entries = await redis.xrange(docket.stream_key, count=10)
    msg_id, message = next(
        (mid, msg) for mid, msg in entries if msg[b"key"] == b"claim-supersedes"
    )

    # Read the message into a worker-style consumer group so it lands in
    # PEL, mirroring what XREADGROUP would do during normal consumption.
    async with docket.redis() as redis:
        # On the memory backend the group is created on demand; on real
        # Redis the worker would have created it already, but here we
        # drive it directly and tolerate "already exists".
        with contextlib.suppress(Exception):
            await redis.xgroup_create(
                docket.stream_key,
                docket.worker_group_name,
                id="0",
                mkstream=True,
            )
        await redis.xreadgroup(
            docket.worker_group_name,
            "test-consumer",
            {docket.stream_key: ">"},
            count=10,
            block=10,
        )
        pending_before = await redis.xpending(
            docket.stream_key, docket.worker_group_name
        )
    assert pending_before["pending"] >= 1, (
        f"setup: expected the message to land in PEL; got {pending_before!r}"
    )

    # Build a stale Execution at generation=1 and bump the runs hash to
    # generation=2 so the claim must take the SUPERSEDED path.
    stale = await Execution.from_message(docket, message, message_id=msg_id)
    assert stale.generation == 1
    async with docket.redis() as redis:
        await redis.hincrby(stale._redis_key, "generation", 1)  # pyright: ignore[reportPrivateUsage]
    stale_with_gen1 = Execution(
        docket=docket,
        function=noop,
        args=(),
        kwargs={},
        key=stale.key,
        when=datetime.now(timezone.utc),
        attempt=1,
        generation=1,
        message_id=msg_id,
    )

    assert (await stale_with_gen1.claim("stale-worker")) is False

    # After SUPERSEDED, the stale message should be gone from both the
    # stream and the consumer group's PEL.
    async with docket.redis() as redis:
        stream_after = await redis.xrange(docket.stream_key, count=10)
        pending_after = await redis.xpending(
            docket.stream_key, docket.worker_group_name
        )
    assert not any(mid == msg_id for mid, _ in stream_after), (
        f"_claim SUPERSEDED must XDEL the stale stream message; "
        f"still present in stream: {stream_after!r}"
    )
    assert pending_after["pending"] == 0, (
        f"_claim SUPERSEDED must XACK the stale stream message; "
        f"PEL still has entries: {pending_after!r}"
    )


async def test_replace_makes_prior_generations_stale_message_unclaimable(
    docket: Docket,
) -> None:
    """``docket.replace()`` must make any prior-generation message
    unclaimable: a worker that holds the old ``message_id`` and tries
    to ``claim`` it sees SUPERSEDED, not a successful claim.

    Pre-fix would let a slow worker that read the old stream entry
    before the replace landed go on to run the replaced task body --
    duplicating execution under the same key.  This locks in the
    contract from the ``replace()`` direction (the supersession check
    in ``_claim`` is the actual enforcer; ``replace`` is responsible
    for bumping the generation that triggers it).
    """

    async def the_task(payload: str) -> None: ...

    docket.register(the_task)

    # Add the original task far enough in the future that it parks in
    # the queue (so the replace path goes through the "scheduled"
    # branch, not the stream branch).
    when = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add(the_task, when=when, key="replace-supersedes")("original-arg")

    # Read the runs hash directly to capture the generation that
    # ``_schedule`` set so we know what counts as "stale" below.
    async with docket.redis() as redis:
        gen_str = await redis.hget(docket.runs_key("replace-supersedes"), "generation")
    assert gen_str is not None
    original_gen = int(gen_str)
    assert original_gen >= 1

    # Now replace the task.  ``_schedule``'s replace branch bumps the
    # generation counter.
    await docket.replace(the_task, when=when, key="replace-supersedes")(
        "replacement-arg"
    )

    async with docket.redis() as redis:
        new_gen_str = await redis.hget(
            docket.runs_key("replace-supersedes"), "generation"
        )
    assert new_gen_str is not None
    assert int(new_gen_str) > original_gen, (
        "replace() must bump the runs hash generation counter"
    )

    # Construct a stale Execution against the same key with the
    # pre-replace generation, then call claim.
    stale = Execution(
        docket=docket,
        function=the_task,
        args=("original-arg",),
        kwargs={},
        key="replace-supersedes",
        when=when,
        attempt=1,
        generation=original_gen,
    )

    assert (await stale.claim("late-worker")) is False, (
        "claim() on a stale-generation Execution must return False "
        "(SUPERSEDED) after the same key has been replace()d -- a "
        "regression here would silently let a stale worker double-run "
        "the replaced task body"
    )

    # And the runs hash still reflects the replacement, not the stale claim.
    after = await docket.get_execution("replace-supersedes")
    assert after is not None
    assert after.args == ("replacement-arg",), (
        f"stale SUPERSEDED claim must not have overwritten the runs hash; "
        f"got: {after.args!r}"
    )
