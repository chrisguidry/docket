# pyright: reportPrivateUsage=false

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import ConnectionError, ResponseError

from docket import Docket
from docket.queue import Queue, QueueSubscription


async def test_queue_round_trip_is_fifo_and_idempotent(docket: Docket) -> None:
    queue = docket.queue("jobs")

    assert await queue.put("alpha", b"one", key="one")
    assert not await queue.put("alpha", b"duplicate", key="one")
    assert await queue.put("alpha", b"two", key="two")

    async with queue.subscribe(
        ["alpha"], visibility_timeout=timedelta(seconds=1)
    ) as subscription:
        first = await subscription.receive(timeout=1)
        assert (first.key, first.data, first.topic) == ("one", b"one", "alpha")
        await first.acknowledge()

        second = await subscription.receive(timeout=1)
        assert (second.key, second.data) == ("two", b"two")
        await second.acknowledge()

    assert await queue.put("alpha", b"one-again", key="one")


async def test_receive_without_timeout(docket: Docket) -> None:
    queue = docket.queue("jobs")
    assert await queue.put("alpha", b"one")

    async with queue.subscribe(
        ["alpha"], visibility_timeout=timedelta(seconds=1)
    ) as subscription:
        message = await subscription.receive()
        await message.acknowledge()


async def test_acknowledgement_tombstone_deduplicates_repairs(
    docket: Docket,
) -> None:
    queue = docket.queue(
        "jobs",
        acknowledgement_ttl=timedelta(milliseconds=50),
    )
    assert await queue.put("alpha", b"one", key="one")

    async with queue.subscribe(
        ["alpha"], visibility_timeout=timedelta(seconds=1)
    ) as subscription:
        message = await subscription.receive(timeout=1)
        await message.acknowledge()
        await message.acknowledge()

    assert not await queue.put("alpha", b"too-soon", key="one")
    await asyncio.sleep(0.06)
    assert await queue.put("alpha", b"after-expiry", key="one")


async def test_bounded_put_waits_for_acknowledgement(docket: Docket) -> None:
    queue = docket.queue("jobs")
    assert await queue.put("alpha", b"one", key="one", max_size=1)
    blocked = asyncio.create_task(queue.put("alpha", b"two", key="two", max_size=1))
    await asyncio.sleep(0.02)
    assert not blocked.done()

    async with queue.subscribe(
        ["alpha"], visibility_timeout=timedelta(seconds=1)
    ) as subscription:
        message = await subscription.receive(timeout=1)
        await message.acknowledge()
        assert await asyncio.wait_for(blocked, timeout=1)
        message = await subscription.receive(timeout=1)
        await message.acknowledge()


async def test_release_moves_message_to_priority_topic(docket: Docket) -> None:
    queue = docket.queue("jobs")
    assert await queue.put("scheduled", b"one", key="one")

    async with queue.subscribe(
        {"retry": 0, "scheduled": 1},
        visibility_timeout=timedelta(seconds=1),
    ) as subscription:
        message = await subscription.receive(timeout=1)
        await message.release("retry", max_size=1)

        redelivered = await subscription.receive(timeout=1)
        assert redelivered.key == "one"
        assert redelivered.topic == "retry"
        await redelivered.acknowledge()


async def test_bounded_release_waits_for_destination_capacity(
    docket: Docket,
) -> None:
    queue = docket.queue("jobs")
    assert await queue.put("scheduled", b"one", key="one")
    assert await queue.put("retry", b"blocker", key="blocker")

    async with queue.subscribe(
        ["scheduled"], visibility_timeout=timedelta(seconds=1)
    ) as scheduled:
        message = await scheduled.receive(timeout=1)
        release = asyncio.create_task(message.release("retry", max_size=1))
        await asyncio.sleep(0.02)
        assert not release.done()

        async with queue.subscribe(
            ["retry"], visibility_timeout=timedelta(seconds=1)
        ) as retry:
            blocker = await retry.receive(timeout=1)
            await blocker.acknowledge()
            await asyncio.wait_for(release, timeout=1)

            redelivered = await retry.receive(timeout=1)
            assert redelivered.key == "one"
            await redelivered.acknowledge()


async def test_bounded_release_to_same_topic_does_not_deadlock(
    docket: Docket,
) -> None:
    queue = docket.queue("jobs")
    assert await queue.put("retry", b"one", max_size=1)
    async with queue.subscribe(
        ["retry"], visibility_timeout=timedelta(seconds=1)
    ) as subscription:
        message = await subscription.receive(timeout=1)
        await asyncio.wait_for(message.release("retry", max_size=1), timeout=1)
        redelivered = await subscription.receive(timeout=1)
        await redelivered.acknowledge()


async def test_unacknowledged_message_is_reclaimed_after_visibility_timeout(
    docket: Docket,
) -> None:
    queue = docket.queue("jobs")
    assert await queue.put("alpha", b"one", key="one")

    async with queue.subscribe(
        ["alpha"], visibility_timeout=timedelta(milliseconds=50)
    ) as first:
        claimed = await first.receive(timeout=1)
        assert claimed.key == "one"

    await asyncio.sleep(0.06)
    async with queue.subscribe(
        ["alpha"], visibility_timeout=timedelta(milliseconds=50)
    ) as second:
        reclaimed = await second.receive(timeout=1)
        assert reclaimed.key == "one"
        await reclaimed.acknowledge()


async def test_visibility_is_renewed_while_subscription_is_active(
    docket: Docket,
) -> None:
    queue = docket.queue("jobs")
    assert await queue.put("alpha", b"one", key="one")

    async with queue.subscribe(
        ["alpha"], visibility_timeout=timedelta(milliseconds=50)
    ) as first:
        claimed = await first.receive(timeout=1)
        async with queue.subscribe(
            ["alpha"], visibility_timeout=timedelta(milliseconds=50)
        ) as second:
            await asyncio.sleep(0.12)
            with pytest.raises(asyncio.TimeoutError):
                await second.receive(timeout=0.05)
        await claimed.acknowledge()


async def test_topics_route_messages_to_matching_subscriptions(
    docket: Docket,
) -> None:
    queue = docket.queue("jobs")
    assert await queue.put("alpha", b"one", key="one")

    async with (
        queue.subscribe(["beta"], visibility_timeout=timedelta(seconds=1)) as wrong,
        queue.subscribe(["alpha"], visibility_timeout=timedelta(seconds=1)) as right,
    ):
        message = await right.receive(timeout=1)
        with pytest.raises(asyncio.TimeoutError):
            await wrong.receive(timeout=0.05)
        await message.acknowledge()


async def test_queue_validates_configuration(docket: Docket) -> None:
    queue = docket.queue("jobs")

    with pytest.raises(ValueError, match="max_size must be non-negative"):
        await queue.put("alpha", b"one", max_size=-1)
    with pytest.raises(ValueError, match="at least one topic"):
        queue.subscribe([], visibility_timeout=timedelta(seconds=1))
    with pytest.raises(ValueError, match="visibility_timeout must be positive"):
        queue.subscribe(["alpha"], visibility_timeout=timedelta(0))

    invalid_tombstone = Queue(
        docket,
        "invalid",
        acknowledgement_ttl=timedelta(seconds=-1),
    )
    with pytest.raises(ValueError, match="acknowledgement_ttl"):
        await invalid_tombstone.put("alpha", b"one")


async def test_message_validates_release_and_settlement(docket: Docket) -> None:
    queue = docket.queue("jobs")
    assert await queue.put("alpha", b"one")
    async with queue.subscribe(
        ["alpha"], visibility_timeout=timedelta(seconds=1)
    ) as subscription:
        message = await subscription.receive(timeout=1)
        with pytest.raises(ValueError, match="max_size must be non-negative"):
            await message.release("alpha", max_size=-1)
        await message.acknowledge()
        await message.release("alpha")


async def test_corrupt_message_is_rejected(docket: Docket) -> None:
    subscription = docket.queue("jobs").subscribe(
        ["alpha"], visibility_timeout=timedelta(seconds=1)
    )
    with pytest.raises(ValueError, match="is missing"):
        subscription._message("alpha", b"1-0", {b"key": b"one"})


async def test_consumer_retries_connection_errors(
    docket: Docket, caplog: pytest.LogCaptureFixture
) -> None:
    subscription = docket.queue("jobs").subscribe(
        ["alpha"], visibility_timeout=timedelta(seconds=1)
    )
    subscription._claim = AsyncMock(
        side_effect=[ConnectionError("offline"), asyncio.CancelledError()]
    )
    with (
        caplog.at_level(logging.WARNING),
        patch("docket.queue.asyncio.sleep", new=AsyncMock()),
        pytest.raises(asyncio.CancelledError),
    ):
        await subscription._consume("alpha", 0)
    assert "lost its Redis connection" in caplog.text


@asynccontextmanager
async def _redis_connection(redis: AsyncMock) -> AsyncGenerator[AsyncMock]:
    yield redis


async def test_claim_recovers_a_missing_group(docket: Docket) -> None:
    subscription = docket.queue("jobs").subscribe(
        ["alpha"], visibility_timeout=timedelta(seconds=1)
    )
    stream_key = subscription.queue._stream_key("alpha")
    subscription._initialized_streams.add(stream_key)
    subscription._next_recovery["alpha"] = float("inf")
    redis = AsyncMock()
    redis.xreadgroup.side_effect = [
        ResponseError("NOGROUP no such key"),
        [],
        [(stream_key, [(b"1-0", {b"key": b"one", b"data": b"payload"})])],
    ]

    with (
        patch.object(docket, "redis", side_effect=lambda: _redis_connection(redis)),
        patch(
            "docket.queue.ensure_consumer_group", new=AsyncMock()
        ) as ensure_group,
    ):
        message = await subscription._claim("alpha")

    assert message.key == "one"
    ensure_group.assert_awaited_once()


async def test_claim_propagates_other_redis_errors(docket: Docket) -> None:
    subscription = docket.queue("jobs").subscribe(
        ["alpha"], visibility_timeout=timedelta(seconds=1)
    )
    stream_key = subscription.queue._stream_key("alpha")
    subscription._initialized_streams.add(stream_key)
    subscription._next_recovery["alpha"] = float("inf")
    redis = AsyncMock()
    redis.xreadgroup.side_effect = ResponseError("WRONGTYPE")

    with (
        patch.object(docket, "redis", side_effect=lambda: _redis_connection(redis)),
        pytest.raises(ResponseError, match="WRONGTYPE"),
    ):
        await subscription._claim("alpha")


async def test_visibility_renewal_retries_connection_errors(
    docket: Docket, caplog: pytest.LogCaptureFixture
) -> None:
    subscription: QueueSubscription = docket.queue("jobs").subscribe(
        ["alpha"], visibility_timeout=timedelta(milliseconds=40)
    )
    message = subscription._message(
        "alpha", b"1-0", {b"key": b"one", b"data": b"payload"}
    )
    subscription._outstanding.add(message)
    redis = AsyncMock()
    redis.xclaim.side_effect = ConnectionError("offline")

    with (
        caplog.at_level(logging.WARNING),
        patch.object(docket, "redis", side_effect=lambda: _redis_connection(redis)),
        patch(
            "docket.queue.asyncio.sleep",
            new=AsyncMock(side_effect=[None, asyncio.CancelledError()]),
        ),
        pytest.raises(asyncio.CancelledError),
    ):
        await subscription._renew_visibility()
    assert "could not renew message visibility" in caplog.text


async def test_subscription_must_be_active_and_cannot_be_reentered(
    docket: Docket,
) -> None:
    subscription = docket.queue("jobs").subscribe(
        ["alpha"], visibility_timeout=timedelta(seconds=1)
    )

    with pytest.raises(RuntimeError, match="not active"):
        await subscription.receive(timeout=0.01)

    async with subscription:
        with pytest.raises(RuntimeError, match="already active"):
            await subscription.__aenter__()
