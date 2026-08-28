"""Tests for the XCLAIM calls that renew a worker's leases.

A worker holds one message per running task, and one XCLAIM naming every
held id makes Redis serialize every message back to a worker that discards
the reply.  These tests cover how those calls are split and what they ask
for.
"""

from typing import Any, cast

import pytest
from redis.exceptions import ConnectionError

from docket import Docket
from docket._redelivery import LEASE_RENEWAL_BATCH, renew_leases
from docket._redis import RedisClient


class RecordingXclaim:
    """Delegates to a real client and records the XCLAIM calls it makes."""

    def __init__(self, wrapped: Any, fail_first: bool = False) -> None:
        self._wrapped = wrapped
        self._fail_first = fail_first
        self.calls: list[dict[str, Any]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    async def xclaim(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if self._fail_first and len(self.calls) == 1:
            raise ConnectionError("Simulated Redis error")
        return await self._wrapped.xclaim(**kwargs)


async def renewable_message_ids(docket: Docket, count: int) -> list[bytes]:
    """Message ids for a group that exists, so XCLAIM answers them."""
    await docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]
    return [f"{number}-0".encode() for number in range(1, count + 1)]


async def test_lease_renewal_claims_in_chunks_and_asks_only_for_ids(docket: Docket):
    """Renewal splits the ids across XCLAIM calls, and each call uses JUSTID.

    One XCLAIM naming every held id blocks Redis while it serializes every
    message back to a worker that throws the reply away.
    """
    message_ids = await renewable_message_ids(docket, LEASE_RENEWAL_BATCH + 1)

    async with docket.redis() as redis:
        recording = RecordingXclaim(redis)
        await renew_leases(
            cast(RedisClient, recording),
            stream_key=docket.stream_key,
            group_name=docket.worker_group_name,
            consumer_name="worker-a",
            message_ids=message_ids,
        )

    assert [len(call["message_ids"]) for call in recording.calls] == [
        LEASE_RENEWAL_BATCH,
        1,
    ]
    assert all(call["justid"] for call in recording.calls)


async def test_lease_renewal_claims_the_rest_after_a_chunk_fails(
    docket: Docket, caplog: pytest.LogCaptureFixture
):
    """A chunk that fails leaves the other chunks of that pass renewed."""
    message_ids = await renewable_message_ids(docket, LEASE_RENEWAL_BATCH + 1)

    async with docket.redis() as redis:
        recording = RecordingXclaim(redis, fail_first=True)
        await renew_leases(
            cast(RedisClient, recording),
            stream_key=docket.stream_key,
            group_name=docket.worker_group_name,
            consumer_name="worker-a",
            message_ids=message_ids,
        )

    assert len(recording.calls) == 2
    assert "Failed to renew leases" in caplog.text
