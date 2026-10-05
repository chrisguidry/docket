"""Tests for chunked, JUSTID lease renewal.

A worker keeps the messages it holds by renewing their leases: XCLAIM with
``idle=0`` resets each message's idle clock so the sweep does not take it away.
Renewal names every held message, so it goes out in bounded chunks and asks for
JUSTID, and Redis returns only the claimed ids rather than the bodies the worker
already has.
"""

from typing import Any, Sequence, cast

import pytest
from redis.exceptions import ConnectionError

from docket._redelivery import LEASE_RENEWAL_BATCH, renew_leases
from docket._redis import RedisClient, RedisMessageID


class SpyRedis:
    """A stand-in redis client that records every XCLAIM and can fail chosen ones.

    ``renew_leases`` calls only ``xclaim``, so this narrow spy captures the
    kwargs of each chunk.  A chunk whose index is in ``fail_on`` raises, to prove
    one refused chunk does not stop the rest.
    """

    def __init__(self, *, fail_on: Sequence[int] = ()) -> None:
        self.calls: list[dict[str, Any]] = []
        self.fail_on = set(fail_on)

    async def xclaim(self, **kwargs: Any) -> list[RedisMessageID]:
        index = len(self.calls)
        self.calls.append(kwargs)
        if index in self.fail_on:
            raise ConnectionError("Redis refused this chunk")
        return []


def ids(count: int) -> list[RedisMessageID]:
    return [f"{i}-0".encode() for i in range(count)]


async def do_renew(spy: SpyRedis, message_ids: Sequence[RedisMessageID]) -> None:
    await renew_leases(
        cast(RedisClient, spy),
        stream_key="docket:stream",
        group_name="docket:workers",
        consumer_name="worker",
        message_ids=message_ids,
    )


async def test_a_short_list_renews_in_one_call():
    """Fewer ids than one batch renew in a single XCLAIM."""
    spy = SpyRedis()
    await do_renew(spy, ids(3))
    assert len(spy.calls) == 1
    assert spy.calls[0]["message_ids"] == ids(3)


async def test_more_ids_than_the_batch_renew_in_several_calls():
    """More held messages than one batch split across several XCLAIM calls.

    Each chunk names at most ``LEASE_RENEWAL_BATCH`` ids, so no single command
    names every held message and blocks Redis while it runs.
    """
    message_ids = ids(LEASE_RENEWAL_BATCH + 1)
    spy = SpyRedis()

    await do_renew(spy, message_ids)

    assert len(spy.calls) == 2
    assert spy.calls[0]["message_ids"] == message_ids[:LEASE_RENEWAL_BATCH]
    assert spy.calls[1]["message_ids"] == message_ids[LEASE_RENEWAL_BATCH:]


async def test_every_chunk_asks_for_justid_and_zero_idle():
    """Every renewal asks for JUSTID and keeps the idle-resetting semantics.

    JUSTID makes Redis return only ids, not the bodies the worker discards.
    ``min_idle_time=0`` and ``idle=0`` reset the idle clock of every held
    message under the same consumer.
    """
    message_ids = ids(LEASE_RENEWAL_BATCH * 2 + 1)
    spy = SpyRedis()

    await do_renew(spy, message_ids)

    assert len(spy.calls) == 3
    for call in spy.calls:
        assert call["justid"] is True
        assert call["min_idle_time"] == 0
        assert call["idle"] == 0
        assert call["consumername"] == "worker"


async def test_a_refused_chunk_is_logged_and_the_rest_still_renew(
    caplog: pytest.LogCaptureFixture,
):
    """One chunk Redis refuses is logged, and the remaining chunks still renew."""
    message_ids = ids(LEASE_RENEWAL_BATCH * 2 + 1)
    spy = SpyRedis(fail_on=[1])

    await do_renew(spy, message_ids)

    assert len(spy.calls) == 3
    assert spy.calls[0]["message_ids"] == message_ids[:LEASE_RENEWAL_BATCH]
    assert spy.calls[2]["message_ids"] == message_ids[LEASE_RENEWAL_BATCH * 2 :]
    assert "Failed to renew leases" in caplog.text


async def test_no_held_messages_issues_no_call():
    """A worker holding nothing issues no XCLAIM."""
    spy = SpyRedis()
    await do_renew(spy, [])
    assert spy.calls == []
