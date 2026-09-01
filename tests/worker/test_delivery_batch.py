"""Tests for the cap on how many messages one Redis command may claim."""

from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncGenerator
from unittest.mock import patch

import pytest

from docket._redis import RedisClient
from docket.worker import MESSAGE_BATCH

from docket import Docket, Worker

# A batch small enough that a test backlog needs several commands to drain.
A_SMALL_BATCH = 5

# More messages than one batch, and more than one round of batches.
A_BURST = A_SMALL_BATCH * 4 + 1


def test_the_default_batch_is_a_thousand_messages():
    """The default balances round trips against the size of one reply."""
    assert MESSAGE_BATCH == 1000


@pytest.mark.parametrize("batch", [0, -1])
def test_a_batch_below_one_is_rejected(docket: Docket, batch: int):
    """A batch of zero would ask Redis for no messages, so the worker would
    never make progress."""
    with pytest.raises(ValueError, match="message_batch must be at least 1"):
        Worker(docket, message_batch=batch)


async def test_a_burst_larger_than_one_batch_still_drains(docket: Docket):
    """The poll loop reads again while slots are free, so the cap splits a
    burst across commands rather than leaving any of it behind."""
    ran = 0

    async def counter() -> None:
        nonlocal ran
        ran += 1

    docket.register(counter)

    await docket.add_many([docket.call(counter)() for _ in range(A_BURST)])

    async with Worker(
        docket,
        concurrency=A_BURST * 2,
        message_batch=A_SMALL_BATCH,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        await worker.run_until_finished()

    assert ran == A_BURST


class CountingClient:
    """Delegates to a real client, recording what each command asked for."""

    def __init__(self, wrapped: Any, counts: list[int]):
        self._wrapped = wrapped
        self._counts = counts

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
        self._counts.append(kwargs["count"])
        return await self._wrapped.xreadgroup(*args, **kwargs)

    async def xautoclaim(self, *args: Any, **kwargs: Any) -> Any:
        self._counts.append(kwargs["count"])
        return await self._wrapped.xautoclaim(*args, **kwargs)


@asynccontextmanager
async def counting_redis(
    docket: Docket, counts: list[int]
) -> AsyncGenerator[None, None]:
    """Record the ``count`` of every delivery read and sweep claim."""
    original_redis = Docket.redis

    @asynccontextmanager
    async def wrapped(self: Docket) -> AsyncGenerator[RedisClient, None]:
        async with original_redis(self) as redis:
            yield CountingClient(redis, counts)  # type: ignore[arg-type]

    with patch.object(Docket, "redis", wrapped):
        yield


async def test_no_command_asks_for_more_than_the_batch(docket: Docket):
    """However many slots are free, no XREADGROUP or XAUTOCLAIM asks for more
    messages than the worker's batch."""
    counts: list[int] = []

    ran = 0

    async def counter() -> None:
        nonlocal ran
        ran += 1

    docket.register(counter)

    await docket.add_many([docket.call(counter)() for _ in range(A_BURST)])

    async with counting_redis(docket, counts):
        async with Worker(
            docket,
            concurrency=A_BURST * 2,
            message_batch=A_SMALL_BATCH,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            await worker.run_until_finished()

    assert ran == A_BURST
    assert counts, "the worker never asked Redis for messages"
    assert max(counts) == A_SMALL_BATCH
