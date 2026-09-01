"""Tests for the fleet-wide lease that paces a periodic, Redis-heavy job.

Only one worker per interval should run a scan that costs the same however
large the fleet is.  ``take_lease`` is that gate: the first caller wins the
lease, later callers lose it until it expires on its own.
"""

import asyncio
import importlib
import time
from typing import cast

import pytest

from docket._lease import take_lease
from docket._redis import RedisClient


@pytest.fixture
def lease_redis() -> RedisClient:
    """An in-process Redis for the lease.

    The lease logic is backend-independent, so these unit tests run it against a
    burner Redis whatever backend the suite targets.
    """
    burner_redis = importlib.import_module("burner_redis")
    return cast(RedisClient, burner_redis.BurnerRedis())


async def test_the_first_caller_wins_the_lease(lease_redis: RedisClient):
    """The first caller to take a free lease wins it."""
    assert await take_lease(lease_redis, "lease", "worker-a", 1000) is True


async def test_a_second_caller_loses_a_held_lease(lease_redis: RedisClient):
    """While one caller holds the lease, a second caller loses it."""
    assert await take_lease(lease_redis, "lease", "worker-a", 1000) is True
    assert await take_lease(lease_redis, "lease", "worker-b", 1000) is False


async def test_the_holder_is_recorded_on_the_key(lease_redis: RedisClient):
    """The winner's name is stored on the lease key."""
    await take_lease(lease_redis, "lease", "worker-a", 1000)
    assert await lease_redis.get("lease") == b"worker-a"


async def test_the_lease_is_winnable_again_after_it_expires(lease_redis: RedisClient):
    """Once the lease expires nobody renews it, so the next caller wins it.

    A short expiry with a real Redis: poll until the key lapses rather than
    sleeping a fixed time that races the backend's clock.
    """
    assert await take_lease(lease_redis, "lease", "worker-a", 50) is True

    deadline = time.monotonic() + 2.0
    while await take_lease(lease_redis, "lease", "worker-b", 50) is False:
        assert time.monotonic() < deadline, "lease never expired"
        await asyncio.sleep(0.01)
