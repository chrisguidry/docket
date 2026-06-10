"""Tests for Redis Sentinel URL handling.

These exercise URL parsing and connection pool construction only; building a
Sentinel-backed connection pool is lazy and does not connect, so no live
Sentinel topology is required.
"""

from unittest import mock

import pytest
from redis.asyncio import Redis

from docket import Docket
from docket._redis import (
    DEFAULT_SENTINEL_PORT,
    OwnedSentinelConnectionPool,
    RedisConnection,
    parse_sentinel_url,
)

SENTINEL_URL = "redis+sentinel://sentinel-a:26379,sentinel-b:26379/mymaster/1"


@pytest.mark.parametrize(
    "url,expected",
    [
        ("redis://localhost:6379/0", False),
        ("rediss://localhost:6379/0", False),
        ("memory://", False),
        ("redis+cluster://localhost:6379/0", False),
        ("rediss+cluster://localhost:6379/0", False),
        ("redis+sentinel://localhost:26379/mymaster", True),
        ("rediss+sentinel://localhost:26379/mymaster", True),
        ("redis+sentinel://user:pass@localhost:26379/mymaster/0", True),
    ],
)
def test_is_sentinel_url(url: str, expected: bool):
    """RedisConnection.is_sentinel should correctly identify sentinel URLs."""
    connection = RedisConnection(url)
    assert connection.is_sentinel == expected


def test_sentinel_url_is_neither_cluster_nor_memory():
    """Sentinel URLs should not be routed down the cluster or memory paths."""
    connection = RedisConnection(SENTINEL_URL)
    assert not connection.is_cluster
    assert not connection.is_memory


def test_parse_sentinel_url_minimal():
    """A single member without a port assumes the Sentinel default port."""
    config = parse_sentinel_url("redis+sentinel://sentinel-a/mymaster")
    assert config.sentinels == [("sentinel-a", DEFAULT_SENTINEL_PORT)]
    assert config.service_name == "mymaster"
    assert config.db == 0
    assert config.connection_kwargs == {}
    assert config.sentinel_kwargs == {}


def test_parse_sentinel_url_full():
    """Members, db, master auth, and sentinel auth all parse out of the URL."""
    config = parse_sentinel_url(
        "redis+sentinel://user:s%40crit@sentinel-a:26379,sentinel-b:26380/mymaster/3"
        "?sentinel_username=watcher&sentinel_password=tower"
    )
    assert config.sentinels == [("sentinel-a", 26379), ("sentinel-b", 26380)]
    assert config.service_name == "mymaster"
    assert config.db == 3
    assert config.connection_kwargs == {"username": "user", "password": "s@crit"}
    assert config.sentinel_kwargs == {"username": "watcher", "password": "tower"}


def test_parse_sentinel_url_tls():
    """A rediss prefix turns on TLS for data nodes and Sentinels alike."""
    config = parse_sentinel_url("rediss+sentinel://sentinel-a:26379/mymaster")
    assert config.connection_kwargs == {"ssl": True}
    assert config.sentinel_kwargs == {"ssl": True}


def test_parse_sentinel_url_ipv6_member():
    """Bracketed IPv6 members parse with and without an explicit port."""
    config = parse_sentinel_url("redis+sentinel://[::1]:26380,[fe80::2]/mymaster")
    assert config.sentinels == [("::1", 26380), ("fe80::2", DEFAULT_SENTINEL_PORT)]


def test_parse_sentinel_url_skips_empty_members():
    """Stray commas in the member list are ignored."""
    config = parse_sentinel_url("redis+sentinel://sentinel-a,,sentinel-b,/mymaster")
    assert config.sentinels == [
        ("sentinel-a", DEFAULT_SENTINEL_PORT),
        ("sentinel-b", DEFAULT_SENTINEL_PORT),
    ]


def test_parse_sentinel_url_rejects_missing_host():
    with pytest.raises(ValueError, match="Missing host"):
        parse_sentinel_url("redis+sentinel://:26379/mymaster")


def test_parse_sentinel_url_rejects_invalid_port():
    with pytest.raises(ValueError, match="Invalid port"):
        parse_sentinel_url("redis+sentinel://sentinel-a:notaport/mymaster")


def test_parse_sentinel_url_rejects_empty_member_list():
    with pytest.raises(ValueError, match="at least one sentinel host"):
        parse_sentinel_url("redis+sentinel:///mymaster")


def test_parse_sentinel_url_rejects_missing_service_name():
    with pytest.raises(ValueError, match="requires a service name"):
        parse_sentinel_url("redis+sentinel://sentinel-a:26379")


def test_parse_sentinel_url_rejects_invalid_db():
    with pytest.raises(ValueError, match="Invalid database index"):
        parse_sentinel_url("redis+sentinel://sentinel-a:26379/mymaster/notadb")


async def test_redis_connection_builds_sentinel_pool():
    """Entering a sentinel RedisConnection builds a Sentinel-backed pool."""
    async with RedisConnection(SENTINEL_URL) as connection:
        assert connection.is_connected
        pool = connection._connection_pool  # pyright: ignore[reportPrivateUsage]
        assert isinstance(pool, OwnedSentinelConnectionPool)
        assert pool.service_name == "mymaster"
        assert pool.is_master
        assert pool.connection_kwargs["db"] == 1
        assert len(pool.sentinel_clients) == 2

        async with connection.client() as r:
            assert isinstance(r, Redis)
            assert r.connection_pool is pool

    assert not connection.is_connected


async def test_sentinel_pool_close_also_closes_sentinel_clients():
    """Closing the owned pool closes the Sentinel manager's daemon clients."""
    connection = RedisConnection(SENTINEL_URL)
    pool = await connection._connection_pool_from_url()  # pyright: ignore[reportPrivateUsage]
    assert isinstance(pool, OwnedSentinelConnectionPool)
    expected = list(pool.sentinel_clients)

    closed: list[Redis] = []

    async def tracking_aclose(self: Redis) -> None:
        closed.append(self)

    with mock.patch.object(Redis, "aclose", tracking_aclose):
        await pool.aclose()

    assert closed == expected


async def test_result_storage_pool_decodes_responses_for_sentinel():
    """The result store's decoded pool keeps decode_responses for sentinel URLs."""
    connection = RedisConnection(SENTINEL_URL)
    pool = await connection._connection_pool_from_url(  # pyright: ignore[reportPrivateUsage]
        decode_responses=True
    )
    assert isinstance(pool, OwnedSentinelConnectionPool)
    assert pool.connection_kwargs["decode_responses"] is True
    await pool.aclose()


def test_prefix_is_not_hash_tagged_for_sentinel():
    """Sentinel mode talks to a single logical master, so no hash tags."""
    docket = Docket(name="my-docket", url=SENTINEL_URL)
    assert docket.prefix == "my-docket"
