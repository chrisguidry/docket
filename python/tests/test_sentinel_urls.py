"""Tests for Redis Sentinel URL handling.

These exercise URL parsing and connection pool construction only; building a
Sentinel-backed connection pool is lazy and does not connect, so no live
Sentinel topology is required.
"""

from unittest import mock

import pytest
from redis.asyncio import Redis

from docket import Docket
from docket._redis import RedisConnection
from docket._redis_sentinel import (
    DEFAULT_SENTINEL_PORT,
    SENTINEL_SOCKET_KEEPALIVE_OPTIONS,
    OwnedSentinelConnectionPool,
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
    """Bracketed IPv6 members parse with and without an explicit port, in any
    position — every currently-supported CPython rejects netlocs with data
    before a bracket, so the netloc never goes through urlsplit verbatim."""
    config = parse_sentinel_url("redis+sentinel://s1,[::1]:26380,[fe80::2]/mymaster")
    assert config.sentinels == [
        ("s1", DEFAULT_SENTINEL_PORT),
        ("::1", 26380),
        ("fe80::2", DEFAULT_SENTINEL_PORT),
    ]


def test_redis_connection_accepts_ipv6_member_after_hostname():
    """RedisConnection construction itself must tolerate IPv6 members that
    follow another member in the netloc."""
    connection = RedisConnection("redis+sentinel://s1:26379,[::1]:26380/mymaster")
    assert connection.is_sentinel


def test_redis_connection_tolerates_scheme_less_url():
    """A URL without :// falls back to plain urlparse."""
    connection = RedisConnection("localhost:6379")
    assert not connection.is_sentinel
    assert not connection.is_cluster
    assert not connection.is_memory


def test_parse_sentinel_url_skips_empty_members():
    """Stray commas in the member list are ignored."""
    config = parse_sentinel_url("redis+sentinel://sentinel-a,,sentinel-b,/mymaster")
    assert config.sentinels == [
        ("sentinel-a", DEFAULT_SENTINEL_PORT),
        ("sentinel-b", DEFAULT_SENTINEL_PORT),
    ]


def test_parse_sentinel_url_passes_through_pool_options():
    """Standard redis-py options in the query string apply to the data nodes,
    with the same type conversion as a standalone redis:// URL."""
    config = parse_sentinel_url(
        "redis+sentinel://sentinel-a/mymaster"
        "?max_connections=50&socket_timeout=7.5&health_check_interval=30"
    )
    assert config.connection_kwargs == {
        "max_connections": 50,
        "socket_timeout": 7.5,
        "health_check_interval": 30,
    }
    assert config.sentinel_kwargs == {}


def test_parse_sentinel_url_tls_shares_ssl_options_with_sentinels():
    """On the rediss+sentinel scheme the ssl_* options apply to the Sentinel
    daemon connections too, so one private CA verifies the whole topology
    (discovery would otherwise fail certificate verification)."""
    config = parse_sentinel_url(
        "rediss+sentinel://sentinel-a/mymaster"
        "?ssl_ca_certs=/etc/redis/ca.pem&ssl_check_hostname=false&socket_timeout=5"
    )
    assert config.connection_kwargs == {
        "ssl": True,
        "ssl_ca_certs": "/etc/redis/ca.pem",
        "ssl_check_hostname": False,
        "socket_timeout": 5.0,
    }
    assert config.sentinel_kwargs == {
        "ssl": True,
        "ssl_ca_certs": "/etc/redis/ca.pem",
        "ssl_check_hostname": False,
    }


def test_parse_sentinel_url_plaintext_sentinels_get_no_ssl_options():
    """Without TLS the daemons stay plaintext: ssl_* options pass through to
    the data-node kwargs only, like any other redis-py option."""
    config = parse_sentinel_url(
        "redis+sentinel://sentinel-a/mymaster?ssl_ca_certs=/etc/redis/ca.pem"
    )
    assert config.connection_kwargs == {"ssl_ca_certs": "/etc/redis/ca.pem"}
    assert config.sentinel_kwargs == {}


def test_parse_sentinel_url_db_comes_from_path_only():
    """The database is taken from the path; a stray ?db= is not an alternate
    source, and credentials stay sourced from the URL userinfo."""
    config = parse_sentinel_url("redis+sentinel://sentinel-a/mymaster/2?db=5")
    assert config.db == 2
    assert "db" not in config.connection_kwargs


def test_parse_sentinel_url_credentials_come_from_userinfo_only():
    """Master credentials come from the URL userinfo; stray ?username=/?password=
    query parameters are not honored as an alternate source."""
    config = parse_sentinel_url(
        "redis+sentinel://user:pass@sentinel-a/mymaster?username=qu&password=qp"
    )
    assert config.connection_kwargs["username"] == "user"
    assert config.connection_kwargs["password"] == "pass"


def test_parse_sentinel_url_rejects_invalid_option_value():
    with pytest.raises(ValueError, match="Invalid value for 'max_connections'"):
        parse_sentinel_url("redis+sentinel://sentinel-a/mymaster?max_connections=lots")


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


async def test_sentinel_pool_honors_url_pool_options():
    """URL query options reach the pool, overriding docket's defaults the way
    ConnectionPool.from_url lets a standalone URL's socket_timeout win."""
    connection = RedisConnection(
        "redis+sentinel://sentinel-a/mymaster?max_connections=50&socket_timeout=7.5"
    )
    pool = await connection._connection_pool_from_url()  # pyright: ignore[reportPrivateUsage]
    assert isinstance(pool, OwnedSentinelConnectionPool)
    assert pool.max_connections == 50
    assert pool.connection_kwargs["socket_timeout"] == 7.5
    await pool.aclose()


async def test_sentinel_pool_defaults_tight_keepalive():
    """docket disables the read timeout, so the Sentinel pool must default tight
    TCP keepalive to notice a silently-dead master rather than waiting on the
    OS-default keepalive (hours), which Sentinel outpaces by failing over in
    seconds."""
    connection = RedisConnection(SENTINEL_URL)
    pool = await connection._connection_pool_from_url()  # pyright: ignore[reportPrivateUsage]
    assert isinstance(pool, OwnedSentinelConnectionPool)
    assert pool.connection_kwargs["socket_keepalive"] is True
    assert (
        pool.connection_kwargs["socket_keepalive_options"]
        == SENTINEL_SOCKET_KEEPALIVE_OPTIONS
    )
    # The probe timers are populated from the platform's TCP keepalive constants.
    assert SENTINEL_SOCKET_KEEPALIVE_OPTIONS
    await pool.aclose()


async def test_sentinel_pool_keepalive_is_overridable_from_url():
    """A socket_keepalive in the URL still wins over docket's default, the way
    any URL pool option overrides the defaults."""
    connection = RedisConnection(
        "redis+sentinel://sentinel-a/mymaster?socket_keepalive=false"
    )
    pool = await connection._connection_pool_from_url()  # pyright: ignore[reportPrivateUsage]
    assert isinstance(pool, OwnedSentinelConnectionPool)
    assert pool.connection_kwargs["socket_keepalive"] is False
    await pool.aclose()


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
