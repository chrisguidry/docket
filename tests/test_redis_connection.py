"""The Redis client and connection pool that a RedisConnection holds."""

# pyright: reportPrivateUsage=false

from docket._redis import RedisConnection
from tests.conftest import skip_cluster, skip_memory


async def test_client_is_shared_across_uses(redis_url: str) -> None:
    """Every use of client() hands back the same client object.

    Building a redis-py client copies its response-callback table, which costs
    real CPU when a worker does it several times per task.
    """
    async with RedisConnection(redis_url) as connection:
        async with connection.client() as first:
            pass
        async with connection.client() as second:
            pass

    assert first is second


async def test_client_stays_usable_after_a_use_ends(redis_url: str) -> None:
    """Leaving one client() block leaves the shared client ready for the next."""
    async with RedisConnection(redis_url) as connection:
        key = connection.prefix("shared-client")

        async with connection.client() as r:
            await r.set(key, b"1")

        async with connection.client() as r:
            assert await r.get(key) == b"1"
            await r.delete(key)


@skip_memory
@skip_cluster
async def test_connection_pool_closes_when_the_connection_exits(  # pragma: no cover
    redis_url: str,
) -> None:
    """The shared client does not hold the pool open past the connection.

    Needs a real, standalone Redis: the memory backend has no pool and cluster
    mode reaches Redis through a different client.  The body is unreachable on
    those two backends by design, so it's marked ``no cover`` -- per-job
    ``--cov-fail-under=100`` is enforced on every backend independently.
    """
    connection = RedisConnection(redis_url)
    async with connection:
        pool = connection._connection_pool
        assert pool is not None

        key = connection.prefix("pool-close")
        async with connection.client() as r:
            await r.set(key, b"1")
            await r.delete(key)

        assert any(c.is_connected for c in pool._available_connections)

    assert not any(c.is_connected for c in pool._available_connections)
