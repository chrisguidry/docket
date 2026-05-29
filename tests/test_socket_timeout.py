"""Regression tests for redis-py client socket-timeout behavior (issue #425).

These need a real, standalone Redis: the memory backend has no socket, and
cluster mode uses a different connection path.  They run on the real-Redis legs
of the main matrix and across every redis-py major in the test-redis-py job.
"""

# pyright: reportPrivateUsage=false

import time

from docket._redis import RedisConnection
from tests.conftest import skip_cluster, skip_memory


@skip_memory
@skip_cluster
async def test_connection_pool_disables_socket_read_timeout(redis_url: str):
    """Docket's blocking reads must not be cut short by a client read timeout.

    redis-py 8 defaults socket_timeout to 5s, which would abort the 60s strike
    stream xread and the unbounded execution pubsub.listen.  Docket overrides
    socket_timeout to None so blocking reads wait for the server as intended.
    """
    async with RedisConnection(redis_url) as connection:
        assert connection._connection_pool is not None
        redis_connection = connection._connection_pool.make_connection()
        assert redis_connection.socket_timeout is None


@skip_memory
@skip_cluster
async def test_blocking_read_outlasts_redis_py_default_socket_timeout(redis_url: str):
    """A blocking xread longer than redis-py 8's 5s default socket_timeout waits
    for the full block instead of raising TimeoutError partway through."""
    async with RedisConnection(redis_url) as connection:
        async with connection.client() as r:
            start = time.monotonic()
            result = await r.xread({"docket:absent-stream": "$"}, block=6_000)
            elapsed = time.monotonic() - start

    assert not result
    assert elapsed >= 5.5
