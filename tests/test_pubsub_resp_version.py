"""Regression tests for the RESP version of docket's pub/sub connections.

These need a real, standalone Redis: the memory backend never speaks RESP, and
cluster mode subscribes through a different pool.  They run on the real-Redis
legs of the main matrix and across every redis-py major in the test-redis-py
job.
"""

# pyright: reportPrivateUsage=false

from typing import cast

from redis.asyncio.connection import AbstractConnection, Connection

from docket.docket import Docket
from tests.conftest import skip_cluster, skip_memory


@skip_memory
@skip_cluster
async def test_pubsub_connections_speak_resp2(docket: Docket):
    """Subscribers must negotiate RESP2, where a message is not a push reply.

    Under RESP3 every pub/sub message arrives as a push reply, and hiredis-py
    leaks a list for each one.  Docket's subscribers listen for the life of a
    worker, so that leak has no bound.
    """
    async with docket._pubsub() as pubsub:
        await pubsub.subscribe(docket.key("resp-version"))
        connection = cast(AbstractConnection, getattr(pubsub, "connection"))

    assert connection.protocol == 2


@skip_memory
@skip_cluster
async def test_data_connections_keep_the_client_default_resp_version(docket: Docket):
    """Only pub/sub drops to RESP2; every other command keeps redis-py's default."""
    assert docket._redis._connection_pool is not None
    connection = docket._redis._connection_pool.make_connection()

    assert connection.protocol == Connection().protocol
