"""Regression tests for the RESP version of docket's pub/sub connections.

These need a real Redis: the memory backend never speaks RESP.  They run on
the real-Redis legs of the main matrix and across every redis-py major in the
test-redis-py job.
"""

# pyright: reportPrivateUsage=false

from typing import cast

import pytest
from redis._parsers import _AsyncRESP3Parser
from redis.asyncio.connection import AbstractConnection, Connection

from docket.docket import Docket
from tests._container import PROVIDER_ENABLED
from tests.conftest import skip_cluster, skip_memory


async def subscribed_connection(docket: Docket) -> AbstractConnection:
    async with docket._redis.pubsub() as pubsub:
        await pubsub.subscribe(docket.key("resp-version"))
        return cast(AbstractConnection, getattr(pubsub, "connection"))


@skip_memory
@pytest.mark.skipif(PROVIDER_ENABLED, reason="every connection has a provider")
async def test_pubsub_connections_speak_resp2_without_a_streaming_provider(
    docket: Docket,
):
    """Without a provider that rotates, subscribers negotiate RESP2, where a
    message is not a push reply.

    Under RESP3 every pub/sub message arrives as a push reply, and hiredis-py
    3.4.1 and earlier leak a list for each one.  Docket's subscribers listen
    for the life of a worker, so that leak has no bound.
    """
    connection = await subscribed_connection(docket)

    assert connection.protocol == 2


@skip_memory
@pytest.mark.skipif(not PROVIDER_ENABLED, reason="needs a streaming provider")
async def test_pubsub_connections_speak_resp3_with_a_streaming_provider(
    docket: Docket,
):
    """A subscribed connection can send AUTH only under RESP3, and it reads
    with redis-py's Python parser, which doesn't leak push replies."""
    connection = await subscribed_connection(docket)

    assert connection.protocol == 3
    assert type(connection._parser) is _AsyncRESP3Parser


@skip_memory
@skip_cluster
async def test_data_connections_keep_the_client_default_resp_version(docket: Docket):
    """Only pub/sub chooses its RESP version; every other command keeps
    redis-py's default.

    The data pool must not carry an explicit ``protocol`` at all: redis-py 6
    turns ``protocol=None`` into a ``HELLO None`` handshake that fails on
    every connection, which the newer versions mask by normalizing None.
    """
    assert docket._redis._connection_pool is not None
    assert "protocol" not in docket._redis._connection_pool.connection_kwargs
    connection = docket._redis._connection_pool.make_connection()

    assert connection.protocol == Connection().protocol
