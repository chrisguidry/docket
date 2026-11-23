"""Re-authenticating docket's connections when a credential provider rotates.

redis-py registers a re-auth callback with a StreamingCredentialProvider for
each client that it gives the provider.  A provider like redis-entraid's keeps
only the newest callback, and docket opens many clients on one provider: the
data and pub/sub clients, the result store, and the strike list.  So docket
gives the provider only to its pools, which never register, and registers one
callback for each provider here.  That callback re-authenticates every open
pool and cluster that uses the provider, and every subscribed connection.

A subscribed RESP2 connection can't send AUTH, but a subscribed RESP3
connection can, so with a streaming provider the pub/sub connections negotiate
RESP3.  The callback writes AUTH onto each subscribed connection and reads
nothing: the subscriber's own reader is already waiting on that socket, and it
reads the reply as a pub/sub message of type "pong".  The connection is never
closed, so the subscriber loses no message and receives none twice.  redis-py
has its own re-auth for RESP3 pub/sub, but it reads the AUTH reply itself,
which fails while the subscriber's reader waits on the socket, and it registers
a callback with the provider for every subscriber, so docket turns it off.

A connection in use during a refresh gets its AUTH when it comes back to its
pool.  redis-py sends that AUTH from a listener that runs after the connection
is already available again, so another task can take the connection and read
from the socket before the AUTH reply arrives (redis/redis-py#4433).  docket's
pools are ReauthenticatingPools, which send it before giving the connection back.
"""

import logging
from contextlib import AbstractContextManager, contextmanager
from typing import Generator, TypedDict, TypeVar
from weakref import WeakKeyDictionary

from redis._parsers import BaseParser, _AsyncRESP3Parser
from redis.asyncio import ConnectionPool
from redis.asyncio.client import PubSub
from redis.asyncio.cluster import ClusterNode, RedisCluster
from redis.asyncio.connection import AbstractConnection
from redis.auth.token import TokenInterface
from redis.credentials import CredentialProvider, StreamingCredentialProvider
from redis.event import AfterPubSubConnectionInstantiationEvent, EventDispatcher

logger: logging.Logger = logging.getLogger(__name__)

T = TypeVar("T")


class ProtocolOptions(TypedDict, total=False):
    """The RESP version a pool negotiates, and the parser that reads it.  A
    missing key leaves redis-py's default."""

    protocol: int
    parser_class: type[BaseParser]


def pubsub_protocol(provider: CredentialProvider | None) -> ProtocolOptions:
    """The protocol for docket's pub/sub connections.

    Only a streaming provider rotates, and only RESP3 lets a subscribed
    connection send AUTH, so the pub/sub connections negotiate RESP3 with a
    streaming provider and RESP2 otherwise.

    Under RESP3 every pub/sub message is a push reply, and hiredis-py 3.4.1 and
    earlier leak one small list for each one (redis/hiredis-py#235, fixed in
    3.4.2 by redis/hiredis-py#239).  docket's subscribers listen for the life
    of a worker, so that leak has no bound: about 100 bytes per message until
    the process dies.  docket can't choose which hiredis-py an application
    installs, so RESP3 pub/sub reads with redis-py's own Python parser, which
    doesn't leak.  RESP2 pub/sub messages are not push replies, so the
    RESP2 connections keep whichever parser redis-py picks.
    """
    if isinstance(provider, StreamingCredentialProvider):
        return {"protocol": 3, "parser_class": _AsyncRESP3Parser}
    return {"protocol": 2}


class WithoutPubSubReauth(EventDispatcher):
    """redis-py's event dispatcher, without its re-auth for pub/sub.

    For each RESP3 pub/sub connection with a streaming provider, redis-py
    registers a callback with the provider that sends AUTH and then reads the
    reply.  A provider like redis-entraid's keeps only the newest callback, so
    that registration takes the refreshes away from docket's Rotation.  And the
    read fails with "readuntil() called while another coroutine is already
    waiting" while a subscriber waits on the same socket, which breaks the
    connection.  The Rotation re-authenticates subscribers instead.
    """

    def dispatch(self, event: object) -> None:
        if isinstance(event, AfterPubSubConnectionInstantiationEvent):
            return
        super().dispatch(event)


class ReauthenticatingPool(ConnectionPool):
    """A ConnectionPool that sends a connection's pending AUTH while the
    connection is still in use, so redis-py's release listener finds nothing
    left to send."""

    async def release(self, connection: AbstractConnection) -> None:
        try:
            if connection.is_connected:
                await connection.re_auth()
            else:
                # A closed connection authenticates with the provider's current
                # credentials when it next connects, so sending its queued AUTH
                # would only reconnect it early.  A subscriber's connection
                # always comes back closed.
                connection.set_re_auth_token(None)  # pyright: ignore[reportArgumentType]
        except BaseException:
            # A failed AUTH leaves its token queued, and redis-py's listener
            # would send it again once the connection is available.  Without
            # the token and the socket, the next use reconnects with the
            # provider's current credentials instead.
            connection.set_re_auth_token(None)  # pyright: ignore[reportArgumentType]
            await connection.disconnect(nowait=True)
            raise
        finally:
            await super().release(connection)


@contextmanager
def _member(members: set[T], member: T) -> Generator[None, None, None]:
    members.add(member)
    try:
        yield
    finally:
        members.discard(member)


class Rotation:
    """The open pools, clusters, and subscribers that one provider's refreshed
    credentials must reach."""

    def __init__(self) -> None:
        self._pools: set[ConnectionPool] = set()
        self._clusters: set[RedisCluster] = set()
        self._subscribers: set[PubSub] = set()

    def following_pool(self, pool: ConnectionPool) -> AbstractContextManager[None]:
        return _member(self._pools, pool)

    def following_cluster(
        self, cluster: RedisCluster
    ) -> AbstractContextManager[None]:  # pragma: no cover - needs a cluster
        return _member(self._clusters, cluster)

    def following_subscriber(
        self, pubsub: PubSub
    ) -> AbstractContextManager[None]:  # pragma: no cover - needs a real Redis
        return _member(self._subscribers, pubsub)

    async def reauthenticate(
        self, token: TokenInterface
    ) -> None:  # pragma: no cover - needs a real Redis
        # A pool sends AUTH on its idle connections now, and on each connection
        # in use when that connection comes back to the pool.  A pool's AUTH
        # reads its reply, so when a pool has an idle connection, a token the
        # server rejects raises here, before any subscriber is sent it.
        for pool in list(self._pools):
            await pool.re_auth_callback(token)
        for cluster in list(self._clusters):
            for node in cluster_nodes(cluster):
                await node.re_auth_callback(token)
        for pubsub in list(self._subscribers):
            await reauthenticate_subscriber(pubsub, token)

    async def log_error(
        self, error: Exception
    ) -> None:  # pragma: no cover - needs a failing provider
        logger.warning("The credential provider failed to refresh", exc_info=error)


def cluster_nodes(
    cluster: RedisCluster,
) -> list[ClusterNode]:  # pragma: no cover - needs a cluster
    """Every node object of ``cluster`` that holds connections.

    redis-py reads the cluster's slots through its startup nodes, and when it
    copies the discovered nodes into ``startup_nodes`` it keeps the startup
    node it already has, so that connection belongs to an object that
    ``get_nodes()`` doesn't return.  Without it, the slots connection keeps
    the old token, and the server closes it when that token expires.
    """
    manager = cluster.nodes_manager
    nodes = [*manager.nodes_cache.values(), *manager.startup_nodes.values()]
    return list({id(node): node for node in nodes}.values())


async def reauthenticate_subscriber(
    pubsub: PubSub, token: TokenInterface
) -> None:  # pragma: no cover - needs a real Redis
    """Write AUTH onto a subscribed connection and leave the reply to its reader.

    A subscriber whose connection is closed authenticates when it reconnects,
    with the provider's current credentials.  The server's reply arrives at the
    subscriber's reader: "+OK", which Subscription drops, or an error, which
    redis-py raises from the reader's next read, as it raises a rejected AUTH
    from any command.
    """
    connection = pubsub.connection
    if connection is None or not connection.is_connected:
        return
    try:
        # The reader is waiting for a reply on this socket, so this only
        # writes.  Health checks read a reply, so they are off.
        await connection.send_command(
            "AUTH", token.try_get("oid"), token.get_value(), check_health=False
        )
    except Exception:
        logger.warning(
            "Could not re-authenticate a subscribed connection", exc_info=True
        )


_rotations: "WeakKeyDictionary[StreamingCredentialProvider, Rotation]" = (
    WeakKeyDictionary()
)


def rotation_for(provider: CredentialProvider | None) -> Rotation:
    """The Rotation that a provider's refreshes reach.  Only a streaming
    provider refreshes, so any other provider gets one that never runs."""
    if not isinstance(provider, StreamingCredentialProvider):
        return Rotation()
    if provider not in _rotations:
        rotation = Rotation()
        # redis-py types these callbacks as sync, but its own async clients
        # register coroutine functions here, and the async token managers
        # await them.
        provider.on_next(rotation.reauthenticate)  # pyright: ignore[reportArgumentType]
        provider.on_error(rotation.log_error)  # pyright: ignore[reportArgumentType]
        _rotations[provider] = rotation
    return _rotations[provider]


class WithoutRotation(CredentialProvider):
    """A provider's credentials without its stream of refreshes.

    RedisCluster registers its own re-auth callback with a streaming provider
    each time it initializes, which would take the refreshes away from every
    other connection.  This gives it the credentials and nothing to register.
    """

    def __init__(
        self, provider: CredentialProvider
    ) -> None:  # pragma: no cover - needs a cluster
        self._provider = provider

    def get_credentials(self) -> tuple[str] | tuple[str, str]:  # pragma: no cover
        return self._provider.get_credentials()

    async def get_credentials_async(
        self,
    ) -> tuple[str] | tuple[str, str]:  # pragma: no cover - needs a cluster
        return await self._provider.get_credentials_async()
