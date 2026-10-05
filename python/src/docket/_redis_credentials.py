"""Re-authenticating docket's connections when a credential provider rotates.

redis-py registers a re-auth callback with a StreamingCredentialProvider for
each client that it gives the provider.  A provider like redis-entraid's keeps
only the newest callback, and docket opens many clients on one provider: the
data and pub/sub clients, the result store, and the strike list.  So docket
gives the provider only to its pools, which never register, and registers one
callback for each provider here.  That callback re-authenticates every open
pool and cluster that uses the provider.

A subscribed RESP2 connection can't send AUTH, so it keeps the credentials it
connected with.  Its reader owns its socket, and a reconnect from here would
race the reader for it.
"""

import logging
from contextlib import AbstractContextManager, contextmanager
from typing import Generator, TypeVar
from weakref import WeakKeyDictionary

from redis.asyncio import ConnectionPool
from redis.asyncio.cluster import RedisCluster
from redis.auth.token import TokenInterface
from redis.credentials import CredentialProvider, StreamingCredentialProvider

logger: logging.Logger = logging.getLogger(__name__)

T = TypeVar("T")


@contextmanager
def _member(members: set[T], member: T) -> Generator[None, None, None]:
    members.add(member)
    try:
        yield
    finally:
        members.discard(member)


class Rotation:
    """The open pools and clusters that one provider's refreshed credentials
    must reach."""

    def __init__(self) -> None:
        self._pools: set[ConnectionPool] = set()
        self._clusters: set[RedisCluster] = set()

    def following_pool(self, pool: ConnectionPool) -> AbstractContextManager[None]:
        return _member(self._pools, pool)

    def following_cluster(
        self, cluster: RedisCluster
    ) -> AbstractContextManager[None]:  # pragma: no cover - needs a cluster
        return _member(self._clusters, cluster)

    async def reauthenticate(
        self, token: TokenInterface
    ) -> None:  # pragma: no cover - needs a real Redis
        # A pool sends AUTH on its idle connections now, and on each connection
        # in use when that connection comes back to the pool.  A subscriber's
        # connection is disconnected before it comes back, so it never gets
        # that AUTH.
        for pool in list(self._pools):
            await pool.re_auth_callback(token)
        for cluster in list(self._clusters):
            for node in cluster.get_nodes():
                await node.re_auth_callback(token)

    async def log_error(
        self, error: Exception
    ) -> None:  # pragma: no cover - needs a failing provider
        logger.warning("The credential provider failed to refresh", exc_info=error)


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
