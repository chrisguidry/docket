"""Redis connection management.

This module is the single point of control for Redis connections, including
the burner-redis backend used for memory:// URLs.

This module is designed to be the single point of cluster-awareness, so that
other modules can remain simple. When Redis Cluster support is added, only
this module will need to change.
"""

import asyncio
import logging
import typing
from contextlib import AsyncExitStack, asynccontextmanager
from types import TracebackType
from typing import Any, AsyncGenerator, Protocol, runtime_checkable
from urllib.parse import ParseResult, urlparse, urlunparse

from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.client import PubSub
from redis.asyncio.cluster import RedisCluster
from redis.asyncio.connection import Connection, SSLConnection

if typing.TYPE_CHECKING:
    from burner_redis import BurnerRedis


logger: logging.Logger = logging.getLogger(__name__)


class AsyncCloseable(Protocol):
    """Protocol for objects with an async aclose() method."""

    async def aclose(self) -> None: ...


@runtime_checkable
class RedisClient(Protocol):
    """Protocol capturing the Redis client interface that docket uses.

    This is the structural type shared by redis.asyncio.Redis,
    redis.asyncio.cluster.RedisCluster, and burner_redis.BurnerRedis.
    Method signatures use Any to accommodate differences between implementations.
    """

    async def get(self, *args: Any, **kwargs: Any) -> Any: ...
    async def set(self, *args: Any, **kwargs: Any) -> Any: ...
    async def exists(self, *args: Any, **kwargs: Any) -> Any: ...
    async def keys(self, *args: Any, **kwargs: Any) -> Any: ...
    async def type(self, *args: Any, **kwargs: Any) -> Any: ...
    async def ttl(self, *args: Any, **kwargs: Any) -> Any: ...
    async def delete(self, *args: Any, **kwargs: Any) -> Any: ...
    async def expire(self, *args: Any, **kwargs: Any) -> Any: ...
    async def hget(self, *args: Any, **kwargs: Any) -> Any: ...
    async def hgetall(self, *args: Any, **kwargs: Any) -> Any: ...
    async def hdel(self, *args: Any, **kwargs: Any) -> Any: ...
    async def hincrby(self, *args: Any, **kwargs: Any) -> Any: ...
    async def hset(self, *args: Any, **kwargs: Any) -> Any: ...
    async def sadd(self, *args: Any, **kwargs: Any) -> Any: ...
    async def srem(self, *args: Any, **kwargs: Any) -> Any: ...
    async def smembers(self, *args: Any, **kwargs: Any) -> Any: ...
    async def scard(self, *args: Any, **kwargs: Any) -> Any: ...
    async def zadd(self, *args: Any, **kwargs: Any) -> Any: ...
    async def zrem(self, *args: Any, **kwargs: Any) -> Any: ...
    async def zcard(self, *args: Any, **kwargs: Any) -> Any: ...
    async def zrange(self, *args: Any, **kwargs: Any) -> Any: ...
    async def zrangebyscore(self, *args: Any, **kwargs: Any) -> Any: ...
    async def zscore(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xadd(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xack(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xautoclaim(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xclaim(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xread(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xrange(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xlen(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xtrim(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xpending(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xinfo_groups(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xinfo_consumers(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xinfo_stream(self, *args: Any, **kwargs: Any) -> Any: ...
    async def xgroup_create(self, *args: Any, **kwargs: Any) -> Any: ...
    async def info(self, *args: Any, **kwargs: Any) -> Any: ...
    async def publish(self, *args: Any, **kwargs: Any) -> Any: ...
    def pubsub(self, **kwargs: Any) -> Any: ...
    def scan_iter(self, *args: Any, **kwargs: Any) -> Any: ...
    def register_script(self, script: str | bytes) -> Any: ...
    def pipeline(self, **kwargs: Any) -> Any: ...
    def lock(self, *args: Any, **kwargs: Any) -> Any: ...


async def close_resource(resource: AsyncCloseable, name: str) -> None:
    """Close a resource with error handling.

    Designed to be used with AsyncExitStack.push_async_callback().
    """
    try:
        await resource.aclose()
    except Exception:  # pragma: no cover
        logger.warning("Failed to close %s", name, exc_info=True)


# Cache of BurnerRedis instances keyed by URL
_memory_servers: dict[str, "BurnerRedis"] = {}
_memory_servers_lock = asyncio.Lock()


async def clear_memory_servers() -> None:
    """Clear all cached BurnerRedis instances.

    This is primarily for testing to ensure isolation between tests.
    """
    async with _memory_servers_lock:
        _memory_servers.clear()


def get_memory_server(url: str) -> "BurnerRedis | None":
    """Get the cached BurnerRedis instance for a URL, if any.

    This is primarily for testing to verify server isolation.
    """
    return _memory_servers.get(url)


class RedisConnection:
    """Manages Redis connections for both standalone and cluster modes.

    This class encapsulates the lifecycle management of Redis connections,
    hiding whether the underlying connection is to a standalone Redis server
    or a Redis Cluster. It provides a unified interface for getting Redis
    clients, pub/sub connections, and publishing messages.

    Example:
        async with RedisConnection("redis://localhost:6379/0") as connection:
            async with connection.client() as r:
                await r.set("key", "value")
    """

    # Standalone mode: connection pool for all Redis operations
    _connection_pool: ConnectionPool | None
    # Cluster mode: the RedisCluster client for data operations
    _cluster_client: RedisCluster | None
    # Cluster mode: connection pool to a single node for pub/sub (cluster doesn't
    # support pub/sub natively, so we connect directly to one primary node)
    _node_pool: ConnectionPool | None
    # Memory mode: in-process BurnerRedis instance
    _memory_client: "BurnerRedis | None"
    _parsed: ParseResult
    _stack: AsyncExitStack

    def __init__(self, url: str) -> None:
        """Initialize a Redis connection manager.

        Args:
            url: Redis URL (redis://, rediss://, redis+cluster://, or memory://)
        """
        self.url = url
        self._parsed = urlparse(url)
        self._connection_pool = None
        self._cluster_client = None
        self._node_pool = None
        self._memory_client = None

    async def __aenter__(self) -> "RedisConnection":
        """Connect to Redis when entering the context."""
        assert not self.is_connected, "RedisConnection is not reentrant"

        self._stack = AsyncExitStack()
        await self._stack.__aenter__()

        if self.is_cluster:  # pragma: no cover
            self._cluster_client = await self._create_cluster_client()
            self._stack.callback(lambda: setattr(self, "_cluster_client", None))
            self._stack.push_async_callback(
                close_resource, self._cluster_client, "cluster client"
            )

            self._node_pool = self._create_node_pool()
            self._stack.callback(lambda: setattr(self, "_node_pool", None))
            self._stack.push_async_callback(
                close_resource, self._node_pool, "node pool"
            )
        elif self.is_memory:
            self._memory_client = await self._get_or_create_memory_client()
            self._stack.callback(lambda: setattr(self, "_memory_client", None))
        else:
            self._connection_pool = await self._connection_pool_from_url()
            self._stack.callback(lambda: setattr(self, "_connection_pool", None))
            self._stack.push_async_callback(
                close_resource, self._connection_pool, "connection pool"
            )

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the Redis connection when exiting the context."""
        try:
            await self._stack.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            del self._stack

    @property
    def is_connected(self) -> bool:
        """Check if the connection is established."""
        return (
            self._connection_pool is not None
            or self._cluster_client is not None
            or self._memory_client is not None
        )

    @property
    def is_cluster(self) -> bool:
        """Check if this connection is to a Redis Cluster."""
        return self._parsed.scheme in ("redis+cluster", "rediss+cluster")

    @property
    def is_memory(self) -> bool:
        """Check if this connection is to an in-memory backend."""
        return self._parsed.scheme == "memory"

    @property
    def cluster_client(self) -> RedisCluster | None:
        """Get the cluster client, if connected in cluster mode."""
        return self._cluster_client

    @property
    def memory_client(self) -> "BurnerRedis | None":
        """Get the memory client, if connected in memory mode."""
        return self._memory_client

    def prefix(self, name: str) -> str:
        """Return a prefix, hash-tagged for cluster mode key slot hashing.

        In Redis Cluster mode, keys with the same hash tag {name} are
        guaranteed to be on the same slot, which is required for multi-key
        operations.

        Args:
            name: The base name for the prefix

        Returns:
            "{name}" for cluster mode, or just "name" for standalone mode
        """
        if self.is_cluster:
            return f"{{{name}}}"
        return name

    def _normalized_url(self) -> str:
        """Convert a cluster URL to a standard Redis URL for redis-py.

        redis-py doesn't support the redis+cluster:// scheme, so we normalize
        it to redis:// (or rediss://) before passing to RedisCluster.from_url().

        Returns:
            The URL with +cluster removed from the scheme if cluster mode,
            otherwise the original URL
        """
        if not self.is_cluster:
            return self.url
        new_scheme = self._parsed.scheme.replace("+cluster", "")
        return urlunparse(self._parsed._replace(scheme=new_scheme))

    async def _create_cluster_client(self) -> RedisCluster:  # pragma: no cover
        """Create and initialize an async RedisCluster client.

        Returns:
            An initialized RedisCluster client ready for use
        """
        client: RedisCluster = RedisCluster.from_url(self._normalized_url())
        await client.initialize()
        return client

    def _create_node_pool(self) -> ConnectionPool:  # pragma: no cover
        """Create a connection pool to a cluster node for pub/sub operations.

        Redis Cluster doesn't natively support pub/sub through the cluster client,
        so we create a regular connection pool connected to one of the primary nodes.
        This pool persists for the lifetime of the RedisConnection.

        Returns:
            A ConnectionPool connected to a cluster primary node
        """
        assert self._cluster_client is not None
        nodes = self._cluster_client.get_primaries()
        if not nodes:
            raise RuntimeError("No primary nodes available in cluster")
        node = nodes[0]
        return ConnectionPool(
            host=node.host,
            port=int(node.port),
            username=self._parsed.username,
            password=self._parsed.password,
            connection_class=SSLConnection
            if self._parsed.scheme == "rediss+cluster"
            else Connection,
            decode_responses=False,
        )

    async def _connection_pool_from_url(
        self, decode_responses: bool = False
    ) -> ConnectionPool:
        """Create a Redis connection pool from the URL.

        This is only for real Redis connections (redis://, rediss://).
        Memory backend uses BurnerRedis directly, not connection pools.

        Args:
            decode_responses: If True, decode Redis responses from bytes to strings

        Returns:
            A ConnectionPool ready for use with Redis clients
        """
        return ConnectionPool.from_url(self.url, decode_responses=decode_responses)

    async def _get_or_create_memory_client(self) -> "BurnerRedis":
        """Get or create a BurnerRedis instance for a memory:// URL."""
        global _memory_servers

        from burner_redis import BurnerRedis

        # Fast path: instance already exists
        client = _memory_servers.get(self.url)
        if client is not None:
            return client

        async with _memory_servers_lock:
            client = _memory_servers.get(self.url)
            if client is not None:  # pragma: no cover
                return client

            client = BurnerRedis()
            _memory_servers[self.url] = client
            return client

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[RedisClient, None]:
        """Get a Redis client, handling standalone, cluster, and memory modes."""
        if self._cluster_client is not None:  # pragma: no cover
            yield self._cluster_client
        elif self._memory_client is not None:
            yield self._memory_client
        else:
            async with Redis(connection_pool=self._connection_pool) as r:
                yield r

    @asynccontextmanager
    async def pubsub(self) -> AsyncGenerator[PubSub | typing.Any, None]:
        """Get a pub/sub connection, handling standalone, cluster, and memory modes."""
        if self._cluster_client is not None:  # pragma: no cover
            async with self._cluster_pubsub() as ps:
                yield ps
        elif self._memory_client is not None:
            ps = self._memory_client.pubsub()
            try:
                yield ps
            finally:
                await ps.aclose()
        else:
            async with Redis(connection_pool=self._connection_pool) as r:
                async with r.pubsub() as pubsub:
                    yield pubsub

    async def publish(self, channel: str, message: str) -> int:
        """Publish a message to a pub/sub channel."""
        if self._cluster_client is not None:  # pragma: no cover
            async with Redis(connection_pool=self._node_pool) as r:
                return await r.publish(channel, message)
        elif self._memory_client is not None:
            return await self._memory_client.publish(channel, message)
        else:
            async with Redis(connection_pool=self._connection_pool) as r:
                return await r.publish(channel, message)

    @asynccontextmanager
    async def _cluster_pubsub(self) -> AsyncGenerator[PubSub, None]:  # pragma: no cover
        """Create a pub/sub connection using the shared node pool.

        Redis Cluster doesn't natively support pub/sub through the cluster client,
        so we use a regular Redis client connected to one of the primary nodes.
        The underlying connection pool is managed by the RedisConnection lifecycle.

        Yields:
            A PubSub object connected to a cluster node
        """
        client = Redis(connection_pool=self._node_pool)
        pubsub = client.pubsub()
        try:
            yield pubsub
        finally:
            try:
                await pubsub.aclose()
            except Exception:
                logger.warning("Failed to close cluster pubsub", exc_info=True)
            try:
                await client.aclose()
            except Exception:
                logger.warning("Failed to close cluster client", exc_info=True)
