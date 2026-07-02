"""Redis connection management.

This module is the single point of control for Redis connections, including
the burner-redis backend used for memory:// URLs.

This module is designed to be the single point of cluster-awareness, so that
other modules can remain simple. When Redis Cluster support is added, only
this module will need to change.

Redis Sentinel support lives in the companion ``_redis_sentinel.py`` module;
this module only detects the ``redis+sentinel://`` scheme and dispatches there.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta
from threading import Lock as _ThreadLock
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Iterable,
    Literal,
    Mapping,
    Protocol,
    Sequence,
    TypeAlias,
    TypedDict,
    cast,
    overload,
    runtime_checkable,
)
from urllib.parse import ParseResult, urlparse, urlunparse

from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.client import PubSub
from redis.asyncio.cluster import RedisCluster
from redis.asyncio.connection import Connection, SSLConnection

logger: logging.Logger = logging.getLogger(__name__)


# Docket performs long and unbounded blocking reads: the 60s strike-stream
# xread and the indefinite execution state/progress pubsub.listen.  redis-py
# 8.0.0 changed the client socket_timeout default to 5s, which aborts those
# reads mid-flight with a TimeoutError.  We disable the client read timeout so
# blocking reads wait for the server as intended; redis-py's default-on TCP
# keepalive still detects genuinely dead peers.
BLOCKING_READ_SOCKET_TIMEOUT: float | None = None


# ---------------------------------------------------------------------------
# Input type aliases (mirror redis-py's type domain)
# ---------------------------------------------------------------------------

KeyT: TypeAlias = str | bytes | memoryview
EncodableT: TypeAlias = str | bytes | bytearray | memoryview | int | float
StreamIDT: TypeAlias = str | bytes | int
ExpiryT: TypeAlias = int | timedelta
AbsExpiryT: TypeAlias = int | datetime


# ---------------------------------------------------------------------------
# Stream return-shape aliases
#
# docket always uses decode_responses=False, so stream-related responses are
# fully bytes-typed.  These aliases consolidate the shapes that stream-reading
# methods (xrange, xread, xreadgroup, xclaim, xautoclaim) return so callers
# can annotate their receivers without restating the nesting each time.
# ---------------------------------------------------------------------------

RedisStreamID: TypeAlias = bytes
RedisMessageID: TypeAlias = bytes
RedisMessage: TypeAlias = dict[bytes, bytes]
RedisMessages: TypeAlias = Sequence[tuple[RedisMessageID, RedisMessage]]
RedisStream: TypeAlias = tuple[RedisStreamID, RedisMessages]
RedisReadGroupResponse: TypeAlias = Sequence[RedisStream]


class RedisStreamPendingMessage(TypedDict):
    """One entry returned by XPENDING ... IDLE/RANGE."""

    message_id: bytes
    consumer: bytes
    time_since_delivered: int
    times_delivered: int


# ---------------------------------------------------------------------------
# Companion protocols
# ---------------------------------------------------------------------------


class AsyncCloseable(Protocol):
    """Protocol for objects with an async aclose() method."""

    async def aclose(self) -> None: ...


class Pipeline(Protocol):
    """The subset of pipeline operations docket actually invokes inside
    ``async with redis.pipeline() as pipeline:`` blocks.

    Pipeline command methods are synchronous: they queue the command and
    return the pipeline itself for chaining.  Only ``execute()`` is awaited,
    and it returns the heterogeneous results of the queued commands in
    order.
    """

    def delete(self, *names: KeyT) -> "Pipeline": ...
    def eval(
        self, script: str | bytes, numkeys: int, *keys_and_args: EncodableT
    ) -> "Pipeline": ...
    def evalsha(
        self, sha: str | bytes, numkeys: int, *keys_and_args: EncodableT
    ) -> "Pipeline": ...
    def expire(self, name: KeyT, time: ExpiryT) -> "Pipeline": ...
    def hgetall(self, name: KeyT) -> "Pipeline": ...
    def hincrby(self, name: KeyT, key: KeyT, amount: int = 1) -> "Pipeline": ...
    def hset(
        self,
        name: KeyT,
        key: KeyT | None = None,
        value: EncodableT | None = None,
        mapping: Mapping[KeyT, EncodableT] | None = None,
    ) -> "Pipeline": ...
    def sadd(self, name: KeyT, *values: EncodableT) -> "Pipeline": ...
    def xack(self, name: KeyT, groupname: KeyT, *ids: StreamIDT) -> "Pipeline": ...
    def xdel(self, name: KeyT, *ids: StreamIDT) -> "Pipeline": ...
    def xlen(self, name: KeyT) -> "Pipeline": ...
    def xpending_range(
        self,
        name: KeyT,
        groupname: KeyT,
        min: StreamIDT,
        max: StreamIDT,
        count: int,
        consumername: KeyT | None = None,
        idle: int | None = None,
    ) -> "Pipeline": ...
    def xrange(
        self,
        name: KeyT,
        min: StreamIDT = "-",
        max: StreamIDT = "+",
        count: int | None = None,
    ) -> "Pipeline": ...
    def xtrim(
        self,
        name: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: StreamIDT | None = None,
        limit: int | None = None,
    ) -> "Pipeline": ...
    def zadd(
        self,
        name: KeyT,
        mapping: Mapping[EncodableT, float | int],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> "Pipeline": ...
    def zcard(self, name: KeyT) -> "Pipeline": ...
    def zcount(
        self,
        name: KeyT,
        min: float | str | bytes,
        max: float | str | bytes,
    ) -> "Pipeline": ...
    def zrange(
        self, name: KeyT, start: int, end: int, desc: bool = False
    ) -> "Pipeline": ...
    def zremrangebyscore(
        self,
        name: KeyT,
        min: float | str | bytes,
        max: float | str | bytes,
    ) -> "Pipeline": ...

    async def execute(self, raise_on_error: bool = True) -> list[Any]: ...

    async def __aenter__(self) -> "Pipeline": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None: ...


class Lock(Protocol):
    """A distributed lock acquired via ``async with redis.lock(name):``."""

    async def __aenter__(self) -> "Lock": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None: ...


@runtime_checkable
class PubSubClient(Protocol):
    """Protocol capturing the pub/sub interface that docket uses.

    This is the structural type shared by redis.asyncio.client.PubSub and
    burner_redis.pubsub.PubSub.
    """

    async def subscribe(self, *channels: KeyT) -> None: ...
    async def psubscribe(self, *patterns: KeyT) -> None: ...
    async def get_message(
        self,
        ignore_subscribe_messages: bool = False,
        timeout: float | None = 0.0,
    ) -> dict[str, Any] | None: ...
    def listen(self) -> AsyncIterator[dict[str, Any]]: ...
    async def aclose(self) -> None: ...


# ---------------------------------------------------------------------------
# RedisClient: the public surface advertised by docket.redis()
# ---------------------------------------------------------------------------


@runtime_checkable
class RedisClient(Protocol):
    """The Redis client surface docket uses and exposes via ``docket.redis()``.

    This is the structural type shared by ``redis.asyncio.Redis``,
    ``redis.asyncio.cluster.RedisCluster``, and ``burner_redis.BurnerRedis``.
    Signatures are async-only and committed to ``decode_responses=False``,
    so reads return ``bytes`` rather than the decoded ``str``.

    The protocol covers what docket itself calls plus the queue-pattern
    methods (the LPUSH/BRPOP family) that downstream consumers rely on.
    """

    # ----- Strings & generic key ops -----

    async def get(self, name: KeyT) -> bytes | None: ...
    async def set(
        self,
        name: KeyT,
        value: EncodableT,
        ex: ExpiryT | None = None,
        px: ExpiryT | None = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
        get: bool = False,
        exat: AbsExpiryT | None = None,
        pxat: AbsExpiryT | None = None,
    ) -> bool | bytes | None: ...
    async def setex(self, name: KeyT, time: ExpiryT, value: EncodableT) -> bool: ...
    async def mget(self, keys: Sequence[KeyT]) -> list[bytes | None]: ...
    async def exists(self, *names: KeyT) -> int: ...
    async def keys(self, pattern: KeyT = "*") -> list[bytes]: ...
    async def type(self, name: KeyT) -> bytes: ...
    async def ttl(self, name: KeyT) -> int: ...
    async def delete(self, *names: KeyT) -> int: ...
    async def expire(self, name: KeyT, time: ExpiryT) -> bool: ...

    # ----- Lists -----

    async def blmove(
        self,
        first_list: KeyT,
        second_list: KeyT,
        timeout: float,
        src: Literal["LEFT", "RIGHT"] = "LEFT",
        dest: Literal["LEFT", "RIGHT"] = "RIGHT",
    ) -> bytes | None: ...
    async def blpop(
        self,
        keys: KeyT | Iterable[KeyT],
        timeout: float | None = 0,
    ) -> tuple[bytes, bytes] | None: ...
    async def brpop(
        self,
        keys: KeyT | Iterable[KeyT],
        timeout: float | None = 0,
    ) -> tuple[bytes, bytes] | None: ...
    async def lindex(self, name: KeyT, index: int) -> bytes | None: ...
    async def linsert(
        self,
        name: KeyT,
        where: Literal["BEFORE", "AFTER"],
        refvalue: EncodableT,
        value: EncodableT,
    ) -> int: ...
    async def llen(self, name: KeyT) -> int: ...
    async def lmove(
        self,
        first_list: KeyT,
        second_list: KeyT,
        src: Literal["LEFT", "RIGHT"] = "LEFT",
        dest: Literal["LEFT", "RIGHT"] = "RIGHT",
    ) -> bytes | None: ...
    @overload
    async def lpop(self, name: KeyT) -> bytes | None: ...
    @overload
    async def lpop(self, name: KeyT, count: int) -> list[bytes] | None: ...
    async def lpush(self, name: KeyT, *values: EncodableT) -> int: ...
    async def lrange(self, name: KeyT, start: int, end: int) -> list[bytes]: ...
    async def lrem(self, name: KeyT, count: int, value: EncodableT) -> int: ...
    async def lset(self, name: KeyT, index: int, value: EncodableT) -> bool: ...
    async def ltrim(self, name: KeyT, start: int, end: int) -> bool: ...
    @overload
    async def rpop(self, name: KeyT) -> bytes | None: ...
    @overload
    async def rpop(self, name: KeyT, count: int) -> list[bytes] | None: ...
    async def rpoplpush(self, src: KeyT, dst: KeyT) -> bytes | None: ...
    async def rpush(self, name: KeyT, *values: EncodableT) -> int: ...

    # ----- Hashes -----

    async def hget(self, name: KeyT, key: KeyT) -> bytes | None: ...
    async def hgetall(self, name: KeyT) -> dict[bytes, bytes]: ...
    async def hdel(self, name: KeyT, *keys: KeyT) -> int: ...
    async def hincrby(self, name: KeyT, key: KeyT, amount: int = 1) -> int: ...
    async def hset(
        self,
        name: KeyT,
        key: KeyT | None = None,
        value: EncodableT | None = None,
        mapping: Mapping[KeyT, EncodableT] | None = None,
    ) -> int: ...

    # ----- Sets -----

    async def sadd(self, name: KeyT, *values: EncodableT) -> int: ...
    async def srem(self, name: KeyT, *values: EncodableT) -> int: ...
    async def smembers(self, name: KeyT) -> set[bytes]: ...
    async def scard(self, name: KeyT) -> int: ...

    # ----- Sorted sets -----

    async def zadd(
        self,
        name: KeyT,
        mapping: Mapping[EncodableT, float | int],
        nx: bool = False,
        xx: bool = False,
        ch: bool = False,
        incr: bool = False,
        gt: bool = False,
        lt: bool = False,
    ) -> int | float | None: ...
    async def zrem(self, name: KeyT, *values: EncodableT) -> int: ...
    async def zcard(self, name: KeyT) -> int: ...
    @overload
    async def zrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        desc: bool = False,
        *,
        withscores: Literal[True],
        score_cast_func: type[float] = float,
    ) -> list[tuple[bytes, float]]: ...
    @overload
    async def zrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        desc: bool = False,
        withscores: Literal[False] = False,
        score_cast_func: type[float] = float,
    ) -> list[bytes]: ...
    @overload
    async def zrangebyscore(
        self,
        name: KeyT,
        min: float | str | bytes,
        max: float | str | bytes,
        start: int | None = None,
        num: int | None = None,
        *,
        withscores: Literal[True],
        score_cast_func: type[float] = float,
    ) -> list[tuple[bytes, float]]: ...
    @overload
    async def zrangebyscore(
        self,
        name: KeyT,
        min: float | str | bytes,
        max: float | str | bytes,
        start: int | None = None,
        num: int | None = None,
        withscores: Literal[False] = False,
        score_cast_func: type[float] = float,
    ) -> list[bytes]: ...
    async def zremrangebyscore(
        self,
        name: KeyT,
        min: float | str | bytes,
        max: float | str | bytes,
    ) -> int: ...
    async def zscore(self, name: KeyT, value: EncodableT) -> float | None: ...

    # ----- Streams -----

    async def xadd(
        self,
        name: KeyT,
        fields: Mapping[KeyT, EncodableT],
        id: StreamIDT = "*",
        maxlen: int | None = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: StreamIDT | None = None,
        limit: int | None = None,
    ) -> bytes: ...
    async def xack(self, name: KeyT, groupname: KeyT, *ids: StreamIDT) -> int: ...
    async def xautoclaim(
        self,
        name: KeyT,
        groupname: KeyT,
        consumername: KeyT,
        min_idle_time: int,
        start_id: StreamIDT = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> tuple[bytes, RedisMessages, list[bytes]]: ...
    async def xclaim(
        self,
        name: KeyT,
        groupname: KeyT,
        consumername: KeyT,
        min_idle_time: int,
        message_ids: Sequence[StreamIDT],
        idle: int | None = None,
        time: int | None = None,
        retrycount: int | None = None,
        force: bool = False,
        justid: bool = False,
    ) -> RedisMessages: ...
    async def xread(
        self,
        streams: Mapping[KeyT, StreamIDT],
        count: int | None = None,
        block: int | None = None,
    ) -> RedisReadGroupResponse | None: ...
    async def xreadgroup(
        self,
        groupname: KeyT,
        consumername: KeyT,
        streams: Mapping[KeyT, StreamIDT],
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> RedisReadGroupResponse | None: ...
    async def xrange(
        self,
        name: KeyT,
        min: StreamIDT = "-",
        max: StreamIDT = "+",
        count: int | None = None,
    ) -> RedisMessages: ...
    async def xlen(self, name: KeyT) -> int: ...
    async def xtrim(
        self,
        name: KeyT,
        maxlen: int | None = None,
        approximate: bool = True,
        minid: StreamIDT | None = None,
        limit: int | None = None,
    ) -> int: ...
    async def xdel(self, name: KeyT, *ids: StreamIDT) -> int: ...
    async def xpending(self, name: KeyT, groupname: KeyT) -> dict[str, Any]: ...
    async def xpending_range(
        self,
        name: KeyT,
        groupname: KeyT,
        min: StreamIDT,
        max: StreamIDT,
        count: int,
        consumername: KeyT | None = None,
        idle: int | None = None,
    ) -> list[RedisStreamPendingMessage]: ...
    async def xinfo_groups(self, name: KeyT) -> list[dict[str, Any]]: ...
    async def xinfo_consumers(
        self, name: KeyT, groupname: KeyT
    ) -> list[dict[str, Any]]: ...
    async def xinfo_stream(self, name: KeyT) -> dict[str, Any]: ...
    async def xgroup_create(
        self,
        name: KeyT,
        groupname: KeyT,
        id: StreamIDT = "$",
        mkstream: bool = False,
        entries_read: int | None = None,
    ) -> bool: ...

    # ----- Server / pub-sub / scripting -----

    async def info(self, section: str | None = None) -> dict[str, Any]: ...
    async def publish(self, channel: KeyT, message: EncodableT) -> int: ...
    def pubsub(self, **kwargs: Any) -> PubSubClient: ...
    def scan_iter(
        self,
        match: KeyT | None = None,
        count: int | None = None,
        _type: str | None = None,
    ) -> AsyncIterator[bytes]: ...
    async def evalsha(
        self, sha: str, numkeys: int, *keys_and_args: KeyT | EncodableT
    ) -> Any: ...
    async def script_load(self, script: str | bytes) -> str: ...
    def pipeline(
        self,
        transaction: bool = True,
        shard_hint: str | None = None,
    ) -> Pipeline: ...
    def lock(
        self,
        name: KeyT,
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
    ) -> Lock: ...


class MemoryRedisClient(RedisClient, AsyncCloseable, Protocol):
    """Protocol for the in-process Redis client used by memory:// URLs."""


def is_cluster_client(redis: RedisClient) -> bool:
    """True when ``redis`` speaks to a Redis Cluster rather than a single server.

    Callers that only hold a client (not the :class:`RedisConnection` that
    created it, whose ``is_cluster`` answers this from the URL scheme) can
    use this to pick cluster-safe command strategies -- e.g. full-source
    ``EVAL`` instead of ``EVALSHA`` in pipelines, since a cluster node's
    script cache can't be relied upon across failover and resharding.
    """
    return isinstance(redis, RedisCluster)


async def close_resource(resource: AsyncCloseable, name: str) -> None:
    """Close a resource with error handling.

    Designed to be used with AsyncExitStack.push_async_callback().
    """
    try:
        await resource.aclose()
    except Exception:  # pragma: no cover
        logger.warning("Failed to close %s", name, exc_info=True)


# Cache of BurnerRedis instances keyed by URL and event loop.  BurnerRedis is
# loop-affine, so a memory:// URL may only reuse a client within the same loop.
_MemoryServerKey = tuple[str, int]
_MemoryServerEntry = tuple[asyncio.AbstractEventLoop, MemoryRedisClient]
_memory_servers: dict[_MemoryServerKey, _MemoryServerEntry] = {}
_memory_servers_lock = _ThreadLock()


def _memory_server_key(url: str, loop: asyncio.AbstractEventLoop) -> _MemoryServerKey:
    return url, id(loop)


async def _close_memory_clients(clients: list[MemoryRedisClient]) -> None:
    for client in clients:
        await close_resource(client, "memory client")


async def _drop_closed_memory_servers() -> None:
    clients: list[MemoryRedisClient] = []
    with _memory_servers_lock:
        for key, (loop, client) in list(_memory_servers.items()):
            if loop.is_closed():
                clients.append(client)
                del _memory_servers[key]

    await _close_memory_clients(clients)


def _memory_client_factory() -> Callable[[], MemoryRedisClient]:
    burner_redis = importlib.import_module("burner_redis")
    return cast(
        Callable[[], MemoryRedisClient],
        getattr(burner_redis, "BurnerRedis"),
    )


async def clear_memory_servers() -> None:
    """Discard cached BurnerRedis instances, closing all cached clients.

    Each BurnerRedis may hold internal state tied to the asyncio event loop
    that created it (pub/sub listeners, blocking-read notifiers, Tokio
    background tasks, etc.).  Clearing the cache first prevents new users from
    taking these instances while they are closing.
    """
    with _memory_servers_lock:
        clients = [client for _, client in _memory_servers.values()]
        _memory_servers.clear()

    await _close_memory_clients(clients)


def get_memory_server(url: str) -> MemoryRedisClient | None:
    """Get the cached BurnerRedis instance for a URL, if any.

    This is primarily for testing to verify server isolation.
    """
    loop = asyncio.get_running_loop()
    with _memory_servers_lock:
        entry = _memory_servers.get(_memory_server_key(url, loop))
    if entry is None:
        return None
    return entry[1]


class RedisConnection:
    """Manages Redis connections for standalone, Sentinel, and cluster modes.

    This class encapsulates the lifecycle management of Redis connections,
    hiding whether the underlying connection is to a standalone Redis server,
    a Sentinel-monitored master, or a Redis Cluster. It provides a unified
    interface for getting Redis clients, pub/sub connections, and publishing
    messages.

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
    _memory_client: MemoryRedisClient | None
    _parsed: ParseResult
    _stack: AsyncExitStack

    def __init__(self, url: str) -> None:
        """Initialize a Redis connection manager.

        Args:
            url: Redis URL (redis://, rediss://, redis+sentinel://,
                redis+cluster://, or memory://)
        """
        from ._redis_sentinel import is_sentinel_url, urlparse_multihost

        self.url = url
        # Sentinel URLs list several daemons in the netloc, which urlparse can't
        # handle when a bracketed IPv6 member follows another; carve those by
        # hand and leave standalone, cluster, and memory URLs to urlparse.
        self._parsed = (
            urlparse_multihost(url) if is_sentinel_url(url) else urlparse(url)
        )
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
    def is_sentinel(self) -> bool:
        """Check if this connection discovers its master through Redis Sentinel."""
        return self._parsed.scheme in ("redis+sentinel", "rediss+sentinel")

    @property
    def is_memory(self) -> bool:
        """Check if this connection is to an in-memory backend."""
        return self._parsed.scheme == "memory"

    @property
    def cluster_client(self) -> RedisCluster | None:
        """Get the cluster client, if connected in cluster mode."""
        return self._cluster_client

    @property
    def memory_client(self) -> MemoryRedisClient | None:
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
        client: RedisCluster = RedisCluster.from_url(
            self._normalized_url(), socket_timeout=BLOCKING_READ_SOCKET_TIMEOUT
        )
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
            socket_timeout=BLOCKING_READ_SOCKET_TIMEOUT,
        )

    async def _connection_pool_from_url(
        self, decode_responses: bool = False
    ) -> ConnectionPool:
        """Create a Redis connection pool from the URL.

        This is only for real Redis connections (redis://, rediss://, and the
        +sentinel variants).  Memory backend uses BurnerRedis directly, not
        connection pools.

        Args:
            decode_responses: If True, decode Redis responses from bytes to strings

        Returns:
            A ConnectionPool ready for use with Redis clients
        """
        if self.is_sentinel:
            from ._redis_sentinel import sentinel_connection_pool

            return sentinel_connection_pool(
                self.url,
                decode_responses=decode_responses,
                socket_timeout=BLOCKING_READ_SOCKET_TIMEOUT,
            )
        return ConnectionPool.from_url(  # pyright: ignore[reportUnknownMemberType]
            self.url,
            decode_responses=decode_responses,
            socket_timeout=BLOCKING_READ_SOCKET_TIMEOUT,
        )

    async def _get_or_create_memory_client(self) -> MemoryRedisClient:
        """Get or create a BurnerRedis instance for a memory:// URL."""
        global _memory_servers

        client_factory = _memory_client_factory()
        loop = asyncio.get_running_loop()
        key = _memory_server_key(self.url, loop)

        await _drop_closed_memory_servers()
        with _memory_servers_lock:
            entry = _memory_servers.get(key)
            if entry is not None:
                return entry[1]
            client = client_factory()
            _memory_servers[key] = (loop, client)
            return client

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[RedisClient, None]:
        """Get a Redis client, handling standalone, cluster, and memory modes.

        Casts at the redis-py boundary translate from redis-py's
        ``Awaitable[T] | T`` dual-mode signatures into our async-only protocol.
        At runtime the awaitables resolve correctly; the cast just bridges
        the static-type mismatch.
        """
        if self._cluster_client is not None:  # pragma: no cover
            yield cast(RedisClient, self._cluster_client)
        elif self._memory_client is not None:
            yield self._memory_client
        else:
            async with Redis(connection_pool=self._connection_pool) as r:
                yield cast(RedisClient, r)

    @asynccontextmanager
    async def pubsub(self) -> AsyncGenerator[PubSubClient, None]:
        """Get a pub/sub connection, handling standalone, cluster, and memory modes."""
        if self._cluster_client is not None:  # pragma: no cover
            async with self._cluster_pubsub() as ps:
                yield cast(PubSubClient, ps)
        elif self._memory_client is not None:
            ps = self._memory_client.pubsub()
            try:
                yield ps
            finally:
                await ps.aclose()
        else:
            async with Redis(connection_pool=self._connection_pool) as r:
                async with r.pubsub() as pubsub:  # pyright: ignore[reportUnknownMemberType]
                    yield cast(PubSubClient, pubsub)

    async def publish(self, channel: str, message: str) -> int:
        """Publish a message to a pub/sub channel."""
        if self._cluster_client is not None:  # pragma: no cover
            async with Redis(connection_pool=self._node_pool) as r:
                return cast(int, await r.publish(channel, message))  # pyright: ignore[reportUnknownMemberType]
        elif self._memory_client is not None:
            return await self._memory_client.publish(channel, message)
        else:
            async with Redis(connection_pool=self._connection_pool) as r:
                return cast(int, await r.publish(channel, message))  # pyright: ignore[reportUnknownMemberType]

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
        pubsub = client.pubsub()  # pyright: ignore[reportUnknownMemberType]
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
