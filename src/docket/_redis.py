"""Redis connection management.

This module is the single point of control for Redis connections, including
the fakeredis backend used for memory:// URLs.

This module is designed to be the single point of cluster-awareness, so that
other modules can remain simple. When Redis Cluster support is added, only
this module will need to change.
"""

import asyncio
import logging
import typing
from contextlib import AsyncExitStack, asynccontextmanager
from types import TracebackType
from typing import AsyncGenerator, Protocol
from urllib.parse import ParseResult, urlparse, urlunparse

from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.client import PubSub
from redis.asyncio.cluster import RedisCluster
from redis.asyncio.connection import Connection, SSLConnection

if typing.TYPE_CHECKING:
    from fakeredis.aioredis import FakeServer


logger: logging.Logger = logging.getLogger(__name__)


class AsyncCloseable(Protocol):
    """Protocol for objects with an async aclose() method."""

    async def aclose(self) -> None: ...


async def close_resource(resource: AsyncCloseable, name: str) -> None:
    """Close a resource with error handling.

    Designed to be used with AsyncExitStack.push_async_callback().
    """
    try:
        await resource.aclose()
    except Exception:  # pragma: no cover
        logger.warning("Failed to close %s", name, exc_info=True)


# Cache of FakeServer instances keyed by URL
_memory_servers: dict[str, "FakeServer"] = {}
_memory_servers_lock = asyncio.Lock()


async def clear_memory_servers() -> None:
    """Clear all cached FakeServer instances.

    This is primarily for testing to ensure isolation between tests.
    """
    async with _memory_servers_lock:
        _memory_servers.clear()


def get_memory_server(url: str) -> "FakeServer | None":
    """Get the cached FakeServer for a URL, if any.

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
        return self._connection_pool is not None or self._cluster_client is not None

    @property
    def is_cluster(self) -> bool:
        """Check if this connection is to a Redis Cluster."""
        return self._parsed.scheme in ("redis+cluster", "rediss+cluster")

    @property
    def is_memory(self) -> bool:
        """Check if this connection is to an in-memory fakeredis backend."""
        return self._parsed.scheme == "memory"

    @property
    def cluster_client(self) -> RedisCluster | None:
        """Get the cluster client, if connected in cluster mode."""
        return self._cluster_client

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

        Handles real Redis (redis://) and in-memory fakeredis (memory://).

        Args:
            decode_responses: If True, decode Redis responses from bytes to strings

        Returns:
            A ConnectionPool ready for use with Redis clients
        """
        if self.is_memory:
            return await self._memory_connection_pool(decode_responses)
        return ConnectionPool.from_url(self.url, decode_responses=decode_responses)

    async def _memory_connection_pool(
        self, decode_responses: bool = False
    ) -> ConnectionPool:
        """Create a connection pool for a memory:// URL using fakeredis."""
        global _memory_servers

        from fakeredis.aioredis import FakeConnection, FakeServer

        # Apply Lua runtime patch on first use
        _patch_fakeredis_lua_runtime()

        # Fast path: server already exists
        server = _memory_servers.get(self.url)
        if server is not None:
            return ConnectionPool(
                connection_class=FakeConnection,
                server=server,
                decode_responses=decode_responses,
            )

        async with _memory_servers_lock:
            server = _memory_servers.get(self.url)
            if server is not None:  # pragma: no cover
                return ConnectionPool(
                    connection_class=FakeConnection,
                    server=server,
                    decode_responses=decode_responses,
                )

            server = FakeServer()
            _memory_servers[self.url] = server
            return ConnectionPool(
                connection_class=FakeConnection,
                server=server,
                decode_responses=decode_responses,
            )

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[Redis | RedisCluster, None]:
        """Get a Redis client, handling both standalone and cluster modes."""
        if self._cluster_client is not None:  # pragma: no cover
            yield self._cluster_client
        else:
            async with Redis(connection_pool=self._connection_pool) as r:
                yield r

    @asynccontextmanager
    async def pubsub(self) -> AsyncGenerator[PubSub, None]:
        """Get a pub/sub connection, handling both standalone and cluster modes."""
        if self._cluster_client is not None:  # pragma: no cover
            async with self._cluster_pubsub() as ps:
                yield ps
        else:
            async with Redis(connection_pool=self._connection_pool) as r:
                async with r.pubsub() as pubsub:
                    yield pubsub

    async def publish(self, channel: str, message: str) -> int:
        """Publish a message to a pub/sub channel."""
        if self._cluster_client is not None:  # pragma: no cover
            async with Redis(connection_pool=self._node_pool) as r:
                return await r.publish(channel, message)
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


# ------------------------------------------------------------------------------
# fakeredis Lua runtime memory leak workaround
#
# fakeredis creates a new lupa.LuaRuntime() for every EVAL/EVALSHA call, and
# these runtimes don't get garbage collected properly, causing unbounded memory
# growth. See: https://github.com/cunla/fakeredis-py/issues/446
#
# Until there's an upstream fix, we monkeypatch ScriptingCommandsMixin.eval to
# cache the LuaRuntime on the FakeServer instance and reuse it across calls.
# ------------------------------------------------------------------------------

_lua_patch_applied = False


def _patch_fakeredis_lua_runtime() -> None:  # pragma: no cover
    global _lua_patch_applied
    if _lua_patch_applied:
        return
    _lua_patch_applied = True

    import functools
    import hashlib

    from fakeredis import _msgs as msgs
    from fakeredis._commands import Int, command
    from fakeredis._helpers import SimpleError
    from fakeredis.commands_mixins.scripting_mixin import (
        ScriptingCommandsMixin,
        _check_for_lua_globals,
        _lua_cjson_decode,
        _lua_cjson_encode,
        _lua_cjson_null,
        _lua_redis_log,
    )

    # Import lupa module (fakeredis uses this dynamically)
    try:
        from fakeredis.commands_mixins.scripting_mixin import LUA_MODULE
    except ImportError:
        return  # lupa not installed, nothing to patch

    @command((bytes, Int), (bytes,), flags=msgs.FLAG_NO_SCRIPT)
    def patched_eval(
        self: ScriptingCommandsMixin,
        script: bytes,
        numkeys: int,
        *keys_and_args: bytes,
    ) -> typing.Any:
        if numkeys > len(keys_and_args):
            raise SimpleError(msgs.TOO_MANY_KEYS_MSG)
        if numkeys < 0:
            raise SimpleError(msgs.NEGATIVE_KEYS_MSG)

        sha1 = hashlib.sha1(script).hexdigest().encode()
        self._server.script_cache[sha1] = script

        # Cache LuaRuntime and all callbacks on the server
        if not hasattr(self._server, "_lua_runtime"):
            self._server._lua_runtime = LUA_MODULE.LuaRuntime(
                encoding=None, unpack_returned_tuples=True
            )
            lua_runtime = self._server._lua_runtime
            modules_import_str = "\n".join(
                [f"{module} = require('{module}')" for module in self.load_lua_modules]
            )

            # Create set_globals for initial setup (sets callbacks once)
            set_globals_init = lua_runtime.eval(
                f"""
                function(redis_call, redis_pcall, redis_log, cjson_encode, cjson_decode, cjson_null)
                    redis = {{}}
                    redis.call = redis_call
                    redis.pcall = redis_pcall
                    redis.log = redis_log
                    redis.LOG_DEBUG = 0
                    redis.LOG_VERBOSE = 1
                    redis.LOG_NOTICE = 2
                    redis.LOG_WARNING = 3
                    redis.error_reply = function(msg) return {{err=msg}} end
                    redis.status_reply = function(msg) return {{ok=msg}} end

                    cjson = {{}}
                    cjson.encode = cjson_encode
                    cjson.decode = cjson_decode
                    cjson.null = cjson_null

                    KEYS = {{}}
                    ARGV = {{}}
                    {modules_import_str}
                end
                """
            )

            # Create set_keys_argv to update just KEYS/ARGV per call
            self._server._lua_set_keys_argv = lua_runtime.eval(
                """
                function(keys, argv)
                    KEYS = keys
                    ARGV = argv
                end
                """
            )

            # Capture expected globals before setting up callbacks
            set_globals_init(
                lambda *args: None,
                lambda *args: None,
                lambda *args: None,
                lambda *args: None,
                lambda *args: None,
                None,
            )
            self._server._lua_expected_globals = set(lua_runtime.globals().keys())
            expected_globals = self._server._lua_expected_globals

            # Container to hold current socket - callbacks will look this up
            self._server._lua_current_socket = [None]  # Use list for mutability

            # Create wrapper callbacks that look up the current socket dynamically
            def make_redis_call_wrapper() -> typing.Callable[..., typing.Any]:
                server = self._server
                lr = lua_runtime
                eg = expected_globals

                def wrapper(op: bytes, *args: typing.Any) -> typing.Any:
                    socket = server._lua_current_socket[0]
                    return socket._lua_redis_call(lr, eg, op, *args)

                return wrapper

            def make_redis_pcall_wrapper() -> typing.Callable[..., typing.Any]:
                server = self._server
                lr = lua_runtime
                eg = expected_globals

                def wrapper(op: bytes, *args: typing.Any) -> typing.Any:
                    socket = server._lua_current_socket[0]
                    return socket._lua_redis_pcall(lr, eg, op, *args)

                return wrapper

            # Cache the callback wrappers and static partials
            self._server._lua_redis_call_wrapper = make_redis_call_wrapper()
            self._server._lua_redis_pcall_wrapper = make_redis_pcall_wrapper()
            self._server._lua_log_partial = functools.partial(
                _lua_redis_log, lua_runtime, expected_globals
            )
            self._server._lua_cjson_encode_partial = functools.partial(
                _lua_cjson_encode, lua_runtime, expected_globals
            )
            self._server._lua_cjson_decode_partial = functools.partial(
                _lua_cjson_decode, lua_runtime, expected_globals
            )

            # Set up all callbacks once
            set_globals_init(
                self._server._lua_redis_call_wrapper,
                self._server._lua_redis_pcall_wrapper,
                self._server._lua_log_partial,
                self._server._lua_cjson_encode_partial,
                self._server._lua_cjson_decode_partial,
                _lua_cjson_null,
            )

        lua_runtime = self._server._lua_runtime
        expected_globals = self._server._lua_expected_globals

        # Update the current socket so callbacks can find it
        self._server._lua_current_socket[0] = self

        # Only update KEYS and ARGV per call (callbacks are already set)
        self._server._lua_set_keys_argv(
            lua_runtime.table_from(keys_and_args[:numkeys]),
            lua_runtime.table_from(keys_and_args[numkeys:]),
        )

        try:
            result = lua_runtime.execute(script)
        except SimpleError as ex:
            if ex.value == msgs.LUA_COMMAND_ARG_MSG:
                if self.version < (7,):
                    raise SimpleError(msgs.LUA_COMMAND_ARG_MSG6)
                elif self._server.server_type == "valkey":
                    raise SimpleError(
                        msgs.VALKEY_LUA_COMMAND_ARG_MSG.format(sha1.decode())
                    )
                else:
                    raise SimpleError(msgs.LUA_COMMAND_ARG_MSG)
            if self.version < (7,):
                raise SimpleError(msgs.SCRIPT_ERROR_MSG.format(sha1.decode(), ex))
            raise SimpleError(ex.value)
        except LUA_MODULE.LuaError as ex:
            raise SimpleError(msgs.SCRIPT_ERROR_MSG.format(sha1.decode(), ex))

        _check_for_lua_globals(lua_runtime, expected_globals)

        # Clean up Lua tables (KEYS/ARGV) created for this script execution
        lua_runtime.execute("collectgarbage()")

        return self._convert_lua_result(result, nested=False)

    ScriptingCommandsMixin.eval = patched_eval
