"""Redis connection management.

This module is the single point of control for Redis connections, including
the fakeredis backend used for memory:// URLs and Redis Cluster support.
"""

import asyncio
import typing

from redis.asyncio import ConnectionPool

if typing.TYPE_CHECKING:
    from fakeredis.aioredis import FakeServer
    from redis.asyncio.cluster import RedisCluster

# Cache of FakeServer instances keyed by URL
_memory_servers: dict[str, "FakeServer"] = {}
_memory_servers_lock = asyncio.Lock()

# Cache of RedisCluster instances keyed by URL
_cluster_clients: dict[str, "RedisCluster"] = {}
_cluster_clients_lock = asyncio.Lock()


def is_cluster_url(url: str) -> bool:
    """Check if the URL indicates Redis Cluster mode.

    Args:
        url: Redis URL to check

    Returns:
        True if the URL uses the redis+cluster:// scheme
    """
    return url.startswith("redis+cluster://")


def normalize_cluster_url(url: str) -> str:
    """Convert a redis+cluster:// URL to a standard redis:// URL.

    Args:
        url: Redis cluster URL (redis+cluster://...)

    Returns:
        Normalized URL with redis:// scheme
    """
    if url.startswith("redis+cluster://"):
        return url.replace("redis+cluster://", "redis://", 1)
    return url


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


async def get_cluster_client(url: str) -> "RedisCluster":
    """Get or create a RedisCluster client for a cluster URL.

    RedisCluster manages its own connection pooling internally, so we cache
    the client instance to avoid creating multiple clients for the same cluster.

    Args:
        url: Redis cluster URL (redis+cluster://...)

    Returns:
        A RedisCluster client instance
    """
    global _cluster_clients

    from redis.asyncio.cluster import RedisCluster

    normalized_url = normalize_cluster_url(url)

    # Fast path: client already exists
    client = _cluster_clients.get(url)
    if client is not None:
        return client

    async with _cluster_clients_lock:
        # Double-check after acquiring lock
        client = _cluster_clients.get(url)
        if client is not None:  # pragma: no cover
            return client

        # Create new cluster client and initialize it
        client = RedisCluster.from_url(normalized_url)
        await client.initialize()
        _cluster_clients[url] = client
        return client


async def close_cluster_client(url: str) -> None:
    """Close and remove a cached RedisCluster client.

    Args:
        url: Redis cluster URL whose client should be closed
    """
    async with _cluster_clients_lock:
        client = _cluster_clients.pop(url, None)
        if client is not None:
            await client.aclose()


async def clear_cluster_clients() -> None:
    """Close and remove all cached RedisCluster clients.

    This is primarily for testing to ensure isolation between tests
    that may run in different event loops.
    """
    async with _cluster_clients_lock:
        for url in list(_cluster_clients.keys()):
            client = _cluster_clients.pop(url, None)
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass  # Ignore errors during cleanup


async def connection_pool_from_url(url: str) -> ConnectionPool:
    """Create a Redis connection pool from a URL.

    Handles real Redis (redis://), in-memory fakeredis (memory://), and
    Redis Cluster (redis+cluster://). This is the only place in the codebase
    that imports fakeredis.

    Note: For Redis Cluster URLs, this returns a ConnectionPool but the actual
    cluster client should be obtained via get_cluster_client() since RedisCluster
    manages connections differently.

    Args:
        url: Redis URL (redis://...), memory:// for in-memory backend,
             or redis+cluster:// for Redis Cluster

    Returns:
        A ConnectionPool ready for use with Redis clients
    """
    if url.startswith("memory://"):
        return await _memory_connection_pool(url)
    if is_cluster_url(url):
        # For cluster mode, return a pool from the normalized URL
        # The actual cluster client is obtained separately via get_cluster_client()
        return ConnectionPool.from_url(normalize_cluster_url(url))
    return ConnectionPool.from_url(url)


async def _memory_connection_pool(url: str) -> ConnectionPool:
    """Create a connection pool for a memory:// URL using fakeredis."""
    global _memory_servers

    from fakeredis.aioredis import FakeConnection, FakeServer

    # Apply Lua runtime patch on first use
    _patch_fakeredis_lua_runtime()

    # Fast path: server already exists
    server = _memory_servers.get(url)
    if server is not None:
        return ConnectionPool(connection_class=FakeConnection, server=server)

    async with _memory_servers_lock:
        server = _memory_servers.get(url)
        if server is not None:  # pragma: no cover
            return ConnectionPool(connection_class=FakeConnection, server=server)

        server = FakeServer()
        _memory_servers[url] = server
        return ConnectionPool(connection_class=FakeConnection, server=server)


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

        # Cache LuaRuntime and set_globals function on the server
        if not hasattr(self._server, "_lua_runtime"):
            self._server._lua_runtime = LUA_MODULE.LuaRuntime(
                encoding=None, unpack_returned_tuples=True
            )
            modules_import_str = "\n".join(
                [f"{module} = require('{module}')" for module in self.load_lua_modules]
            )
            self._server._lua_set_globals = self._server._lua_runtime.eval(
                f"""
                function(keys, argv, redis_call, redis_pcall, redis_log, cjson_encode, cjson_decode, cjson_null)
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

                    KEYS = keys
                    ARGV = argv
                    {modules_import_str}
                end
                """
            )
            # Capture expected globals once after first setup
            self._server._lua_set_globals(
                self._server._lua_runtime.table_from([]),
                self._server._lua_runtime.table_from([]),
                lambda *args: None,
                lambda *args: None,
                lambda *args: None,
                lambda *args: None,
                lambda *args: None,
                None,
            )
            self._server._lua_expected_globals = set(
                self._server._lua_runtime.globals().keys()
            )

        lua_runtime = self._server._lua_runtime
        set_globals = self._server._lua_set_globals
        expected_globals = self._server._lua_expected_globals

        set_globals(
            lua_runtime.table_from(keys_and_args[:numkeys]),
            lua_runtime.table_from(keys_and_args[numkeys:]),
            functools.partial(self._lua_redis_call, lua_runtime, expected_globals),
            functools.partial(self._lua_redis_pcall, lua_runtime, expected_globals),
            functools.partial(_lua_redis_log, lua_runtime, expected_globals),
            functools.partial(_lua_cjson_encode, lua_runtime, expected_globals),
            functools.partial(_lua_cjson_decode, lua_runtime, expected_globals),
            _lua_cjson_null,
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

        return self._convert_lua_result(result, nested=False)

    ScriptingCommandsMixin.eval = patched_eval
