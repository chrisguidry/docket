"""The in-process backend behind docket's ``memory://`` URLs.

This module is the single point of BurnerRedis-awareness: the cache of running
servers, the loop affinity that cache has to respect, and the lifecycle of the
clients in it.  ``_redis.py`` only detects the scheme and dispatches here.
"""

from __future__ import annotations

import asyncio
import importlib
from threading import Lock as _ThreadLock
from typing import Callable, cast

from ._redis import MemoryRedisClient, close_resource

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


async def get_or_create_memory_client(url: str) -> MemoryRedisClient:
    """Get or create a BurnerRedis instance for a memory:// URL."""
    global _memory_servers

    client_factory = _memory_client_factory()
    loop = asyncio.get_running_loop()
    key = _memory_server_key(url, loop)

    await _drop_closed_memory_servers()
    with _memory_servers_lock:
        entry = _memory_servers.get(key)
        if entry is not None:
            return entry[1]
        client = client_factory()
        _memory_servers[key] = (loop, client)
        return client


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
