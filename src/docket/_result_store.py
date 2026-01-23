"""Result storage for task execution results.

This module provides:
- ClusterKeyValueStore: A RedisCluster-compatible key-value store
- ResultStorage: An async context manager that wraps either ClusterKeyValueStore
  or py-key-value's RedisStore, managing connection pool lifecycle internally
"""

import json
import logging
from collections.abc import Mapping, Sequence
from contextlib import AsyncExitStack
from types import TracebackType
from typing import TYPE_CHECKING, Any, SupportsFloat

from typing_extensions import Self

from key_value.aio.stores.redis import RedisStore
from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.cluster import RedisCluster

if TYPE_CHECKING:
    from docket._redis import RedisConnection

from docket._redis import close_resource

logger: logging.Logger = logging.getLogger(__name__)


class ClusterKeyValueStore:
    """A key-value store that works with RedisCluster.

    This store implements the AsyncKeyValue protocol from py-key-value,
    storing JSON-serialized data in Redis keys. It's designed to work
    with RedisCluster clients where py-key-value's RedisStore doesn't.

    The store uses a simple key structure: {collection}:{key}
    """

    def __init__(
        self,
        client: RedisCluster,
        *,
        default_collection: str = "default",
    ) -> None:
        """Initialize the cluster key-value store.

        Args:
            client: A RedisCluster client to use for storage
            default_collection: The default collection name for keys
        """
        self._client = client
        self._default_collection = default_collection

    def _make_key(self, key: str, collection: str | None) -> str:
        """Build the full Redis key from collection and key."""
        coll = collection if collection is not None else self._default_collection
        return f"{coll}:{key}"

    async def setup(self) -> None:
        """Initialize the store. No-op for cluster store."""
        pass

    async def get(
        self,
        key: str,
        *,
        collection: str | None = None,
    ) -> dict[str, Any] | None:
        """Retrieve a value by key from the specified collection."""
        redis_key = self._make_key(key, collection)
        data = await self._client.get(redis_key)
        if data is None:
            return None
        return json.loads(data)  # type: ignore[no-any-return]

    async def ttl(
        self,
        key: str,
        *,
        collection: str | None = None,
    ) -> tuple[dict[str, Any] | None, float | None]:
        """Retrieve the value and TTL for a key."""
        redis_key = self._make_key(key, collection)
        data = await self._client.get(redis_key)
        if data is None:
            return None, None
        ttl_val = await self._client.ttl(redis_key)
        # Redis returns -1 for no TTL, -2 for key doesn't exist
        ttl_float: float | None = float(ttl_val) if ttl_val >= 0 else None
        return json.loads(data), ttl_float

    async def put(
        self,
        key: str,
        value: Mapping[str, Any],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        """Store a key-value pair with optional TTL."""
        redis_key = self._make_key(key, collection)
        data = json.dumps(value)
        if ttl is not None:
            ttl_seconds = int(float(ttl))
            await self._client.setex(redis_key, ttl_seconds, data)
        else:
            await self._client.set(redis_key, data)

    async def delete(
        self,
        key: str,
        *,
        collection: str | None = None,
    ) -> bool:
        """Delete a key-value pair."""
        redis_key = self._make_key(key, collection)
        result = await self._client.delete(redis_key)
        return result > 0

    async def get_many(
        self,
        keys: Sequence[str],
        *,
        collection: str | None = None,
    ) -> list[dict[str, Any] | None]:
        """Retrieve multiple values by key."""
        if not keys:
            return []
        redis_keys = [self._make_key(k, collection) for k in keys]
        values = await self._client.mget(redis_keys)
        return [json.loads(v) if v is not None else None for v in values]

    async def ttl_many(
        self,
        keys: Sequence[str],
        *,
        collection: str | None = None,
    ) -> list[tuple[dict[str, Any] | None, float | None]]:
        """Retrieve multiple values and their TTLs."""
        if not keys:
            return []
        results: list[tuple[dict[str, Any] | None, float | None]] = []
        for key in keys:
            val, ttl_val = await self.ttl(key, collection=collection)
            results.append((val, ttl_val))
        return results

    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        """Store multiple key-value pairs."""
        if not keys:
            return
        for key, value in zip(keys, values, strict=True):
            await self.put(key, value, collection=collection, ttl=ttl)

    async def delete_many(
        self,
        keys: Sequence[str],
        *,
        collection: str | None = None,
    ) -> int:
        """Delete multiple key-value pairs."""
        if not keys:
            return 0
        redis_keys = [self._make_key(k, collection) for k in keys]
        return await self._client.delete(*redis_keys)


class ResultStorage:
    """Result storage that implements AsyncKeyValue using a RedisConnection.

    This class wraps either a ClusterKeyValueStore (for cluster mode) or a RedisStore
    (for standalone mode). It creates its own connection pool for standalone mode
    (with decode_responses=True) using the RedisConnection's URL.
    """

    _store: RedisStore | ClusterKeyValueStore
    _pool: ConnectionPool
    _client: Redis
    _stack: AsyncExitStack

    def __init__(
        self,
        redis: "RedisConnection",
        default_collection: str,
    ) -> None:
        self._redis = redis
        self._default_collection = default_collection

    async def __aenter__(self) -> Self:
        self._stack = AsyncExitStack()
        await self._stack.__aenter__()

        if self._redis.is_cluster:  # pragma: no cover
            if self._redis.cluster_client is None:
                raise ValueError("RedisConnection not connected in cluster mode")
            self._store = ClusterKeyValueStore(
                client=self._redis.cluster_client,
                default_collection=self._default_collection,
            )
        else:
            # Create a separate pool with decode_responses=True for result storage
            self._pool = await self._redis._connection_pool_from_url(
                decode_responses=True
            )
            self._stack.callback(lambda: delattr(self, "_pool"))
            self._stack.push_async_callback(close_resource, self._pool, "pool")

            self._client = Redis(connection_pool=self._pool)
            self._stack.callback(lambda: delattr(self, "_client"))
            self._stack.push_async_callback(close_resource, self._client, "client")

            self._store = RedisStore(
                client=self._client, default_collection=self._default_collection
            )

        await self._store.setup()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            await self._stack.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            del self._stack

    # AsyncKeyValue protocol - delegate to self._store

    async def setup(self) -> None:
        """Initialize the store. Already done in __aenter__."""
        pass

    async def get(
        self,
        key: str,
        *,
        collection: str | None = None,
    ) -> dict[str, Any] | None:
        return await self._store.get(key, collection=collection)

    async def ttl(
        self,
        key: str,
        *,
        collection: str | None = None,
    ) -> tuple[dict[str, Any] | None, float | None]:
        return await self._store.ttl(key, collection=collection)

    async def put(
        self,
        key: str,
        value: Mapping[str, Any],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        await self._store.put(key, value, collection=collection, ttl=ttl)

    async def delete(
        self,
        key: str,
        *,
        collection: str | None = None,
    ) -> bool:
        return await self._store.delete(key, collection=collection)

    async def get_many(
        self,
        keys: Sequence[str],
        *,
        collection: str | None = None,
    ) -> list[dict[str, Any] | None]:
        return await self._store.get_many(keys, collection=collection)

    async def ttl_many(
        self,
        keys: Sequence[str],
        *,
        collection: str | None = None,
    ) -> list[tuple[dict[str, Any] | None, float | None]]:
        return await self._store.ttl_many(keys, collection=collection)

    async def put_many(
        self,
        keys: Sequence[str],
        values: Sequence[Mapping[str, Any]],
        *,
        collection: str | None = None,
        ttl: SupportsFloat | None = None,
    ) -> None:
        await self._store.put_many(keys, values, collection=collection, ttl=ttl)

    async def delete_many(
        self,
        keys: Sequence[str],
        *,
        collection: str | None = None,
    ) -> int:
        return await self._store.delete_many(keys, collection=collection)
