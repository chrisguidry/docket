"""Tests for Redis helper functions."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.asyncio import ConnectionPool

from docket import Docket
from docket._redis import (
    cleanup_connection,
    clear_cluster_clients,
    close_cluster_client,
    connection_pool_from_url,
    get_cluster_client,
    get_connection_kwargs,
    is_cluster_url,
    normalize_cluster_url,
)


class TestClusterURLParsing:
    """Tests for cluster URL detection and parsing.

    These tests don't require a cluster - they test pure string operations.
    """

    def test_is_cluster_url_positive(self) -> None:
        """redis+cluster:// URLs should be detected as cluster URLs."""
        assert is_cluster_url("redis+cluster://localhost:7001")
        assert is_cluster_url("redis+cluster://localhost:7001,localhost:7002")
        assert is_cluster_url("redis+cluster://user:pass@localhost:7001")

    def test_is_cluster_url_negative(self) -> None:
        """Regular redis:// URLs should not be detected as cluster URLs."""
        assert not is_cluster_url("redis://localhost:6379")
        assert not is_cluster_url("memory://")
        assert not is_cluster_url("rediss://localhost:6379")

    def test_normalize_cluster_url(self) -> None:
        """Cluster URLs should be normalized to redis:// for redis-py."""
        assert (
            normalize_cluster_url("redis+cluster://localhost:7001")
            == "redis://localhost:7001"
        )
        assert (
            normalize_cluster_url("redis+cluster://localhost:7001,localhost:7002")
            == "redis://localhost:7001,localhost:7002"
        )

    def test_normalize_non_cluster_url(self) -> None:
        """Non-cluster URLs should be returned unchanged."""
        assert (
            normalize_cluster_url("redis://localhost:6379") == "redis://localhost:6379"
        )
        assert normalize_cluster_url("memory://") == "memory://"


class TestGetConnectionKwargs:
    """Tests for extracting connection kwargs from URLs."""

    def test_url_with_auth(self) -> None:
        """URL with credentials extracts username and password."""
        assert get_connection_kwargs("redis://localhost:6379") == {}
        assert get_connection_kwargs("redis://user:pass@localhost:6379") == {
            "username": "user",
            "password": "pass",
        }

    def test_ssl_from_scheme_and_query_param(self) -> None:
        """SSL enabled via rediss:// scheme or ssl query param."""
        assert get_connection_kwargs("rediss://localhost:6379") == {"ssl": True}
        assert get_connection_kwargs("redis://localhost:6379?ssl=true") == {"ssl": True}
        assert "ssl" not in get_connection_kwargs("redis://localhost:6379?ssl=false")


class TestPrefix:
    """Tests for Docket prefix behavior."""

    @pytest.mark.asyncio
    async def test_prefix_standalone(self) -> None:
        """For standalone Redis, prefix returns plain name (backward compatible)."""
        async with Docket(name="my-docket", url="memory://") as docket:
            assert docket.prefix == "my-docket"
            assert docket.queue_key == "my-docket:queue"
            assert docket.stream_key == "my-docket:stream"

    def test_prefix_before_connect(self) -> None:
        """Before connecting, prefix returns plain name."""
        docket = Docket(name="my-docket", url="memory://")
        # Before __aenter__, _cluster_client is None, so no braces
        assert docket.prefix == "my-docket"

    def test_prefix_cluster_mode(self) -> None:
        """When using cluster URL, prefix returns braced format."""
        docket = Docket(name="my-docket", url="redis+cluster://localhost:7001")
        # Prefix is determined by URL scheme, not _cluster_client
        assert docket.prefix == "{my-docket}"
        assert docket.queue_key == "{my-docket}:queue"
        assert docket.stream_key == "{my-docket}:stream"

    def test_key_method(self) -> None:
        """key() method builds keys with the prefix."""
        docket = Docket(name="my-docket", url="memory://")
        assert docket.key("queue") == "my-docket:queue"
        assert docket.key("runs:task-123") == "my-docket:runs:task-123"

        # With cluster URL
        cluster_docket = Docket(name="my-docket", url="redis+cluster://localhost:7001")
        assert cluster_docket.key("queue") == "{my-docket}:queue"

    def test_results_collection(self) -> None:
        """results_collection property returns the results key prefix."""
        docket = Docket(name="my-docket", url="memory://")
        assert docket.results_collection == "my-docket:results"

        # With cluster URL, should use braced prefix
        cluster_docket = Docket(name="my-docket", url="redis+cluster://localhost:7001")
        assert cluster_docket.results_collection == "{my-docket}:results"


class TestClusterClientManagement:
    """Tests for cluster client caching and lifecycle."""

    @pytest.mark.asyncio
    async def test_get_cluster_client_creates_and_caches(self) -> None:
        """get_cluster_client creates a client and caches it."""
        mock_client = AsyncMock()
        mock_client.initialize = AsyncMock()

        with patch(
            "redis.asyncio.cluster.RedisCluster.from_url", return_value=mock_client
        ) as mock_from_url:
            # First call creates client
            client1 = await get_cluster_client("redis+cluster://localhost:7001")
            assert client1 is mock_client
            mock_from_url.assert_called_once_with("redis://localhost:7001")
            mock_client.initialize.assert_called_once()

            # Second call returns cached client
            client2 = await get_cluster_client("redis+cluster://localhost:7001")
            assert client2 is client1
            # from_url not called again
            assert mock_from_url.call_count == 1

        # Clean up
        await clear_cluster_clients()

    @pytest.mark.asyncio
    async def test_close_cluster_client(self) -> None:
        """close_cluster_client closes and removes cached client."""
        mock_client = AsyncMock()
        mock_client.initialize = AsyncMock()
        mock_client.aclose = AsyncMock()

        with patch(
            "redis.asyncio.cluster.RedisCluster.from_url", return_value=mock_client
        ):
            # Create client
            await get_cluster_client("redis+cluster://localhost:7002")

            # Close it
            await close_cluster_client("redis+cluster://localhost:7002")
            mock_client.aclose.assert_called_once()

            # Closing again should be a no-op (client already removed)
            await close_cluster_client("redis+cluster://localhost:7002")
            assert mock_client.aclose.call_count == 1

    @pytest.mark.asyncio
    async def test_clear_cluster_clients(self) -> None:
        """clear_cluster_clients closes all cached clients."""
        mock_client1 = AsyncMock()
        mock_client1.initialize = AsyncMock()
        mock_client1.aclose = AsyncMock()

        mock_client2 = AsyncMock()
        mock_client2.initialize = AsyncMock()
        mock_client2.aclose = AsyncMock()

        clients = [mock_client1, mock_client2]

        with patch(
            "redis.asyncio.cluster.RedisCluster.from_url",
            side_effect=lambda url: clients.pop(0),  # type: ignore[reportUnknownLambdaType]
        ):
            await get_cluster_client("redis+cluster://localhost:7003")
            await get_cluster_client("redis+cluster://localhost:7004")

            await clear_cluster_clients()

            mock_client1.aclose.assert_called_once()
            mock_client2.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_clear_cluster_clients_ignores_close_errors(self) -> None:
        """clear_cluster_clients handles errors during close gracefully."""
        mock_client = AsyncMock()
        mock_client.initialize = AsyncMock()
        mock_client.aclose = AsyncMock(side_effect=Exception("close failed"))

        with patch(
            "redis.asyncio.cluster.RedisCluster.from_url", return_value=mock_client
        ):
            await get_cluster_client("redis+cluster://localhost:7005")
            # Should not raise even though aclose fails
            await clear_cluster_clients()

    @pytest.mark.asyncio
    async def test_connection_pool_from_url_cluster(self) -> None:
        """connection_pool_from_url handles cluster URLs."""
        pool = await connection_pool_from_url("redis+cluster://localhost:7006")
        # Returns a ConnectionPool for the normalized URL
        assert pool is not None
        await pool.disconnect()

    @pytest.mark.asyncio
    async def test_connection_pool_from_url_cluster_multi_host(self) -> None:
        """connection_pool_from_url handles multi-host cluster URLs."""
        # Multi-host URLs like redis+cluster://h1:7001,h2:7002 should work
        # by extracting just the first host for the pool
        pool = await connection_pool_from_url(
            "redis+cluster://host1:7001,host2:7002,host3:7003"
        )
        assert pool is not None
        await pool.disconnect()

    @pytest.mark.asyncio
    async def test_cleanup_connection_cluster_mode(self) -> None:
        """cleanup_connection calls close_cluster_client for cluster URLs."""
        mock_client = AsyncMock()
        mock_client.initialize = AsyncMock()
        mock_client.aclose = AsyncMock()

        with patch(
            "redis.asyncio.cluster.RedisCluster.from_url", return_value=mock_client
        ):
            # Create client first
            await get_cluster_client("redis+cluster://localhost:7007")

            # cleanup_connection should close it
            await cleanup_connection("redis+cluster://localhost:7007")
            mock_client.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_connection_standalone_mode(self) -> None:
        """cleanup_connection is a no-op for standalone URLs."""
        # Should not raise and should not do anything
        await cleanup_connection("redis://localhost:6379")
        await cleanup_connection("memory://test")


class TestDocketClusterMode:
    """Tests for Docket behavior in cluster mode using mocks.

    These tests verify cluster-specific code paths without running a real cluster.
    """

    def test_docket_cluster_result_storage_deferred(self) -> None:
        """Docket defers result storage creation in cluster mode.

        For cluster URLs, result storage is created in __aenter__ when we have
        the cluster client available. Before initialization, _result_storage is None.
        """
        docket = Docket(name="cluster-test", url="redis+cluster://localhost:7001")
        # Before __aenter__, _result_storage should be None (deferred initialization)
        assert docket._result_storage is None  # type: ignore[reportPrivateUsage]

    @pytest.mark.asyncio
    async def test_docket_redis_context_cluster_mode(self) -> None:
        """Docket.redis() yields cluster client when in cluster mode."""
        mock_cluster = AsyncMock()
        memory_pool = await connection_pool_from_url("memory://")

        with patch(
            "docket._redis.get_cluster_client",
            return_value=mock_cluster,
        ):
            docket = Docket(name="test", url="redis+cluster://localhost:7001")
            docket._connection_pool = memory_pool  # type: ignore[reportPrivateUsage]

            async with docket.redis() as redis:
                assert redis is mock_cluster

        await memory_pool.disconnect()

    @pytest.mark.asyncio
    async def test_docket_pubsub_cluster_mode(self) -> None:
        """Docket.pubsub() connects to primary node in cluster mode."""
        memory_pool = await connection_pool_from_url("memory://")

        # Set up mock cluster client with a primary node
        mock_node = MagicMock()
        mock_node.host = "127.0.0.1"
        mock_node.port = 7001

        mock_cluster = MagicMock()
        mock_cluster.get_primaries = MagicMock(return_value=[mock_node])

        # Mock the Redis client that will be created for pubsub
        mock_pubsub = AsyncMock()
        mock_pubsub.__aenter__ = AsyncMock(return_value=mock_pubsub)
        mock_pubsub.__aexit__ = AsyncMock(return_value=None)

        mock_redis = AsyncMock()
        mock_redis.__aenter__ = AsyncMock(return_value=mock_redis)
        mock_redis.__aexit__ = AsyncMock(return_value=None)
        mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

        with (
            patch("docket._redis.get_cluster_client", return_value=mock_cluster),
            patch("docket._redis.Redis", return_value=mock_redis),
        ):
            docket = Docket(name="test", url="redis+cluster://localhost:7001")
            docket._connection_pool = memory_pool  # type: ignore[reportPrivateUsage]

            async with docket._pubsub() as ps:  # type: ignore[reportUnknownVariableType]
                assert ps is mock_pubsub

        await memory_pool.disconnect()

    @pytest.mark.asyncio
    async def test_docket_pubsub_cluster_no_primaries(self) -> None:
        """Docket._pubsub() raises when no primary nodes available."""
        memory_pool = await connection_pool_from_url("memory://")

        # Set up mock cluster client with no primary nodes
        mock_cluster = MagicMock()
        mock_cluster.get_primaries = MagicMock(return_value=[])

        with patch("docket._redis.get_cluster_client", return_value=mock_cluster):
            docket = Docket(name="test", url="redis+cluster://localhost:7001")
            docket._connection_pool = memory_pool  # type: ignore[reportPrivateUsage]

            with pytest.raises(RuntimeError, match="No primary nodes"):
                async with docket._pubsub():  # type: ignore[reportPrivateUsage]
                    pass  # pragma: no cover - exception raised before reaching here

        await memory_pool.disconnect()

    @pytest.mark.asyncio
    async def test_docket_context_manager_cluster_init_and_cleanup(self) -> None:
        """Docket context manager initializes and cleans up cluster client.

        This test exercises the cluster-specific code paths in __aenter__ and
        __aexit__ without requiring a real Redis cluster.
        """
        mock_cluster = AsyncMock()
        mock_cluster.xread = AsyncMock(return_value=[])  # For StrikeList monitoring

        # Create a memory connection pool for StrikeList to use
        memory_pool = await connection_pool_from_url("memory://cluster-test")

        async def mock_get_cluster_client(url: str) -> AsyncMock:
            return mock_cluster

        async def mock_connection_pool(url: str) -> ConnectionPool:
            return memory_pool

        # Patch at various module levels where helpers are used
        with (
            patch(
                "docket._redis.get_cluster_client",
                side_effect=mock_get_cluster_client,
            ),
            patch(
                "docket._redis.connection_pool_from_url",
                side_effect=mock_connection_pool,
            ),
            patch(
                "docket.docket.cleanup_connection",
                new_callable=AsyncMock,
            ) as mock_cleanup_docket,
            patch(
                "docket.docket.connection_pool_from_url",
                side_effect=mock_connection_pool,
            ),
            patch(
                "docket.strikelist.cleanup_connection",
                new_callable=AsyncMock,
            ),
            patch(
                "docket.strikelist.connection_pool_from_url",
                side_effect=mock_connection_pool,
            ),
        ):
            # Use the actual Docket context manager with a cluster URL
            async with Docket(
                name="cluster-ctx-test", url="redis+cluster://localhost:7001"
            ) as docket:
                # Verify prefix is in cluster format
                assert docket.prefix == "{cluster-ctx-test}"

            # Verify cleanup_connection was called during __aexit__
            mock_cleanup_docket.assert_called_once_with(
                "redis+cluster://localhost:7001"
            )

        await memory_pool.disconnect()

    @pytest.mark.asyncio
    async def test_strikelist_send_instruction_cluster_mode(self) -> None:
        """StrikeList uses cluster client for send_instruction in cluster mode."""
        from docket.strikelist import Operator, Strike, StrikeList

        mock_cluster = AsyncMock()
        mock_cluster.xadd = AsyncMock()

        # Make xread block briefly to simulate normal behavior
        async def blocking_xread(*args: object, **kwargs: object) -> list[object]:
            await asyncio.sleep(0.01)
            return []

        mock_cluster.xread = AsyncMock(side_effect=blocking_xread)

        memory_pool = await connection_pool_from_url("memory://strikelist-test")

        async def mock_get_cluster_client(url: str) -> AsyncMock:
            return mock_cluster

        async def mock_connection_pool(url: str) -> ConnectionPool:
            return memory_pool

        with (
            patch(
                "docket._redis.get_cluster_client",
                side_effect=mock_get_cluster_client,
            ),
            patch(
                "docket._redis.connection_pool_from_url",
                side_effect=mock_connection_pool,
            ),
            patch(
                "docket.strikelist.cleanup_connection",
                new_callable=AsyncMock,
            ),
            patch(
                "docket.strikelist.connection_pool_from_url",
                side_effect=mock_connection_pool,
            ),
        ):
            strike_list = StrikeList(
                url="redis+cluster://localhost:7001", name="test-strikelist"
            )
            await strike_list.connect()

            # Wait for initial strikes to be loaded (monitor task polls once)
            await strike_list.wait_for_strikes_loaded()

            # Send a strike instruction - should use cluster client
            instruction = Strike("test_func", None, Operator.EQUAL, None)
            await strike_list.send_instruction(instruction)

            # Verify xadd was called on cluster client
            mock_cluster.xadd.assert_called_once()

            await strike_list.close()

        await memory_pool.disconnect()


class TestStrikeListLocalMode:
    """Tests for StrikeList local-only mode (no Redis connection)."""

    def test_prefix_without_url(self) -> None:
        """StrikeList.prefix returns plain name when URL is None."""
        from docket.strikelist import StrikeList

        strike_list = StrikeList(name="local-only")
        assert strike_list.prefix == "local-only"


class TestPublishMessage:
    """Tests for publish_message helper function."""

    @pytest.mark.asyncio
    async def test_publish_message_cluster_mode(self) -> None:
        """publish_message uses cluster client when URL is cluster URL."""
        from docket._redis import publish_message

        mock_cluster = AsyncMock()
        mock_cluster.execute_command = AsyncMock()

        memory_pool = await connection_pool_from_url("memory://")

        with patch("docket._redis.get_cluster_client", return_value=mock_cluster):
            await publish_message(
                url="redis+cluster://localhost:7001",
                channel="test-channel",
                message="test-message",
                pool=memory_pool,
            )

        mock_cluster.execute_command.assert_called_once_with(
            "PUBLISH", "test-channel", "test-message"
        )

        await memory_pool.disconnect()
