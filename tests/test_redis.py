"""Tests for Redis helper functions."""

import pytest

from docket import Docket
from docket._redis import is_cluster_url, normalize_cluster_url


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


class TestHashTag:
    """Tests for Docket hash_tag behavior."""

    @pytest.mark.asyncio
    async def test_hash_tag_standalone(self) -> None:
        """For standalone Redis, hash_tag returns plain name (backward compatible)."""
        async with Docket(name="my-docket", url="memory://") as docket:
            assert docket.hash_tag == "my-docket"
            assert docket.queue_key == "my-docket:queue"
            assert docket.stream_key == "my-docket:stream"

    def test_hash_tag_before_connect(self) -> None:
        """Before connecting, hash_tag returns plain name."""
        docket = Docket(name="my-docket", url="memory://")
        # Before __aenter__, _cluster_client is None, so no braces
        assert docket.hash_tag == "my-docket"
