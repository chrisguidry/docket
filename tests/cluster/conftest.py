"""Cluster test fixtures that override main conftest fixtures.

The main conftest.py has autouse fixtures that use Docker to spin up a standalone
Redis instance. For cluster tests, we need to skip those fixtures since we're
using an external Redis Cluster.
"""

import pytest

from docket._redis import clear_cluster_clients


# Clear cluster client cache before each test to avoid event loop conflicts
@pytest.fixture(autouse=True)
async def clear_cluster_cache():
    """Clear cached cluster clients before each test.

    RedisCluster clients are cached by URL, but pytest-asyncio creates
    new event loops for each test. This fixture ensures we get fresh
    connections for each test to avoid 'attached to a different loop' errors.
    """
    await clear_cluster_clients()
    yield
    await clear_cluster_clients()


# Override the autouse key_leak_checker to avoid conftest's redis_url dependency
@pytest.fixture(autouse=True)
async def key_leak_checker():
    """Disable key leak checker for cluster tests.

    Cluster tests don't need the key leak checker since they:
    1. Use an external cluster (not managed by the test fixtures)
    2. Have their own cleanup in fixtures
    """
    yield None


# Provide redis_url fixture that cluster tests won't use directly
# but may be needed to satisfy fixture dependencies
@pytest.fixture
def redis_url():
    """Dummy redis_url for cluster tests - not actually used."""
    return "memory://"


# Override redis_server to avoid Docker dependency
@pytest.fixture(scope="session")
def redis_server():
    """No-op redis_server for cluster tests."""
    yield None


# Override docket fixture to avoid redis_url dependency chain
@pytest.fixture
async def docket():
    """No-op docket fixture - cluster tests define their own."""
    pytest.skip("Use cluster-specific docket fixture")


# Override worker fixture
@pytest.fixture
async def worker():
    """No-op worker fixture - cluster tests define their own."""
    pytest.skip("Use cluster-specific worker fixture")
