"""Tests for task result storage and serialization."""

# pyright: reportPrivateUsage=false, reportAttributeAccessIssue=false, reportUnknownMemberType=false

from typing import TYPE_CHECKING, Any, Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from docket import Docket, Worker
from docket.execution import ExecutionState

if TYPE_CHECKING:
    from tests._key_leak_checker import KeyCountChecker


class CustomError(Exception):
    """Custom exception for testing."""

    def __init__(self, message: str, code: int):
        super().__init__(message, code)
        self.message = message
        self.code = code


async def test_result_storage_for_int_return(docket: Docket, worker: Worker):
    """Test that int results are stored and retrievable."""
    result_value = 42

    async def returns_int() -> int:
        return result_value

    docket.register(returns_int)
    execution = await docket.add(returns_int)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED

    # Retrieve result
    result = await execution.get_result()
    assert result == result_value


async def test_result_storage_for_str_return(docket: Docket, worker: Worker):
    """Test that string results are stored and retrievable."""
    result_value = "hello world"

    async def returns_str() -> str:
        return result_value

    docket.register(returns_str)
    execution = await docket.add(returns_str)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED

    # Retrieve result
    result = await execution.get_result()
    assert result == result_value


async def test_result_storage_for_dict_return(docket: Docket, worker: Worker):
    """Test that dict results are stored and retrievable."""
    result_value = {"key": "value", "number": 123}

    async def returns_dict() -> dict[str, Any]:
        return result_value

    docket.register(returns_dict)
    execution = await docket.add(returns_dict)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED

    # Retrieve result
    result = await execution.get_result()
    assert result == result_value


async def test_result_storage_for_object_return(docket: Docket, worker: Worker):
    """Test that object results are stored and retrievable."""

    class CustomObject:
        def __init__(self, value: int):
            self.value = value

        def __eq__(self, other: Any) -> bool:
            return isinstance(other, CustomObject) and self.value == other.value

    result_value = CustomObject(42)

    async def returns_object() -> CustomObject:
        return result_value

    docket.register(returns_object)
    execution = await docket.add(returns_object)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED

    # Retrieve result
    result = await execution.get_result()
    assert result == result_value


async def test_no_storage_for_none_annotated_task(docket: Docket, worker: Worker):
    """Test that tasks annotated with -> None don't store results."""

    async def returns_none_annotated() -> None:
        pass

    docket.register(returns_none_annotated)
    execution = await docket.add(returns_none_annotated)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED
    assert execution.result_key is None

    # get_result should return None
    result = await execution.get_result()
    assert result is None


async def test_no_storage_for_runtime_none(docket: Docket, worker: Worker):
    """Test that tasks returning None at runtime don't store results."""

    async def returns_none_runtime() -> int | None:
        return None

    docket.register(returns_none_runtime)
    execution = await docket.add(returns_none_runtime)()
    await worker.run_until_finished()

    # Verify execution completed
    await execution.sync()
    assert execution.state == ExecutionState.COMPLETED
    assert execution.result_key is None

    # get_result should return None
    result = await execution.get_result()
    assert result is None


async def test_exception_storage_and_retrieval(docket: Docket, worker: Worker):
    """Test that exceptions are stored and re-raised."""
    error_msg = "Test error"
    error_code = 500

    async def raises_error() -> int:
        raise CustomError(error_msg, error_code)

    docket.register(raises_error)
    execution = await docket.add(raises_error)()
    await worker.run_until_finished()

    # Verify execution failed
    await execution.sync()
    assert execution.state == ExecutionState.FAILED
    assert execution.result_key is not None

    # get_result should raise the stored exception
    with pytest.raises(CustomError) as exc_info:
        await execution.get_result()

    # Verify exception details are preserved
    assert exc_info.value.message == error_msg
    assert exc_info.value.code == error_code


async def test_result_key_stored_in_execution_record(docket: Docket, worker: Worker):
    """Test that result key is stored in execution record."""

    async def returns_value() -> int:
        return 123

    docket.register(returns_value)
    execution = await docket.add(returns_value)()
    await worker.run_until_finished()

    # Sync and check result field
    await execution.sync()
    assert execution.result_key == execution.key


async def test_result_storage_uses_provided_or_default(docket: Docket):
    """Test that result_storage uses appropriate store type by default."""
    from urllib.parse import urlparse

    from key_value.aio.stores.redis import RedisStore

    from docket._result_store import ClusterKeyValueStore, ResultStorage

    # docket.result_storage should be a ResultStorage wrapper
    assert isinstance(docket.result_storage, ResultStorage)

    # Check the internal store type
    store = docket.result_storage._store
    if docket._redis.is_cluster:  # pragma: no cover
        # Cluster mode uses ClusterKeyValueStore
        assert isinstance(store, ClusterKeyValueStore)
    else:  # pragma: no cover
        # Standalone mode uses RedisStore
        assert isinstance(store, RedisStore)

        # Verify it's connected to the same Redis
        result_client = store._client  # type: ignore[attr-defined]
        pool_kwargs: dict[str, Any] = result_client.connection_pool.connection_kwargs  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

        if docket.url.startswith("memory://"):  # pragma: no cover
            assert "server" in pool_kwargs
        else:
            parsed = urlparse(docket.url)
            assert pool_kwargs.get("host") == (parsed.hostname or "localhost")
            assert pool_kwargs.get("port") == (parsed.port or 6379)
            expected_db = (
                int(parsed.path.lstrip("/"))
                if parsed.path and parsed.path != "/"
                else 0
            )
            assert pool_kwargs.get("db") == expected_db


async def test_result_storage_uses_custom_when_provided(
    redis_url: str, make_docket_name: Callable[[], str]
):
    """Test that result_storage uses your store if provided."""
    from key_value.aio.protocols.key_value import AsyncKeyValue

    custom_storage = MagicMock(spec=AsyncKeyValue)
    custom_storage.setup = AsyncMock()
    async with Docket(
        name=make_docket_name(),
        url=redis_url,
        result_storage=custom_storage,
    ) as custom_docket:
        assert custom_docket.result_storage is custom_storage


async def test_result_storage_custom_without_setup(
    redis_url: str, make_docket_name: Callable[[], str]
):
    """Test that custom storage works without a setup method."""
    from key_value.aio.protocols.key_value import AsyncKeyValue

    # Create a mock without the setup method
    custom_storage = MagicMock(spec=AsyncKeyValue)
    del custom_storage.setup  # Remove the setup method from the mock
    assert not hasattr(custom_storage, "setup")

    async with Docket(
        name=make_docket_name(),
        url=redis_url,
        result_storage=custom_storage,
    ) as custom_docket:
        assert custom_docket.result_storage is custom_storage


# ClusterKeyValueStore-specific tests (only run in cluster mode)


async def test_cluster_store_get_nonexistent(docket: Docket):  # pragma: no cover
    """Test ClusterKeyValueStore.get returns None for missing keys."""
    from docket._result_store import ClusterKeyValueStore

    if not docket._redis.is_cluster:
        pytest.skip("Only runs in cluster mode")

    store = docket.result_storage
    assert isinstance(store._store, ClusterKeyValueStore)

    result = await store.get("nonexistent-key-test")
    assert result is None


async def test_cluster_store_ttl(  # pragma: no cover
    docket: Docket, key_leak_checker: "KeyCountChecker"
):
    """Test ClusterKeyValueStore.ttl method."""
    from docket._result_store import ClusterKeyValueStore

    if not docket._redis.is_cluster:
        pytest.skip("Only runs in cluster mode")

    store = docket.result_storage
    assert isinstance(store._store, ClusterKeyValueStore)

    # Exempt test keys from leak checker
    key_leak_checker.add_pattern_exemption(f"{docket.results_collection}:*")

    # Put with TTL
    await store.put("ttl-test", {"value": 123}, ttl=300)
    data, ttl_val = await store.ttl("ttl-test")
    assert data == {"value": 123}
    assert ttl_val is not None
    assert 0 < ttl_val <= 300

    # Put without TTL
    await store.put("no-ttl-test", {"value": 456})
    data, ttl_val = await store.ttl("no-ttl-test")
    assert data == {"value": 456}
    assert ttl_val is None

    # Nonexistent key
    data, ttl_val = await store.ttl("nonexistent-ttl-test")
    assert data is None
    assert ttl_val is None


async def test_cluster_store_delete(docket: Docket):  # pragma: no cover
    """Test ClusterKeyValueStore.delete method."""
    from docket._result_store import ClusterKeyValueStore

    if not docket._redis.is_cluster:
        pytest.skip("Only runs in cluster mode")

    store = docket.result_storage
    assert isinstance(store._store, ClusterKeyValueStore)

    await store.put("delete-test", {"value": 789})
    assert await store.get("delete-test") == {"value": 789}

    result = await store.delete("delete-test")
    assert result is True
    assert await store.get("delete-test") is None

    # Delete nonexistent
    result = await store.delete("already-deleted")
    assert result is False


async def test_cluster_store_get_many(  # pragma: no cover
    docket: Docket, key_leak_checker: "KeyCountChecker"
):
    """Test ClusterKeyValueStore.get_many method."""
    from docket._result_store import ClusterKeyValueStore

    if not docket._redis.is_cluster:
        pytest.skip("Only runs in cluster mode")

    store = docket.result_storage
    assert isinstance(store._store, ClusterKeyValueStore)

    # Exempt test keys from leak checker
    key_leak_checker.add_pattern_exemption(f"{docket.results_collection}:*")

    await store.put("many-1", {"a": 1})
    await store.put("many-2", {"b": 2})

    results = await store.get_many(["many-1", "many-2", "many-nonexistent"])
    assert results == [{"a": 1}, {"b": 2}, None]

    # Empty list
    results = await store.get_many([])
    assert results == []


async def test_cluster_store_ttl_many(  # pragma: no cover
    docket: Docket, key_leak_checker: "KeyCountChecker"
):
    """Test ClusterKeyValueStore.ttl_many method."""
    from docket._result_store import ClusterKeyValueStore

    if not docket._redis.is_cluster:
        pytest.skip("Only runs in cluster mode")

    store = docket.result_storage
    assert isinstance(store._store, ClusterKeyValueStore)

    # Exempt test keys from leak checker
    key_leak_checker.add_pattern_exemption(f"{docket.results_collection}:*")

    await store.put("ttl-many-1", {"a": 1}, ttl=300)
    await store.put("ttl-many-2", {"b": 2})  # No TTL initially

    results = await store.ttl_many(["ttl-many-1", "ttl-many-2", "nonexistent-ttl"])
    assert len(results) == 3
    assert results[0][0] == {"a": 1}
    assert results[0][1] is not None  # Has TTL
    assert results[1][0] == {"b": 2}
    assert results[1][1] is None  # No TTL
    assert results[2] == (None, None)

    # Empty list
    results = await store.ttl_many([])
    assert results == []


async def test_cluster_store_put_many(docket: Docket):  # pragma: no cover
    """Test ClusterKeyValueStore.put_many method."""
    from docket._result_store import ClusterKeyValueStore

    if not docket._redis.is_cluster:
        pytest.skip("Only runs in cluster mode")

    store = docket.result_storage
    assert isinstance(store._store, ClusterKeyValueStore)

    await store.put_many(
        ["put-many-1", "put-many-2"],
        [{"x": 1}, {"y": 2}],
        ttl=300,
    )

    assert await store.get("put-many-1") == {"x": 1}
    assert await store.get("put-many-2") == {"y": 2}

    # Empty list
    await store.put_many([], [])


async def test_cluster_store_delete_many(docket: Docket):  # pragma: no cover
    """Test ClusterKeyValueStore.delete_many method."""
    from docket._result_store import ClusterKeyValueStore

    if not docket._redis.is_cluster:
        pytest.skip("Only runs in cluster mode")

    store = docket.result_storage
    assert isinstance(store._store, ClusterKeyValueStore)

    await store.put("del-many-1", {"a": 1})
    await store.put("del-many-2", {"b": 2})

    count = await store.delete_many(["del-many-1", "del-many-2", "nonexistent"])
    assert count == 2

    assert await store.get("del-many-1") is None
    assert await store.get("del-many-2") is None

    # Empty list
    count = await store.delete_many([])
    assert count == 0
