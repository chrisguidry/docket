"""Tests for task result storage and serialization."""

from typing import Any, Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from docket import Docket, Worker
from docket.execution import ExecutionState


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
    """Test that result_storage uses RedisStore by default."""
    from urllib.parse import urlparse

    from key_value.aio.stores.redis import RedisStore

    assert isinstance(docket.result_storage, RedisStore)

    # Verify it's connected to the same Redis
    result_client = docket.result_storage._client  # type: ignore[attr-defined]
    pool_kwargs: dict[str, Any] = result_client.connection_pool.connection_kwargs  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

    if docket.url.startswith("memory://"):  # pragma: no cover
        assert "server" in pool_kwargs
    else:
        parsed = urlparse(docket.url)
        assert pool_kwargs.get("host") == (parsed.hostname or "localhost")
        assert pool_kwargs.get("port") == (parsed.port or 6379)
        expected_db = (
            int(parsed.path.lstrip("/")) if parsed.path and parsed.path != "/" else 0
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
