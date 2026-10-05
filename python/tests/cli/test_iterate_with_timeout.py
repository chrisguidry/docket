import asyncio
from typing import Any

import pytest

from docket.cli import iterate_with_timeout


async def test_iterate_with_timeout_normal_iteration():
    """iterate_with_timeout yields items from iterator normally."""

    async def mock_iterator():
        for i in range(3):
            yield {"value": i}

    results: list[dict[str, Any] | None] = []
    async for item in iterate_with_timeout(mock_iterator(), timeout=1.0):
        results.append(item)

    assert results == [{"value": 0}, {"value": 1}, {"value": 2}]


async def test_iterate_with_timeout_yields_none_on_timeout():
    """iterate_with_timeout yields None when asyncio.TimeoutError occurs."""

    async def slow_iterator():
        await asyncio.sleep(2.0)
        yield {"value": 1}  # pragma: no cover

    results: list[dict[str, Any] | None] = []
    async for item in iterate_with_timeout(
        slow_iterator(), timeout=0.1
    ):  # pragma: no branch
        results.append(item)
        if item is None:  # pragma: no branch
            break
        # pragma: no cover - we always timeout in this test

    assert results[0] is None


async def test_iterate_with_timeout_stops_on_stop_iteration():
    """iterate_with_timeout breaks and cleans up on StopAsyncIteration."""

    async def finite_iterator():
        yield {"value": 1}

    results: list[dict[str, Any] | None] = []
    async for item in iterate_with_timeout(finite_iterator(), timeout=1.0):
        results.append(item)

    assert results == [{"value": 1}]


async def test_iterate_with_timeout_cleanup_on_break():
    """iterate_with_timeout ensures aclose() is called in finally block."""
    close_called = False

    class MockAsyncIterator:
        def __aiter__(self):  # pragma: no cover
            return self

        async def __anext__(self) -> dict[str, Any]:
            return {"value": 1}

        async def aclose(self) -> None:
            nonlocal close_called
            close_called = True

    mock_iter = MockAsyncIterator()
    gen = iterate_with_timeout(mock_iter, timeout=1.0)
    async for _item in gen:  # pragma: no branch
        break
        # pragma: no cover - we always break in this test

    await gen.aclose()
    assert close_called, "aclose() should be called in finally block"


async def test_iterate_with_timeout_cleanup_on_exception():
    """iterate_with_timeout ensures aclose() even when exception occurs."""
    close_called = False

    class MockAsyncIterator:
        def __aiter__(self):  # pragma: no cover
            return self

        async def __anext__(self) -> dict[str, Any]:
            raise ValueError("Test error")

        async def aclose(self) -> None:
            nonlocal close_called
            close_called = True

    mock_iter = MockAsyncIterator()

    with pytest.raises(ValueError, match="Test error"):
        async for _item in iterate_with_timeout(mock_iter, timeout=1.0):
            pass  # pragma: no cover - exception always raised before this

    assert close_called, "aclose() should be called even on exception"
