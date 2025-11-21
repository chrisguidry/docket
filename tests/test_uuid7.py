"""Tests for uuid7 polyfill functionality."""

import sys
import time
import uuid
from typing import Callable

import pytest

from docket._uuid7 import _vendored_uuid7  # pyright: ignore[reportPrivateUsage]
from docket._uuid7 import uuid7 as docket_uuid7

# Build list of implementations to test
implementations = [
    pytest.param(_vendored_uuid7, id="vendored"),
    pytest.param(docket_uuid7, id="docket"),
]

# Add stdlib if available (Python 3.14+)
if sys.version_info >= (3, 14):  # pragma: no branch
    from uuid import uuid7 as stdlib_uuid7  # pragma: no cover

    implementations.append(pytest.param(stdlib_uuid7, id="stdlib"))  # pragma: no cover


@pytest.fixture(params=implementations)
def uuid7_impl(request: pytest.FixtureRequest) -> Callable[[], uuid.UUID]:
    """Parametrized fixture that provides different uuid7 implementations."""
    return request.param


def test_uuid7_returns_uuid_object(uuid7_impl: Callable[[], uuid.UUID]) -> None:
    """uuid7() should return a UUID object."""
    result = uuid7_impl()
    assert isinstance(result, uuid.UUID)


def test_uuid7_is_version_7(uuid7_impl: Callable[[], uuid.UUID]) -> None:
    """uuid7() should return a version 7 UUID."""
    result = uuid7_impl()
    assert isinstance(result, uuid.UUID)
    assert result.version == 7


def test_uuid7_is_variant_rfc4122(uuid7_impl: Callable[[], uuid.UUID]) -> None:
    """uuid7() should return an RFC 4122 variant UUID."""
    result = uuid7_impl()
    assert isinstance(result, uuid.UUID)
    assert result.variant == uuid.RFC_4122


def test_uuid7_chronological_ordering(uuid7_impl: Callable[[], uuid.UUID]) -> None:
    """UUIDs generated in sequence should be chronologically sortable."""
    uuids: list[uuid.UUID] = []
    for _ in range(10):
        result = uuid7_impl()
        assert isinstance(result, uuid.UUID)
        uuids.append(result)
        time.sleep(0.001)  # 1ms delay to ensure different timestamps

    # UUIDs should be in ascending order
    assert uuids == sorted(uuids)


def test_uuid7_monotonicity_rapid_generation(
    uuid7_impl: Callable[[], uuid.UUID],
) -> None:
    """UUIDs generated rapidly (without delays) should maintain monotonic ordering.

    This tests the sequence counter mechanism that ensures ordering even when
    multiple UUIDs are generated within the same timestamp tick.
    """
    # Generate many UUIDs rapidly - many will share the same timestamp
    # Using 1000 to increase likelihood of catching monotonicity issues
    uuids: list[uuid.UUID] = []
    for _ in range(1000):
        result = uuid7_impl()
        assert isinstance(result, uuid.UUID)
        uuids.append(result)

    # Even with shared timestamps, they should maintain order via sequence counter
    assert uuids == sorted(uuids), (
        "UUIDs should be monotonic even within same timestamp"
    )


def test_uuid7_uniqueness(uuid7_impl: Callable[[], uuid.UUID]) -> None:
    """Rapidly generated UUIDs should be unique."""
    uuids: set[uuid.UUID] = set()
    for _ in range(1000):
        result = uuid7_impl()
        assert isinstance(result, uuid.UUID)
        uuids.add(result)

    # All UUIDs should be unique
    assert len(uuids) == 1000


def test_uuid7_as_str(uuid7_impl: Callable[[], uuid.UUID]) -> None:
    """uuid7() can be converted to string."""
    result = str(uuid7_impl())
    assert isinstance(result, str)
    # Should be in standard UUID format
    assert len(result) == 36
    assert result.count("-") == 4
    # Should be a valid UUID
    parsed = uuid.UUID(result)
    assert parsed.version == 7


def test_uuid7_as_int(uuid7_impl: Callable[[], uuid.UUID]) -> None:
    """uuid7() can be converted to integer via .int property."""
    result = uuid7_impl()
    uuid_int = result.int
    assert isinstance(uuid_int, int)
    assert uuid_int > 0


def test_uuid7_as_hex(uuid7_impl: Callable[[], uuid.UUID]) -> None:
    """uuid7() can be converted to hex via .hex property."""
    result = uuid7_impl()
    hex_str = result.hex
    assert isinstance(hex_str, str)
    assert len(hex_str) == 32
    # Should be valid hex
    int(hex_str, 16)


def test_uuid7_as_bytes(uuid7_impl: Callable[[], uuid.UUID]) -> None:
    """uuid7() can be converted to bytes via .bytes property."""
    result = uuid7_impl()
    uuid_bytes = result.bytes
    assert isinstance(uuid_bytes, bytes)
    assert len(uuid_bytes) == 16
