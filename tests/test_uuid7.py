"""Tests for uuid7 polyfill functionality."""

import time
import uuid
from datetime import datetime, timezone

from docket._uuid7 import uuid7, uuid7str  # pyright: ignore[reportUnknownVariableType]


def test_uuid7_returns_uuid_object() -> None:
    """uuid7() should return a UUID object."""
    result = uuid7()
    assert isinstance(result, uuid.UUID)


def test_uuid7_is_version_7() -> None:
    """uuid7() should return a version 7 UUID."""
    result = uuid7()
    assert isinstance(result, uuid.UUID)
    assert result.version == 7


def test_uuid7_is_variant_rfc4122() -> None:
    """uuid7() should return an RFC 4122 variant UUID."""
    result = uuid7()
    assert isinstance(result, uuid.UUID)
    assert result.variant == uuid.RFC_4122


def test_uuid7_chronological_ordering() -> None:
    """UUIDs generated in sequence should be chronologically sortable."""
    uuids: list[uuid.UUID] = []
    for _ in range(10):
        result = uuid7()
        assert isinstance(result, uuid.UUID)
        uuids.append(result)
        time.sleep(0.001)  # 1ms delay to ensure different timestamps

    # UUIDs should be in ascending order
    assert uuids == sorted(uuids)


def test_uuid7_uniqueness() -> None:
    """Rapidly generated UUIDs should be unique."""
    uuids: set[uuid.UUID] = set()
    for _ in range(1000):
        result = uuid7()
        assert isinstance(result, uuid.UUID)
        uuids.add(result)

    # All UUIDs should be unique
    assert len(uuids) == 1000


def test_uuid7str_returns_string() -> None:
    """uuid7str() should return a string representation."""
    result = uuid7str()
    assert isinstance(result, str)
    # Should be in standard UUID format
    assert len(result) == 36
    assert result.count("-") == 4


def test_uuid7str_is_valid_uuid() -> None:
    """uuid7str() should return a valid UUID string."""
    result = uuid7str()
    parsed = uuid.UUID(result)
    assert parsed.version == 7
    assert parsed.variant == uuid.RFC_4122


def test_uuid7_with_zero_ns() -> None:
    """uuid7(ns=0) should return the zero UUID."""
    result = uuid7(ns=0)
    assert result == uuid.UUID("00000000-0000-0000-0000-000000000000")


def test_uuid7_with_specific_timestamp() -> None:
    """uuid7() should accept a specific timestamp in nanoseconds."""
    # Use a fixed timestamp
    ns = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000_000)
    result = uuid7(ns=ns)
    assert isinstance(result, uuid.UUID)
    assert result.version == 7


def test_uuid7_as_type_str() -> None:
    """uuid7(as_type='str') should return a string."""
    result = uuid7(as_type="str")
    assert isinstance(result, str)
    assert len(result) == 36


def test_uuid7_as_type_int() -> None:
    """uuid7(as_type='int') should return an integer."""
    result = uuid7(as_type="int")
    assert isinstance(result, int)
    assert result > 0


def test_uuid7_as_type_hex() -> None:
    """uuid7(as_type='hex') should return a hex string."""
    result = uuid7(as_type="hex")
    assert isinstance(result, str)
    assert len(result) == 32
    # Should be valid hex
    int(result, 16)


def test_uuid7_as_type_bytes() -> None:
    """uuid7(as_type='bytes') should return bytes."""
    result = uuid7(as_type="bytes")
    assert isinstance(result, bytes)
    assert len(result) == 16
