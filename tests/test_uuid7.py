"""Tests for uuid7 polyfill functionality."""

import time
import uuid

from docket._uuid7 import uuid7


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


def test_uuid7_as_str() -> None:
    """uuid7() can be converted to string."""
    result = str(uuid7())
    assert isinstance(result, str)
    # Should be in standard UUID format
    assert len(result) == 36
    assert result.count("-") == 4
    # Should be a valid UUID
    parsed = uuid.UUID(result)
    assert parsed.version == 7


def test_uuid7_as_int() -> None:
    """uuid7() can be converted to integer via .int property."""
    result = uuid7()
    uuid_int = result.int
    assert isinstance(uuid_int, int)
    assert uuid_int > 0


def test_uuid7_as_hex() -> None:
    """uuid7() can be converted to hex via .hex property."""
    result = uuid7()
    hex_str = result.hex
    assert isinstance(hex_str, str)
    assert len(hex_str) == 32
    # Should be valid hex
    int(hex_str, 16)


def test_uuid7_as_bytes() -> None:
    """uuid7() can be converted to bytes via .bytes property."""
    result = uuid7()
    uuid_bytes = result.bytes
    assert isinstance(uuid_bytes, bytes)
    assert len(uuid_bytes) == 16
