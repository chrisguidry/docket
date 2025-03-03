from datetime import timedelta

import pytest

from docket.cli import duration


def test_duration_parsing_passes_through_defaults():
    """Should pass through timedelta objects."""
    assert duration(timedelta(seconds=123)) == timedelta(seconds=123)


def test_duration_parsing_plain_seconds():
    """Should parse plain integers as seconds."""
    assert duration("123") == timedelta(seconds=123)
    assert duration("0") == timedelta(seconds=0)
    assert duration("60") == timedelta(seconds=60)


def test_duration_parsing_seconds_with_suffix():
    """Should parse values with 's' suffix as seconds."""
    assert duration("123s") == timedelta(seconds=123)
    assert duration("0s") == timedelta(seconds=0)
    assert duration("60s") == timedelta(seconds=60)


def test_duration_parsing_minutes_with_suffix():
    """Should parse values with 'm' suffix as minutes."""
    assert duration("5m") == timedelta(minutes=5)
    assert duration("0m") == timedelta(minutes=0)
    assert duration("60m") == timedelta(hours=1)


def test_duration_parsing_hours_with_suffix():
    """Should parse values with 'h' suffix as hours."""
    assert duration("2h") == timedelta(hours=2)
    assert duration("0h") == timedelta(hours=0)
    assert duration("24h") == timedelta(days=1)


def test_duration_parsing_minutes_seconds_format():
    """Should parse 'mm:ss' format correctly."""
    assert duration("1:30") == timedelta(minutes=1, seconds=30)
    assert duration("0:45") == timedelta(seconds=45)
    assert duration("10:00") == timedelta(minutes=10)


def test_duration_parsing_hours_minutes_seconds_format():
    """Should parse 'hh:mm:ss' format correctly."""
    assert duration("1:30:45") == timedelta(hours=1, minutes=30, seconds=45)
    assert duration("0:0:10") == timedelta(seconds=10)
    assert duration("2:0:0") == timedelta(hours=2)


@pytest.mark.parametrize(
    "invalid_duration",
    [
        "1:2:3:4",  # Too many parts
        "abc",  # Not a number
        ":",  # Empty parts
    ],
)
def test_duration_parsing_invalid_format(invalid_duration: str):
    """Should raise ValueError for invalid formats."""
    with pytest.raises(ValueError):
        duration(invalid_duration)
