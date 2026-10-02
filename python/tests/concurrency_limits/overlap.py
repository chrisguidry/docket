"""Helpers for testing time-based overlap in concurrency tests.

Uses time.monotonic() to avoid clock drift and system time changes.
"""

from typing import Sequence


def intervals_overlap(
    start_a: float, end_a: float, start_b: float, end_b: float
) -> bool:
    """Check if two time intervals overlap.

    Two intervals overlap if A starts before B ends AND B starts before A ends.
    """
    return start_a < end_b and start_b < end_a


def assert_no_overlaps(intervals: Sequence[tuple[float, float]], context: str) -> None:
    """Assert that no intervals in the sequence overlap.

    Args:
        intervals: Sequence of (start, end) tuples from time.monotonic()
        context: Description for error messages (e.g., "customer 1 tasks")
    """
    for i, (start_a, end_a) in enumerate(intervals):
        for j, (start_b, end_b) in enumerate(intervals[i + 1 :], start=i + 1):
            if intervals_overlap(start_a, end_a, start_b, end_b):  # pragma: no cover
                raise AssertionError(
                    f"{context} overlapped: "
                    f"interval {i} [{start_a:.3f}-{end_a:.3f}] and "
                    f"interval {j} [{start_b:.3f}-{end_b:.3f}]"
                )


def assert_some_overlap(
    intervals: Sequence[tuple[float, float]], context: str, min_overlaps: int = 1
) -> None:
    """Assert that at least some intervals overlap (proving concurrent execution).

    Args:
        intervals: Sequence of (start, end) tuples from time.monotonic()
        context: Description for error messages (e.g., "different customer tasks")
        min_overlaps: Minimum number of overlapping pairs required
    """
    overlaps = 0
    for i, (start_a, end_a) in enumerate(intervals):
        for start_b, end_b in intervals[i + 1 :]:
            if intervals_overlap(start_a, end_a, start_b, end_b):  # pragma: no branch
                overlaps += 1

    if overlaps < min_overlaps:  # pragma: no cover
        raise AssertionError(
            f"{context} should have at least {min_overlaps} overlapping pairs, "
            f"but found {overlaps}"
        )
