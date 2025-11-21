"""
UUID v7 polyfill for Python < 3.14.

On Python 3.14+, we use the stdlib implementation. On older versions,
we use a simplified vendored implementation.

The vendored code can be removed once Python 3.13 support is dropped.
"""

import os
import sys
import time
import uuid
from typing import Callable

__all__ = ("uuid7",)


# Vendored implementation for Python < 3.14
# Simplified from Stephen Simmons' implementation (v0.1.0, 2021-12-27)
def _vendored_uuid7() -> uuid.UUID:
    """
    Generate a UUID v7 with embedded timestamp.

    Returns a UUID object (version 7, variant RFC 4122) where the first
    48 bits contain a Unix timestamp in milliseconds, followed by random
    bits for uniqueness and a monotonic sequence counter.
    """
    # Get current time in nanoseconds
    ns = time.time_ns()

    # Split into seconds and fractional parts for high-precision timestamp
    # The UUID embeds 36 bits of seconds + 24 bits of fractional seconds
    sixteen_secs = 16_000_000_000
    t1, rest1 = divmod(ns, sixteen_secs)
    t2, rest2 = divmod(rest1 << 16, sixteen_secs)
    t3, _ = divmod(rest2 << 12, sixteen_secs)
    t3 |= 7 << 12  # UUID version 7 in top 4 bits

    # Variant bits (0b10) and random bits for the rest
    t4 = 2 << 14  # Variant RFC 4122

    # Six random bytes for uniqueness
    rand = os.urandom(6)

    # Build the UUID from components
    r = int.from_bytes(rand, "big")
    uuid_int = (t1 << 96) + (t2 << 80) + (t3 << 64) + (t4 << 48) + r
    return uuid.UUID(int=uuid_int)


# On Python 3.14+, use stdlib uuid7; otherwise use vendored implementation
if sys.version_info >= (3, 14):
    from uuid import uuid7
else:
    uuid7: Callable[[], uuid.UUID] = _vendored_uuid7
