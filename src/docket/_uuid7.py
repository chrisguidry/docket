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
# Original: https://github.com/stevesimmons/uuid7
def _vendored_uuid7() -> uuid.UUID:
    """
    Generate a UUID v7 with embedded timestamp.

    Simplified to match stdlib signature (no parameters). This implementation
    preserves the core time-based UUID generation but removes extended features
    (custom timestamps, sequence counters, output formats) from the original.

    The 128 bits in the UUID are allocated as follows:
    - 36 bits of whole seconds
    - 24 bits of fractional seconds, giving approx 50ns resolution
    - 14 bits of random data (original had sequence counter here)
    - 48 bits of randomness
    plus, at locations defined by RFC4122, 4 bits for the
    uuid version (0b0111) and 2 bits for the uuid variant (0b10).

             0                   1                   2                   3
             0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    t1      |                 unixts (secs since epoch)                     |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    t2/t3   |unixts |  frac secs (12 bits)  |  ver  |  frac secs (12 bits)  |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    t4/rand |var|       rand (14 bits)      |          rand (16 bits)       |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    rand    |                          rand (32 bits)                       |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    """
    # Get current time in nanoseconds
    ns = time.time_ns()

    # Split into seconds and fractional parts for high-precision timestamp
    # Treat the first 8 bytes of the uuid as a long (t1) and two ints
    # (t2 and t3) holding 36 bits of whole seconds and 24 bits of
    # fractional seconds.
    sixteen_secs = 16_000_000_000
    t1, rest1 = divmod(ns, sixteen_secs)
    t2, rest2 = divmod(rest1 << 16, sixteen_secs)
    t3, _ = divmod(rest2 << 12, sixteen_secs)
    t3 |= 7 << 12  # Put uuid version 0b0111 in top 4 bits

    # Variant 0b10 in top two bits, remaining 14 bits are random
    # (original implementation had sequence counter in these 14 bits)
    t4 = 2 << 14

    # Six random bytes for the lower part of the uuid
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
