"""
Implementation of UUID v7 per the October 2021 draft update
to RFC4122 from 2005:
https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format

Stephen Simmons, v0.1.0, 2021-12-27
"""

# pyright: reportUnknownParameterType=false, reportMissingParameterType=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportDeprecated=false

__all__ = (
    "uuid7",
    "uuid7str",
    "time_ns",
    "check_timing_precision",
    "uuid_to_datetime",
)

import datetime
import os
import struct
import time
from typing import Callable, Optional, Union
import uuid

# Expose function used by uuid7() to get current time in nanoseconds
# since the Unix epoch.
time_ns = time.time_ns


def uuid7(
    ns: Optional[int] = None,
    as_type: Optional[str] = None,
    time_func: Callable[[], int] = time_ns,
    _last=[0, 0, 0, 0],
    _last_as_of=[0, 0, 0, 0],
) -> Union[uuid.UUID, str, int, bytes]:
    """
    UUID v7, following the proposed extension to RFC4122 described in
    https://www.ietf.org/id/draft-peabody-dispatch-new-uuid-format-02.html.
    All representations (string, byte array, int) sort chronologically,
    with a potential time resolution of 50ns (if the system clock
    supports this).

    Parameters
    ----------

    ns      - Optional integer with the whole number of nanoseconds
                since Unix epoch, to set the "as of" timestamp.
                As a special case, uuid7(ns=0) returns the zero UUID.

    as_type - Optional string to return the UUID in a different format.
                A uuid.UUID (version 7, variant 0x10) is returned unless
                this is one of 'str', 'int', 'hex' or 'bytes'.

    time_func - Set the time function, which must return integer
                nanoseconds since the Unix epoch, midnight on 1-Jan-1970.
                Defaults to time.time_ns(). This is exposed because
                time.time_ns() may have a low resolution on Windows.

    _last and _last_as_of - Used internally to trigger incrementing a
                sequence counter when consecutive calls have the same time
                values. The values [t1, t2, t3, seq] are described below.

    Returns
    -------

    A UUID object, or if as_type is specified, a string, int or
    bytes of length 16.

    Implementation notes
    --------------------

    The 128 bits in the UUID are allocated as follows:
    - 36 bits of whole seconds
    - 24 bits of fractional seconds, giving approx 50ns resolution
    - 14 bits of sequential counter, if called repeatedly in same time tick
    - 48 bits of randomness
    plus, at locations defined by RFC4122, 4 bits for the
    uuid version (0b111) and 2 bits for the uuid variant (0b10).

             0                   1                   2                   3
             0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    t1      |                 unixts (secs since epoch)                     |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    t2/t3   |unixts |  frac secs (12 bits)  |  ver  |  frac secs (12 bits)  |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    t4/rand |var|       seq (14 bits)       |          rand (16 bits)       |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    rand    |                          rand (32 bits)                       |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    Indicative timings:
    - uuid.uuid4()            2.4us
    - uuid7()                 3.7us
    - uuid7(as_type='int')    1.6us
    - uuid7(as_type='str')    2.5us

    Examples
    --------

    >>> uuid7()
    UUID('061cb26a-54b8-7a52-8000-2124e7041024')

    >>> uuid7(0)
    UUID('00000000-0000-0000-0000-00000000000')

    >>> for fmt in ('bytes', 'hex', 'int', 'str', 'uuid', None):
    ...     print(fmt, repr(uuid7(as_type=fmt)))
    bytes b'\x06\x1c\xb8\xfe\x0f\x0b|9\x80\x00\tjt\x85\xb3\xbb'
    hex '061cb8fe0f0b7c3980011863b956b758'
    int 8124504378724980906989670469352026642
    str '061cb8fe-0f0b-7c39-8003-d44a7ee0bdf6'
    uuid UUID('061cb8fe-0f0b-7c39-8004-0489578299f6')
    None UUID('061cb8fe-0f0f-7df2-8000-afd57c2bf446')
    """
    if ns is None:
        ns = time_func()
        last = _last
    else:
        last = _last_as_of
        ns = int(ns)  # Fail fast if not an int

    if ns == 0:
        # Special cose for all-zero uuid. Strictly speaking not a UUIDv7.
        t1 = t2 = t3 = t4 = 0
        rand = b"\0" * 6
    else:
        # Treat the first 8 bytes of the uuid as a long (t1) and two ints
        # (t2 and t3) holding 36 bits of whole seconds and 24 bits of
        # fractional seconds.
        # This gives a nominal 60ns resolution, comparable to the
        # timestamp precision in Linux (~200ns) and Windows (100ns ticks).
        sixteen_secs = 16_000_000_000
        t1, rest1 = divmod(ns, sixteen_secs)
        t2, rest2 = divmod(rest1 << 16, sixteen_secs)
        t3, _ = divmod(rest2 << 12, sixteen_secs)
        t3 |= 7 << 12  # Put uuid version in top 4 bits, which are 0 in t3

        # The next two bytes are an int (t4) with two bits for
        # the variant 2 and a 14 bit sequence counter which increments
        # if the time is unchanged.
        if t1 == last[0] and t2 == last[1] and t3 == last[2]:
            # Stop the seq counter wrapping past 0x3FFF.
            # This won't happen in practice, but if it does,
            # uuids after the 16383rd with that same timestamp
            # will not longer be correctly ordered but
            # are still unique due to the 6 random bytes.
            if last[3] < 0x3FFF:
                last[3] += 1
        else:
            last[:] = (t1, t2, t3, 0)
        t4 = (2 << 14) | last[3]  # Put variant 0b10 in top two bits

        # Six random bytes for the lower part of the uuid
        rand = os.urandom(6)

    # Build output
    if as_type == "str":
        return f"{t1:>08x}-{t2:>04x}-{t3:>04x}-{t4:>04x}-{rand.hex()}"

    r = int.from_bytes(rand, "big")
    uuid_int = (t1 << 96) + (t2 << 80) + (t3 << 64) + (t4 << 48) + r
    if as_type == "int":
        return uuid_int
    elif as_type == "hex":
        return f"{uuid_int:>032x}"
    elif as_type == "bytes":
        return uuid_int.to_bytes(16, "big")
    else:
        return uuid.UUID(int=uuid_int)


def uuid7str(ns: Optional[int] = None) -> str:
    "uuid7() as a string without creating a UUID object first."
    return uuid7(ns, as_type="str")  # type: ignore


def check_timing_precision(
    timing_func: Optional[Callable[[], int]] = None,
) -> str:
    """
    Message indicating the timing precision from various time/clock
    functions that might be used for UUIDv7 generation.

    This tests time.time_ns(), time.perf_counter_ns()
    and datetime.datetime.utcnow converted to ns.

    A user-supplied timing function may also be provided.
    It must return the number of ns since the Unix Epoch
    (midnight at 1-Jan-1970).

    Note that time.time_ns() updates every 200us under Linux
    and potentially as infrequently as every 5ms under Windows.

    Usage:
    >>> check_timing_precision()
    # Under Linux
    time.time_ns()           has a timing precision of   221ns rather than 221ns (1,000 distinct samples in 0.00s)
    time.perf_counter_ns()   has a timing precision of   215ns rather than 215ns (1,000 distinct samples in 0.00s)
    datetime.datetime.utcnow has a timing precision of 1,046ns rather than 679ns (1,000 distinct samples in 0.00s)
    # Under Windows
    time.time_ns()           has a timing precision of 4,950,500ns rather than   709ns (705,068 samples of which 101 are distinct, in 0.50s)
    time.perf_counter_ns()   has a timing precision of       823ns rather than   823ns (1,000 samples of which 1,000 are distinct, in 0.00s)
    datetime.datetime.utcnow has a timing precision of 5,882,365ns rather than 2,812ns (177,792 samples of which 85 are distinct, in 0.50s)
    """
    timing_funcs = [
        ("time.time_ns()", time.time_ns),
        ("time.perf_counter_ns()", time.perf_counter_ns),
        (
            "datetime.datetime.utcnow",
            lambda: int(datetime.datetime.utcnow().timestamp() * 1_000_000_000),
        ),
    ]
    if timing_func is not None:
        timing_funcs.append(("user-supplied", timing_func))

    lines = []
    for desc, fn in timing_funcs:
        started_ns = time.perf_counter_ns()
        values = set()
        ctr = 0
        while True:
            values.add(fn())
            ctr += 1
            elapsed_ns = time.perf_counter_ns() - started_ns
            if elapsed_ns > 500_000_000 or len(values) >= 1000:
                break
        precision_ns = elapsed_ns / len(values)
        ideal_precision_ns = elapsed_ns / ctr
        lines.append(
            f"{desc} has a timing precision of {precision_ns:0,.0f}ns \
rather than {ideal_precision_ns:0,.0f}ns ({ctr:,} samples of which \
{len(values):,} are distinct, in {elapsed_ns / 1_000_000_000:0.2f}s)"
        )

    return "\n".join(lines)


def timestamp_ns(
    s: Union[str, uuid.UUID, int],
    suppress_error=True,
) -> Optional[int]:
    """
    Recover the timestamp from a UUIDv7, passed in
    as a string, integer or a UUID object.

    If the UUID is not a version 7 UUID, either raise a ValueError
    or return None, depending on suppress_error.

    Usage:
    >>> uuid_to_datetime("1eb22fe4-3f0c-62b1-a88c-8dc55231702f")
    datetime.datetime(2020, 11, 10, 2, 41, 42, 182162)
    """
    if isinstance(s, uuid.UUID):
        x = s.bytes
    elif not s:
        x = b"\0" * 16
    elif isinstance(s, int):
        x = int.to_bytes(s, length=16, byteorder="big")
    else:  # String form that should look like a UUID
        int_uuid = int(str(s).replace("-", ""), base=16)
        x = int.to_bytes(int_uuid, length=16, byteorder="big")

    uuid_version = x[6] >> 4
    if uuid_version == 7:
        bits = struct.unpack(">IHHHHI", x)
        uuid_version = (bits[2] >> 12) & 0xF
        # uuid_variant = (bits[3] >> 62) & 0x3
        whole_secs = (bits[0] << 4) + (bits[1] >> 12)
        frac_binary = (
            ((bits[1] & 0x0FFF) << 26) + ((bits[2] & 0x0FFF) << 14) + (bits[3] & 0x3FFF)
        )
        frac_ns, _ = divmod(frac_binary * 1_000_000_000, 1 << 38)
        ns_since_epoch = whole_secs * 1_000_000_000 + frac_ns
        return ns_since_epoch
    elif suppress_error:
        return None
    else:
        raise ValueError(
            f"{str(s)} is a version {uuid_version} UUID, \
not v7 so we cannot extract the timestamp."
        )


def uuid_to_datetime(
    s: Union[str, uuid.UUID, int],
    suppress_error=True,
) -> Optional[datetime.datetime]:
    ns_since_epoch = timestamp_ns(s, suppress_error=suppress_error)
    if ns_since_epoch is None:
        return None
    else:
        return datetime.datetime.fromtimestamp(
            ns_since_epoch / 1_000_000_000,
            tz=datetime.timezone.utc,
        )
