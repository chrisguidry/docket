"""Redis Sentinel support for docket's ``redis+sentinel://`` URLs.

This module is the single point of Sentinel-awareness: URL parsing, the parsed
configuration, the connection pool that resolves the master through the Sentinel
daemons, and the tight TCP keepalive defaults that let a silently-dead master be
noticed promptly.  ``_redis.py`` only detects the scheme and dispatches here.
"""

from __future__ import annotations

import logging
import socket
from dataclasses import dataclass
from typing import Any, Sequence, cast
from urllib.parse import ParseResult, parse_qs, unquote, urlencode, urlparse

from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.connection import parse_url
from redis.asyncio.sentinel import Sentinel, SentinelConnectionPool

from ._redis import close_resource

logger: logging.Logger = logging.getLogger(__name__)


# Redis Sentinel daemons listen on 26379 by default, so sentinel members
# without an explicit port assume that rather than the data-node port 6379.
DEFAULT_SENTINEL_PORT = 26379

SENTINEL_SCHEMES = ("redis+sentinel", "rediss+sentinel")


# docket disables the client read timeout (see BLOCKING_READ_SOCKET_TIMEOUT in
# _redis.py) because of its long, unbounded blocking reads.  That means a master
# that dies *silently* — a network partition or frozen hypervisor that never
# sends a FIN/RST — would leave a blocked XREADGROUP waiting on the OS-default
# TCP keepalive (~2 hours on Linux) while Sentinel completes failover in seconds.
# We build the Sentinel pool ourselves, so we pin tight keepalive probes here: a
# dead peer is noticed in roughly TCP_KEEPIDLE + TCP_KEEPCNT * TCP_KEEPINTVL
# seconds.  The probes only fire on an otherwise-idle socket and a healthy peer
# answers them, so they don't disturb a legitimately long blocking read — which
# is exactly why disabling the read timeout is safe.  These match redis-py 8.0's
# own defaults; pinning them keeps the behaviour identical on the older redis-py
# releases docket supports (redis>=5), where connections default to no keepalive.
_SENTINEL_KEEPALIVE_TIMERS: tuple[tuple[str, int], ...] = (
    ("TCP_KEEPIDLE", 30),
    ("TCP_KEEPINTVL", 5),
    ("TCP_KEEPCNT", 3),
)
SENTINEL_SOCKET_KEEPALIVE_OPTIONS: dict[int, int] = {
    int(getattr(socket, name)): value
    for name, value in _SENTINEL_KEEPALIVE_TIMERS
    if hasattr(socket, name)
}


def is_sentinel_url(url: str) -> bool:
    """Whether ``url`` is a redis+sentinel:// or rediss+sentinel:// URL.

    A plain string-prefix check so the rest of ``_redis.py`` can leave standalone,
    cluster, and memory URLs to ``urlparse`` and only carve the netloc by hand for
    the multi-host sentinel case.
    """
    return url.partition("://")[0] in SENTINEL_SCHEMES


def urlparse_multihost(url: str) -> ParseResult:
    """urlparse a sentinel URL while preserving its multi-host netloc.

    A sentinel URL lists several daemons in its netloc, e.g.
    ``s1:26379,[::1]:26379``.  ``urlparse`` raises ``ValueError("Invalid IPv6
    URL")`` for any netloc where data precedes a ``[`` bracket — true on every
    currently-supported CPython release — so the netloc is carved off by hand,
    the rest of the URL is parsed with a placeholder host, and the real netloc is
    restored on the result.
    """
    scheme, _, remainder = url.partition("://")
    end = len(remainder)
    for terminator in "/?#":
        index = remainder.find(terminator)
        if index != -1:
            end = min(end, index)
    netloc, tail = remainder[:end], remainder[end:]
    parsed = urlparse(f"{scheme}://netloc-placeholder{tail}")
    return parsed._replace(netloc=netloc)


@dataclass(frozen=True)
class SentinelConfiguration:
    """A Redis Sentinel topology parsed from a redis+sentinel:// URL.

    ``connection_kwargs`` applies to the data-node (master) connections and
    ``sentinel_kwargs`` to the connections to the Sentinel daemons themselves;
    both hold redis-py-native keys (``username``, ``password``, ``ssl``, plus
    any standard connection options from the URL query string) so a caller can
    splat them directly.
    """

    sentinels: list[tuple[str, int]]
    service_name: str
    db: int
    connection_kwargs: dict[str, Any]
    sentinel_kwargs: dict[str, Any]


def parse_sentinel_url(url: str) -> SentinelConfiguration:
    """Parse a redis+sentinel:// or rediss+sentinel:// URL.

    The grammar follows the redis-sentinel-url convention:

        redis+sentinel://[user:pass@]host[:port][,host2[:port2],...]/service_name[/db]

    The database comes from the path and the data-node (master) credentials from
    the URL userinfo; the ``sentinel_username`` and ``sentinel_password`` query
    parameters configure authentication to the Sentinel daemons separately. A
    ``rediss`` prefix turns on TLS for both the data nodes and the Sentinel
    daemons.

    All other query parameters are standard redis-py connection options
    (``max_connections``, ``socket_timeout``, ``health_check_interval``, ...)
    and apply to the data-node connections with exactly the same parsing as a
    standalone ``redis://`` URL.

    Raises:
        ValueError: for a missing host, malformed port, missing service name,
            malformed database index, or malformed connection option
    """
    parsed = urlparse_multihost(url)

    # urlparse only exposes the segment before the first comma through
    # .hostname/.port, so the multi-host netloc is split by hand and each member
    # parsed on its own; urlparse handles ports, default ports, bracketed IPv6,
    # and port validation per member.
    hostpart = parsed.netloc.rpartition("@")[2]
    sentinels: list[tuple[str, int]] = []
    for member in hostpart.split(","):
        member = member.strip()
        if not member:
            continue
        member_url = urlparse(f"//{member}")
        host = member_url.hostname
        if not host:
            raise ValueError(f"Missing host in sentinel member {member!r}")
        try:
            port = member_url.port
        except ValueError:
            raise ValueError(f"Invalid port in sentinel member {member!r}") from None
        sentinels.append((host, port if port is not None else DEFAULT_SENTINEL_PORT))
    if not sentinels:
        raise ValueError("A sentinel URL requires at least one sentinel host")

    segments = [segment for segment in parsed.path.split("/") if segment]
    if not segments:
        raise ValueError(
            "A sentinel URL requires a service name, "
            "e.g. redis+sentinel://localhost:26379/mymaster"
        )
    service_name = segments[0]
    db = 0
    if len(segments) > 1:
        try:
            db = int(segments[1])
        except ValueError:
            raise ValueError(
                f"Invalid database index {segments[1]!r} in sentinel URL"
            ) from None

    tls = parsed.scheme == "rediss+sentinel"

    query = parse_qs(parsed.query, keep_blank_values=True)
    sentinel_kwargs: dict[str, Any] = {}
    sentinel_username = query.pop("sentinel_username", [""])[0]
    sentinel_password = query.pop("sentinel_password", [""])[0]
    if sentinel_username:
        sentinel_kwargs["username"] = sentinel_username
    if sentinel_password:
        sentinel_kwargs["password"] = sentinel_password
    if tls:
        sentinel_kwargs["ssl"] = True

    # The remaining query parameters are standard redis-py connection options
    # (max_connections, socket_timeout, health_check_interval, ...).  Funnel
    # them through redis-py's parse_url on a synthetic standalone URL so they get
    # exactly the same type conversion and validation as redis:// URLs.
    connection_kwargs: dict[str, Any] = {}
    if query:
        remaining_query = urlencode(
            [(name, value) for name, values in query.items() for value in values]
        )
        funneled: dict[str, Any] = parse_url(f"redis:///?{remaining_query}")
        # The query string carries only typed pool options; the database comes
        # from the path and credentials from the userinfo, so we don't let
        # parse_url turn a stray ?db=/?username=/?password= into another source.
        for governed in ("db", "username", "password"):
            funneled.pop(governed, None)
        connection_kwargs.update(funneled)

    if parsed.username:
        connection_kwargs["username"] = unquote(parsed.username)
    if parsed.password:
        connection_kwargs["password"] = unquote(parsed.password)
    if tls:
        connection_kwargs["ssl"] = True

    return SentinelConfiguration(
        sentinels=sentinels,
        service_name=service_name,
        db=db,
        connection_kwargs=connection_kwargs,
        sentinel_kwargs=sentinel_kwargs,
    )


class OwnedSentinelConnectionPool(SentinelConnectionPool):
    """A SentinelConnectionPool that owns its Sentinel manager.

    redis-py's Sentinel manager keeps one Redis client per Sentinel daemon and
    has no aclose() of its own, so closing the pool also closes those clients.

    The class-level annotations refine attributes that redis-py assigns in
    untyped __init__ code, so callers get typed access.
    """

    service_name: str
    is_master: bool
    connection_kwargs: dict[str, Any]
    sentinel_manager: Sentinel

    @property
    def sentinel_clients(self) -> Sequence[Redis]:
        """The Redis clients connected to the Sentinel daemons."""
        return cast(
            "Sequence[Redis]",
            self.sentinel_manager.sentinels,  # pyright: ignore[reportUnknownMemberType]
        )

    async def aclose(self) -> None:
        await super().aclose()
        for sentinel_client in self.sentinel_clients:
            await close_resource(sentinel_client, "sentinel client")


def sentinel_connection_pool(
    url: str, *, decode_responses: bool, socket_timeout: float | None
) -> ConnectionPool:
    """Create a connection pool that resolves the master through Sentinel.

    The pool asks the listed Sentinel daemons for the current master and follows
    failover automatically, so the rest of the standalone code path (client,
    pub/sub, publish, result storage) works unchanged.  ``socket_timeout`` is
    docket's blocking-read timeout, applied to the data-node connections.

    Args:
        url: The redis+sentinel:// or rediss+sentinel:// URL.
        decode_responses: If True, decode Redis responses from bytes to strings.
        socket_timeout: The read timeout for data-node connections.

    Returns:
        A ConnectionPool ready for use with Redis clients.
    """
    config = parse_sentinel_url(url)
    sentinel = Sentinel(config.sentinels, sentinel_kwargs=config.sentinel_kwargs)
    # Defaults first so URL query options override them, just as
    # ConnectionPool.from_url lets a socket_timeout in a standalone URL win.
    pool_kwargs: dict[str, Any] = {
        "db": config.db,
        "decode_responses": decode_responses,
        "socket_timeout": socket_timeout,
        "socket_keepalive": True,
        "socket_keepalive_options": SENTINEL_SOCKET_KEEPALIVE_OPTIONS,
        **config.connection_kwargs,
    }
    return OwnedSentinelConnectionPool(config.service_name, sentinel, **pool_kwargs)
