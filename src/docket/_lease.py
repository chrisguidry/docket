"""A fleet-wide lease that lets one worker run a periodic, Redis-heavy job.

Some jobs cost the same however large the fleet is: a scan of the whole
scheduled-tasks queue, or of the whole pending list.  Every worker running one
each interval multiplies that cost by the fleet size for no gain.  A lease gates
the job to one worker per interval instead.

The lease is a single SET with NX and PX: the first worker to set the key holds
it, and later workers fail the NX and skip the job.  Nobody renews or deletes
it, so it expires on its own after ``ttl_ms``, and that expiry is what paces the
fleet to at most one run per interval.
"""

from __future__ import annotations

from ._redis import RedisClient


async def take_lease(redis: RedisClient, key: str, holder: str, ttl_ms: int) -> bool:
    """Try to take the fleet-wide lease at ``key`` for ``ttl_ms`` milliseconds.

    ``SET key holder NX PX ttl_ms`` succeeds only when the key is free, so at
    most one caller per interval wins.  redis-py returns a truthy value on
    success and ``None`` when the key already exists, so this returns whether
    ``holder`` won the lease.  The caller that wins runs the job; a caller that
    loses skips it and waits for its next turn.
    """
    return bool(await redis.set(key, holder, nx=True, px=ttl_ms))
