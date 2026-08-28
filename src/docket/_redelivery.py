"""How a worker takes messages off the stream, keeps them, and finds lost ones.

A worker keeps a message by renewing its lease: XCLAIM with ``idle=0`` resets
the message's idle clock, so the sweep below does not take it away while the
task is still running.  Renewal names every message the worker holds, so it
goes out in chunks of ``LEASE_RENEWAL_BATCH`` ids, and it asks for JUSTID.
Redis answers a plain XCLAIM with the whole body of every message, and it
blocks while it builds that reply for a worker that discards it.

A worker finds messages that another worker abandoned by sweeping its consumer
group's pending list with XAUTOCLAIM.  Redis reads up to ten times ``count``
entries of that list per call, so a scan on every pass of the worker loop pins
a busy Redis at 100% CPU once the pending list grows long.  The sweep is
periodic and bounded instead.

Each call claims at most ``REDELIVERY_SCAN_BATCH`` entries and starts where the
last call stopped.  A sweep that always restarted at ``0-0`` would never read
the entries past its first window, so those could never be redelivered.  Redis
returns the cursor ``0-0`` at the end of the pending list, which means the
sweep is done.

The first sweep runs on the worker's first pass, so a worker that replaces a
dead one picks up its messages as soon as the lease below lets it.  While a
sweep is in progress the worker scans on every pass, so the sweep finishes
quickly.  The next sweep then starts a quarter of the redelivery timeout later,
the cadence lease renewal uses, jittered between 75% and 125% of that.  Without the jitter, a fleet of
workers that started together would scan the pending list in lockstep, and
every worker's scan would reach Redis at the same moment.  An abandoned message
therefore waits at most a jittered quarter of the timeout longer than the
timeout itself.

One sweep costs Redis a pass over the whole pending list, so the cost of
sweeping must not grow with the number of workers.  The fleet runs one sweep
per interval instead of one per worker: before a worker starts a sweep it takes
a lease with ``SET {docket}:redelivery-sweep <worker> NX PX <interval>``, and
only the worker that takes the lease sweeps.  A worker that does not take it
waits for its own next timer.  Nobody renews or deletes the lease.  It expires
on its own, and that expiry is what paces the fleet.

A worker also caps how many new messages one XREADGROUP may return.  Redis
serializes every message it returns before it answers, and it blocks while it
does that, so a worker with tens of thousands of free slots would ask for tens
of thousands of messages at once.  ``DELIVERY_BATCH`` caps that ask.  The
worker loop reads again right away while it still has free slots, so the worker
fills up just as fast; only the time Redis spends on one call drops.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import timedelta
from typing import Sequence

from ._redis import RedisClient, RedisMessageID, RedisMessages

logger = logging.getLogger(__name__)

# Most message ids one XCLAIM call may name.
LEASE_RENEWAL_BATCH = 5000

# Most entries one XAUTOCLAIM call may claim, and so a tenth of the entries
# Redis will read for it.
REDELIVERY_SCAN_BATCH = 1000

# Most messages one XREADGROUP call may return.
DELIVERY_BATCH = 5000

# The cursor Redis returns at the end of the pending list, and the one that
# starts a fresh sweep.
SWEEP_START = "0-0"

# The narrowest and widest wait between sweeps, as a fraction of the interval.
JITTER = (0.75, 1.25)


async def renew_leases(
    redis: RedisClient,
    *,
    stream_key: str,
    group_name: str,
    consumer_name: str,
    message_ids: Sequence[RedisMessageID],
) -> None:
    """Reset the idle time of the messages one worker holds.

    A chunk that Redis refuses is logged and passed over, so the rest of the
    worker's messages still get their leases renewed on this pass.
    """
    for start in range(0, len(message_ids), LEASE_RENEWAL_BATCH):
        try:
            await redis.xclaim(
                name=stream_key,
                groupname=group_name,
                consumername=consumer_name,
                min_idle_time=0,
                message_ids=message_ids[start : start + LEASE_RENEWAL_BATCH],
                idle=0,
                justid=True,
            )
        except Exception:
            logger.warning("Failed to renew leases", exc_info=True)


class RedeliverySweep:
    """Where a worker's scan for abandoned messages is, and when it resumes."""

    def __init__(self, timeout: timedelta, *, lease_key: str, worker_name: str) -> None:
        self.min_idle_time = int(timeout.total_seconds() * 1000)
        self.interval = timeout.total_seconds() / 4
        self.lease_key = lease_key
        self.lease_milliseconds = max(1, int(self.interval * 1000))
        self.worker_name = worker_name
        self.cursor = SWEEP_START
        self.next_sweep = time.monotonic()

    async def due(self, redis: RedisClient) -> bool:
        """May this worker scan the pending list on this pass?

        A sweep that is already under way carries on without asking again.  A
        new sweep waits for this worker's timer, and then for the fleet-wide
        lease.  A worker that does not take the lease waits for its own next
        timer.
        """
        if self.cursor != SWEEP_START:
            return True
        if time.monotonic() < self.next_sweep:
            return False
        took_lease = await redis.set(
            self.lease_key,
            self.worker_name,
            nx=True,
            px=self.lease_milliseconds,
        )
        if not took_lease:
            self._rest()
        return bool(took_lease)

    async def claim(
        self,
        redis: RedisClient,
        *,
        stream_key: str,
        group_name: str,
        count: int,
    ) -> RedisMessages:
        """Claim the next batch of abandoned messages and move the cursor."""
        cursor, claimed, *_ = await redis.xautoclaim(
            name=stream_key,
            groupname=group_name,
            consumername=self.worker_name,
            min_idle_time=self.min_idle_time,
            start_id=self.cursor,
            count=min(count, REDELIVERY_SCAN_BATCH),
        )
        self.advance(cursor)
        return claimed

    def advance(self, cursor: bytes | str) -> None:
        """Record where the claim that Redis just answered stopped."""
        self.cursor = cursor.decode() if isinstance(cursor, bytes) else cursor
        if self.cursor == SWEEP_START:
            self._rest()

    def _rest(self) -> None:
        """Hold off the next sweep for a jittered interval."""
        low, high = JITTER
        wait = random.uniform(self.interval * low, self.interval * high)
        self.next_sweep = time.monotonic() + wait
