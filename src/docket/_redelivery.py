"""How a worker finds messages that another worker abandoned.

A worker sweeps its consumer group's pending list with XAUTOCLAIM to find
messages that another worker left behind.  ``RedeliverySweep`` owns that claim:
it bounds each call, keeps the cursor across calls, and creates the consumer
group again when Redis reports it missing.

Each call claims at most ``REDELIVERY_SCAN_BATCH`` entries, so Redis reads at
most about ten times that many entries of the pending list per call.  The cost
of one call stays bounded however long the list grows.

Each call starts where the last one stopped.  A sweep that always restarted at
``0-0`` would never read the entries past its first window, so those could
never be redelivered.  The sweep keeps the cursor that XAUTOCLAIM returns.
Redis returns the cursor ``0-0`` at the end of the pending list, and the next
call then starts a fresh sweep from the front.

Walking the whole pending list once reads O(pending list) entries, so sweeping
on every poll pass pins Redis once that list grows large.  A worker sweeps on a
timer instead, at a quarter of the redelivery timeout.  Each wait is jittered by
``JITTER`` so a fleet that started together does not sweep in lockstep.  An
abandoned message therefore waits at most about a jittered quarter of the
timeout beyond the timeout itself before a sweep reclaims it.

One sweep costs O(pending list) no matter how many workers run, so the fleet
sweeps once per interval rather than once per worker.  When a worker's timer
elapses it takes a fleet-wide lease with ``SET {docket}:leases:redelivery-sweep
<worker> NX PX <interval>``, and only the worker holding the lease sweeps.  The
holder keeps it: it refreshes the lease on every pass while its sweep is under
way, so the lease covers the whole sweep and one worker sweeps at a time even
on a long pending list.  A worker whose timer fires while its own lease is
still live keeps that lease, rather than losing the sweep to itself.  Nobody
deletes the lease; it expires one interval after its last refresh, and that
expiry is what paces the fleet to at most one sweep per interval.  A worker
whose lease lapses mid-walk keeps its place in the walk and takes the lease
again when it can: at once if nobody else has it, or after resting if another
worker took it, so that worker can walk the list alone.

Once a sweep is under way the worker sweeps on every pass until the cursor
returns to the front.  Each pass claims no more entries than the worker has
free slots, so a worker with few free slots takes many passes to walk a long
pending list.
"""

from __future__ import annotations

import random
import time
from datetime import timedelta

from redis.exceptions import ResponseError

from ._lua import Arg, Key, redis_script
from ._redis import RedisClient, RedisMessages
from .docket import Docket

# Most entries one XAUTOCLAIM call may claim, and so about a tenth of the
# entries Redis reads for it.
REDELIVERY_SCAN_BATCH = 1000

# The cursor Redis returns at the end of the pending list, and the one that
# starts a fresh sweep from the front.
SWEEP_START = "0-0"

# The narrowest and widest wait between sweeps, as a fraction of the interval.
# The jitter keeps a fleet that started together from sweeping in lockstep.
JITTER = (0.75, 1.25)


@redis_script
async def _refresh_lease(
    redis: RedisClient,
    *,
    lease_key: Key[str],
    holder: Arg[str],
    duration_ms: Arg[int],
) -> int:
    """
    if redis.call('GET', lease_key) == holder then
        return redis.call('PEXPIRE', lease_key, duration_ms)
    end
    return 0
    """
    ...


class RedeliverySweep:
    """A worker's bounded, cursor-kept claim on abandoned messages."""

    def __init__(
        self,
        docket: Docket,
        *,
        worker_name: str,
        redelivery_timeout: timedelta,
    ) -> None:
        self.docket = docket
        self.worker_name = worker_name
        self.min_idle_time = int(redelivery_timeout.total_seconds() * 1000)
        self.interval = redelivery_timeout.total_seconds() / 4
        self.start_id = SWEEP_START
        # Due immediately, so a worker that replaces a dead one picks up its
        # messages without waiting a whole interval first.
        self.next_sweep = time.monotonic()

    async def due(self, redis: RedisClient) -> bool:
        """Whether the worker should sweep on this pass.

        A sweep already under way (the cursor is past the front) sweeps on
        every pass, and refreshes the lease each time so the lease spans the
        whole walk.  A worker that lost the lease mid-walk keeps its place and
        takes the lease again when it can: at once if the lease merely lapsed,
        or after resting if another worker holds it.

        Otherwise the sweep waits for the jittered timer, and then for the
        fleet-wide lease: the worker sweeps this interval only if it takes
        ``SET {docket}:leases:redelivery-sweep worker NX PX interval`` or
        already holds that lease.  A worker that finds another worker on the
        lease arms its next timer and waits, rather than spinning.
        """
        # A holder mid-walk keeps rolling; refreshing the lease each pass makes
        # it span the whole walk.  Anyone else goes through the timer and the
        # lease below, and a worker that lost its lease mid-walk resumes from
        # its kept place once it takes the lease again.
        if self.start_id != SWEEP_START and await self._holds_lease(redis):
            return True
        if time.monotonic() < self.next_sweep:
            return False
        took_lease = await redis.set(
            self.docket.redelivery_sweep_key,
            self.worker_name,
            nx=True,
            px=int(self.interval * 1000),
        )
        if took_lease:
            return True
        if await self._holds_lease(redis):
            return True
        self._rest()
        return False

    async def _holds_lease(self, redis: RedisClient) -> bool:
        """Whether this worker holds the lease, refreshed for another interval.

        The refresh is one script so that reading the holder and extending the
        lease cannot straddle another worker taking it.  A lease another worker
        holds is neither extended nor taken.
        """
        refreshed = await _refresh_lease(
            redis,
            lease_key=self.docket.redelivery_sweep_key,
            holder=self.worker_name,
            duration_ms=int(self.interval * 1000),
        )
        return bool(refreshed)

    async def claim(self, redis: RedisClient, available_slots: int) -> RedisMessages:
        """Claim the messages that another worker has left idle too long.

        The claim starts where the last one stopped, and asks for no more than
        ``REDELIVERY_SCAN_BATCH`` entries however many slots are free.  A
        docket that no worker has bootstrapped yet has no consumer group, so a
        NOGROUP answer means creating the group and claiming again.

        Redis answers with ``SWEEP_START`` at the end of the pending list.  The
        sweep then arms the next jittered timer, and a later pass starts a fresh
        sweep from the front once that timer elapses.
        """
        try:
            cursor, redeliveries, *_ = await redis.xautoclaim(
                name=self.docket.stream_key,
                groupname=self.docket.worker_group_name,
                consumername=self.worker_name,
                min_idle_time=self.min_idle_time,
                start_id=self.start_id,
                count=min(available_slots, REDELIVERY_SCAN_BATCH),
            )
        except ResponseError as e:
            if "NOGROUP" in str(e):
                await self.docket._ensure_stream_and_group()  # pyright: ignore[reportPrivateUsage]
                return await self.claim(redis, available_slots)
            raise  # pragma: no cover

        self.start_id = cursor.decode()
        if self.start_id == SWEEP_START:
            self._rest()
        return redeliveries

    def _rest(self) -> None:
        """Hold off the next sweep for a jittered interval."""
        self.next_sweep = time.monotonic() + random.uniform(*JITTER) * self.interval
