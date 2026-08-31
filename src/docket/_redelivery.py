"""How a worker keeps its own messages and finds ones another worker abandoned.

A worker keeps a message by renewing its lease: XCLAIM with ``idle=0`` resets
the message's idle clock, so the sweep below does not take it away while the
task is still running.  A renewal names every message the worker holds, so it
goes out in chunks of at most ``LEASE_RENEWAL_BATCH`` ids, and it asks for
JUSTID.  A plain XCLAIM answers with the whole body of every message, and Redis
blocks while it builds that reply for a worker that discards it.

A worker sweeps its consumer group's pending list with XAUTOCLAIM to find
messages that another worker left behind.  ``RedeliverySweep`` owns that claim:
it bounds each call, keeps the cursor across calls, and creates the consumer
group again when Redis reports it missing.

Each call claims at most the worker's ``message_batch`` entries, so Redis reads
at most about ten times that many entries of the pending list per call.  The
cost of one call stays bounded however long the list grows.

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
<worker> NX PX <interval>``, and only the worker that takes the lease sweeps.  A
worker that loses the lease arms its own next timer and waits, rather than
spinning.  Nobody renews or deletes the lease; it expires after one interval on
its own, and that expiry is what paces the fleet to at most one sweep per
interval.  The tradeoff is that a lone worker that just held the lease may wait
up to about one extra interval before it sweeps again.  That is acceptable
because redelivery is bounded by the timeout, not latency-critical.

Once a sweep is under way the worker keeps sweeping on every pass until the
cursor returns to the front, so a long pending list is walked promptly rather
than one window per timer tick.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import timedelta
from typing import Sequence

from redis.exceptions import ResponseError

from ._redis import RedisClient, RedisMessageID, RedisMessages
from .docket import Docket

logger = logging.getLogger(__name__)

# The most message ids one XCLAIM may name, so no single renewal command blocks
# Redis long or ships a large reply.
LEASE_RENEWAL_BATCH = 5000

# The cursor Redis returns at the end of the pending list, and the one that
# starts a fresh sweep from the front.
SWEEP_START = "0-0"

# The narrowest and widest wait between sweeps, as a fraction of the interval.
# The jitter keeps a fleet that started together from sweeping in lockstep.
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

    Each XCLAIM asks for JUSTID, so Redis returns only the claimed ids, not the
    message bodies the worker already has and would discard.  The ids go out in
    chunks of at most ``LEASE_RENEWAL_BATCH``, so no single command names every
    held message and blocks Redis while it runs.

    A chunk that Redis refuses is logged and passed over, so the rest of the
    worker's messages still have their leases renewed on this pass.
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
    """A worker's bounded, cursor-kept claim on abandoned messages."""

    def __init__(
        self,
        docket: Docket,
        *,
        worker_name: str,
        redelivery_timeout: timedelta,
        message_batch: int,
    ) -> None:
        self.docket = docket
        self.worker_name = worker_name
        self.message_batch = message_batch
        self.min_idle_time = int(redelivery_timeout.total_seconds() * 1000)
        self.interval = redelivery_timeout.total_seconds() / 4
        self.start_id = SWEEP_START
        # Due immediately, so a worker that replaces a dead one picks up its
        # messages without waiting a whole interval first.
        self.next_sweep = time.monotonic()

    async def due(self, redis: RedisClient) -> bool:
        """Whether the worker should sweep on this pass.

        A sweep already under way (the cursor is past the front) keeps running
        on every pass so it finishes quickly.  Otherwise the sweep waits for the
        jittered timer, and then for the fleet-wide lease: only the worker that
        takes ``SET {docket}:leases:redelivery-sweep worker NX PX interval``
        sweeps this interval.  A worker that loses the lease arms its next timer
        and waits, rather than spinning.
        """
        if self.start_id != SWEEP_START:
            return True
        if time.monotonic() < self.next_sweep:
            return False
        took_lease = await redis.set(
            self.docket.redelivery_sweep_key,
            self.worker_name,
            nx=True,
            px=int(self.interval * 1000),
        )
        if not took_lease:
            self._rest()
        return bool(took_lease)

    async def claim(self, redis: RedisClient, available_slots: int) -> RedisMessages:
        """Claim the messages that another worker has left idle too long.

        The claim starts where the last one stopped, and asks for no more than
        ``message_batch`` entries however many slots are free.  A docket that no
        worker has bootstrapped yet has no consumer group, so a NOGROUP answer
        means creating the group and claiming again.

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
                count=min(available_slots, self.message_batch),
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
