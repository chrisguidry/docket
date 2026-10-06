"""Waits on the waiter streams that concurrency limits park tasks on."""

import asyncio

from docket import Docket


async def wait_for_xlen(docket: Docket, key: str, target: int) -> None:
    # Sleeping before each read, not after a failed one, runs every line on
    # every call, so coverage does not depend on how fast the stream fills.
    # The loop-again branch happens only when the stream is behind on a read,
    # so it is excluded from branch coverage.
    # asyncio.timeout needs Python 3.11, so wait_for bounds the loop instead.
    async def poll() -> None:
        while True:
            await asyncio.sleep(0.01)
            async with docket.redis() as redis:
                if await redis.xlen(key) == target:  # pragma: no branch
                    return

    await asyncio.wait_for(poll(), timeout=2.0)
