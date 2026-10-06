"""Waits on the waiter streams that concurrency limits park tasks on."""

import asyncio
import time

from docket import Docket


async def wait_for_xlen(docket: Docket, key: str, target: int) -> None:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        async with docket.redis() as redis:
            size = await redis.xlen(key)
        if size == target:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(  # pragma: no cover
        f"XLEN({key}) did not reach {target} in time"
    )
