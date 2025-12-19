#!/usr/bin/env python3
"""
Example: Self-Contained Service with In-Memory Backend

Demonstrates running a complete docket service in a single process using
the memory:// backend. This is useful for simple services that need
background task scheduling but don't require distributed workers.

The service has:
- An automatic perpetual task that runs every few seconds
- That perpetual task spawns additional work tasks

To run this example:
    uv run examples/self_contained_service.py

In your own project, you can use the CLI:
    docket worker --url memory:// --docket my-service --tasks myapp.tasks:tasks
"""

import asyncio
import random
from datetime import timedelta

from docket import Docket, Worker
from docket.dependencies import CurrentDocket, Perpetual


async def do_work(item: str) -> None:
    """A simple work task spawned by the monitor."""
    print(f"  Processing: {item}")
    await asyncio.sleep(0.1)


async def monitor(
    docket: Docket = CurrentDocket(),
    perpetual: Perpetual = Perpetual(every=timedelta(seconds=2), automatic=True),
) -> None:
    """Periodic task that spawns work items."""
    count = random.randint(1, 3)
    print(f"Monitor tick - spawning {count} work items")
    for i in range(count):
        await docket.add(do_work)(f"item-{random.randint(1000, 9999)}")


tasks = [monitor, do_work]


async def main() -> None:
    print("Starting self-contained service (memory:// backend)\n")

    async with Docket(
        name="self-contained", url="memory://", execution_ttl=timedelta(0)
    ) as docket:
        for task in tasks:
            docket.register(task)

        async with Worker(docket) as worker:
            await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
