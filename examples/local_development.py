#!/usr/bin/env python3
"""
Example: Local Development Without Redis

This example demonstrates using Docket with the in-memory backend for
local development, prototyping, or situations where you don't have Redis
available but still want to use Docket's task scheduling features.

Use cases:
- Local development on a laptop without Docker/Redis
- Quick prototyping and experimentation
- Educational/tutorial environments
- Desktop applications that need background tasks
- CI/CD environments without Redis containers
- Single-process utilities that benefit from task scheduling

Limitations:
- Single process only (no distributed workers)
- Data stored in memory (lost on restart)
- Performance may differ from real Redis

To run:
    uv run examples/local_development.py
"""

import asyncio
from datetime import datetime, timedelta, timezone

from docket import Docket, Worker
from docket.dependencies import Perpetual, Retry


# Example 1: Simple immediate task
async def process_file(filename: str) -> None:
    print(f"📄 Processing file: {filename}")
    await asyncio.sleep(0.5)  # Simulate work
    print(f"✅ Completed: {filename}")


# Example 2: Scheduled task with retry
async def backup_data(target: str, retry: Retry = Retry(attempts=3)) -> None:
    print(f"💾 Backing up to: {target}")
    await asyncio.sleep(0.3)
    print(f"✅ Backup complete: {target}")


# Example 3: Periodic background task
async def health_check(
    perpetual: Perpetual = Perpetual(every=timedelta(seconds=2), automatic=True),
) -> None:
    print(f"🏥 Health check at {datetime.now(timezone.utc).strftime('%H:%M:%S')}")


async def main():
    print("🚀 Starting Docket with in-memory backend (no Redis required!)\n")

    # Use memory:// URL for in-memory operation
    async with Docket(name="local-dev", url="memory://local-dev") as docket:
        # Register tasks
        docket.register(process_file)
        docket.register(backup_data)
        docket.register(health_check)

        # Schedule some immediate tasks
        print("Scheduling immediate tasks...")
        await docket.add(process_file)("report.pdf")
        await docket.add(process_file)("data.csv")
        await docket.add(process_file)("config.json")

        # Schedule a future task
        in_two_seconds = datetime.now(timezone.utc) + timedelta(seconds=2)
        print("Scheduling backup for 2 seconds from now...")
        await docket.add(backup_data, when=in_two_seconds)("/tmp/backup")

        # The periodic task will be auto-scheduled by the worker
        print("Setting up periodic health check...\n")

        # Run worker to process tasks
        print("=" * 60)
        async with Worker(docket, concurrency=2) as worker:
            # Run for 6 seconds to see the periodic task execute a few times
            print("Worker running for 6 seconds...\n")
            try:
                await asyncio.wait_for(worker.run_forever(), timeout=6.0)
            except asyncio.TimeoutError:
                print("\n" + "=" * 60)
                print("✨ Demo complete!")

        # Show final state
        snapshot = await docket.snapshot()
        print("\nFinal state:")
        print(f"  Snapshot time: {snapshot.taken.strftime('%H:%M:%S')}")
        print(f"  Future tasks: {len(snapshot.future)}")
        print(f"  Running tasks: {len(snapshot.running)}")


if __name__ == "__main__":
    asyncio.run(main())
