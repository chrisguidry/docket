#!/usr/bin/env python
"""
Example demonstrating the Agenda scatter functionality.

This example shows how to use Agenda to distribute tasks evenly over a time period,
which is useful for "find-and-flood" workloads where you want to avoid scheduling
all work immediately.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from docket import Agenda, CurrentExecution, Docket, Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_item(
    item_id: int, execution: CurrentExecution = CurrentExecution()
) -> None:
    """Process a single item from a batch."""
    logger.info(
        f"Processing item {item_id} at {datetime.now(timezone.utc).isoformat()} "
        f"(scheduled for {execution.when.isoformat()})"
    )
    # Simulate some work
    await asyncio.sleep(0.1)
    logger.info(f"Completed processing item {item_id}")


async def send_notification(user_id: str, message: str) -> None:
    """Send a notification to a user."""
    logger.info(f"Sending notification to user {user_id}: {message}")
    # Simulate sending notification
    await asyncio.sleep(0.1)
    logger.info(f"Notification sent to user {user_id}")


async def main() -> None:
    """Demonstrate agenda scattering."""

    # Initialize docket with Redis
    async with Docket(name="agenda-example") as docket:
        # Register our task functions
        docket.register(process_item)
        docket.register(send_notification)

        # Example 1: Scatter items over 30 seconds
        logger.info("=== Example 1: Basic Scattering ===")
        agenda = Agenda()

        # Add 5 items to process
        for i in range(1, 6):
            agenda.add(process_item)(i)

        # Scatter them over 10 seconds
        executions = await agenda.scatter(docket, over=timedelta(seconds=10))
        logger.info(f"Scheduled {len(executions)} tasks:")
        for i, exec in enumerate(executions):
            logger.info(f"  Task {i + 1} scheduled for {exec.when.isoformat()}")

        # Example 2: Scatter with jitter to prevent thundering herd
        logger.info("\n=== Example 2: Scattering with Jitter ===")
        agenda2 = Agenda()

        # Add notifications to send
        users = ["alice", "bob", "charlie", "dave", "eve"]
        for user in users:
            agenda2.add(send_notification)(user, "System maintenance in 1 hour")

        # Scatter over 10 seconds with ±2 second jitter
        executions2 = await agenda2.scatter(
            docket, over=timedelta(seconds=10), jitter=timedelta(seconds=2)
        )
        logger.info(f"Scheduled {len(executions2)} notifications with jitter:")
        for i, exec in enumerate(executions2):
            logger.info(f"  Notification {i + 1} scheduled for {exec.when.isoformat()}")

        # Example 3: Future scatter window
        logger.info("\n=== Example 3: Future Scatter Window ===")
        agenda3 = Agenda()

        # Add mixed task types
        agenda3.add(process_item)(100)
        agenda3.add(send_notification)("admin", "Batch processing started")
        agenda3.add(process_item)(101)
        agenda3.add(send_notification)("admin", "Batch processing halfway")
        agenda3.add(process_item)(102)

        # Schedule to run 15 seconds from now, scattered over 30 seconds
        start_time = datetime.now(timezone.utc) + timedelta(seconds=15)
        executions3 = await agenda3.scatter(
            docket, start=start_time, over=timedelta(seconds=30)
        )
        logger.info(
            f"Scheduled {len(executions3)} mixed tasks starting at {start_time.isoformat()}:"
        )
        for i, exec in enumerate(executions3):
            time_from_now = (exec.when - datetime.now(timezone.utc)).total_seconds()
            logger.info(
                f"  Task {i + 1} scheduled for {exec.when.isoformat()} ({time_from_now:.1f}s from now)"
            )

        # Example 4: Minimal scatter window
        logger.info("\n=== Example 4: Minimal Scatter Window ===")
        agenda4 = Agenda()

        for i in range(200, 203):
            agenda4.add(process_item)(i)

        # Use a very small window for near-immediate scheduling
        executions4 = await agenda4.scatter(docket, over=timedelta(seconds=1))
        logger.info(f"Scheduled {len(executions4)} tasks over 1 second")

        # Run a worker to process the tasks
        logger.info("\n=== Starting Worker ===")
        async with Worker(docket, concurrency=3) as worker:
            # Run for a limited time to demonstrate
            try:
                await asyncio.wait_for(
                    worker.run_until_finished(),
                    timeout=90,  # Run for up to 1.5 minutes
                )
            except asyncio.TimeoutError:
                logger.info("Worker timeout reached, stopping...")

        logger.info("Example completed!")


if __name__ == "__main__":
    asyncio.run(main())
