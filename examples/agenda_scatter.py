#!/usr/bin/env python
"""
Example demonstrating the Agenda scatter functionality for rate-limited workloads.

This example shows a real-world scenario: sending bulk notifications while respecting
rate limits to avoid overwhelming your notification service or triggering spam filters.

Without scatter: All 26 notifications would try to send immediately, potentially:
- Overwhelming your notification service
- Triggering rate limits or spam detection
- Creating a poor user experience with delayed/failed sends

With scatter: Notifications are distributed evenly over time, respecting limits.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from docket import Agenda, CurrentExecution, Docket, Execution, Worker

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def send_notification(
    user: str, message: str, execution: Execution = CurrentExecution()
) -> None:
    """Send a notification to a user."""
    delay = (execution.when - datetime.now(timezone.utc)).total_seconds()
    if delay > 0.1:
        logger.info(f"📅 Notification for {user} scheduled {delay:.1f}s from now")
    else:
        logger.info(f"📧 Sending to {user}: '{message}'")
        # Simulate API call to notification service
        await asyncio.sleep(0.2)
        logger.info(f"✓ Delivered to {user}")


async def main() -> None:
    """Demonstrate scatter for rate-limited notification sending."""

    async with Docket(name="notification-scatter") as docket:
        docket.register(send_notification)

        logger.info("=== Bulk Notification Campaign ===")
        logger.info("Scenario: Alert 26 users about a flash sale")
        logger.info("Constraint: Notification service allows max 30 messages/minute")
        logger.info("Strategy: Scatter over 60 seconds (~1 message every 2.3 seconds)")
        logger.info("")

        # Build the list of users to notify (e.g., from a database query)
        users = [
            "alice@example.com",
            "bob@example.com",
            "charlie@example.com",
            "diana@example.com",
            "eve@example.com",
            "frank@example.com",
            "grace@example.com",
            "henry@example.com",
            "iris@example.com",
            "jack@example.com",
            "kate@example.com",
            "liam@example.com",
            "maya@example.com",
            "noah@example.com",
            "olivia@example.com",
            "peter@example.com",
            "quinn@example.com",
            "ruby@example.com",
            "sam@example.com",
            "tara@example.com",
            "uma@example.com",
            "victor@example.com",
            "wendy@example.com",
            "xavier@example.com",
            "yara@example.com",
            "zoe@example.com",
        ]

        agenda = Agenda()

        # Queue all notifications
        logger.info(f"📋 Preparing notifications for {len(users)} users...")
        for user in users:
            agenda.add(send_notification)(user, "Flash Sale: 50% off for next hour!")

        # Scatter over 60 seconds to respect rate limit
        logger.info("🎯 Scattering notifications over 60 seconds...")
        logger.info("")

        executions = await agenda.scatter(
            docket,
            over=timedelta(seconds=60),
            jitter=timedelta(seconds=0.5),  # Small jitter for natural spacing
        )

        # Show the distribution preview
        first_three = executions[:3]
        last_three = executions[-3:]
        for i, exec in enumerate(first_three, 1):
            delay = (exec.when - datetime.now(timezone.utc)).total_seconds()
            logger.info(f"   Message #{i} scheduled for +{delay:.1f}s")
        logger.info(f"   ... {len(executions) - 6} more evenly distributed ...")
        for i, exec in enumerate(last_three, len(executions) - 2):
            delay = (exec.when - datetime.now(timezone.utc)).total_seconds()
            logger.info(f"   Message #{i} scheduled for +{delay:.1f}s")
        logger.info("")

        # Run worker to process the scattered notifications
        logger.info("🚀 Starting notification sender...")
        logger.info("   Watch how notifications flow steadily, not in a flood!")
        logger.info("")

        start_time = datetime.now(timezone.utc)
        async with Worker(docket, concurrency=2) as worker:
            await worker.run_until_finished()

        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info("")
        logger.info(f"✅ All {len(users)} notifications sent in {elapsed:.1f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
