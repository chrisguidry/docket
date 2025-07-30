#!/usr/bin/env python3
"""
Example demonstrating concurrency control in docket.

This example shows how to use ConcurrencyLimit to control the number of
concurrent tasks based on specific argument values.
"""

import asyncio
from datetime import datetime

from docket import ConcurrencyLimit, Docket, Worker
from common import run_redis


async def process_customer_data(
    customer_id: int,
    data: str,
    concurrency: ConcurrencyLimit = ConcurrencyLimit("customer_id", max_concurrent=1),
):
    """Process customer data - only one task per customer_id can run at a time."""
    print(
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Started processing customer {customer_id}: {data}"
    )

    # Simulate some work
    await asyncio.sleep(0.5)

    print(
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Finished processing customer {customer_id}: {data}"
    )


async def backup_database(
    db_name: str,
    table: str,
    concurrency: ConcurrencyLimit = ConcurrencyLimit("db_name", max_concurrent=2),
):
    """Backup database tables - max 2 concurrent backups per database."""
    print(
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Started backing up {db_name}.{table}"
    )

    # Simulate backup work
    await asyncio.sleep(0.3)

    print(
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Finished backing up {db_name}.{table}"
    )


async def regular_task(task_id: int):
    """Regular task without concurrency limits."""
    print(
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Regular task {task_id} started"
    )
    await asyncio.sleep(0.1)
    print(
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Regular task {task_id} finished"
    )


async def main():
    async with run_redis("7.4.2") as redis_url:
        async with Docket(name="concurrency-demo", url=redis_url) as docket:
            # Register tasks
            docket.register(process_customer_data)
            docket.register(backup_database)
            docket.register(regular_task)

            print("=== Scheduling tasks ===")

            # Schedule multiple tasks for the same customer (will run sequentially)
            print("Scheduling 3 tasks for customer 1001 (will run sequentially)...")
            await docket.add(process_customer_data)(customer_id=1001, data="order-1")
            await docket.add(process_customer_data)(customer_id=1001, data="order-2")
            await docket.add(process_customer_data)(customer_id=1001, data="order-3")

            # Schedule tasks for different customers (will run concurrently)
            print("Scheduling tasks for different customers (will run concurrently)...")
            await docket.add(process_customer_data)(
                customer_id=2001, data="profile-update"
            )
            await docket.add(process_customer_data)(
                customer_id=3001, data="payment-update"
            )

            # Schedule database backups (max 2 concurrent per database)
            print(
                "Scheduling 4 backup tasks for 'users' database (max 2 concurrent)..."
            )
            await docket.add(backup_database)(db_name="users", table="accounts")
            await docket.add(backup_database)(db_name="users", table="profiles")
            await docket.add(backup_database)(db_name="users", table="sessions")
            await docket.add(backup_database)(db_name="users", table="preferences")

            # Schedule backups for different database (will run concurrently with users db)
            print("Scheduling backup for 'orders' database...")
            await docket.add(backup_database)(db_name="orders", table="transactions")

            # Schedule regular tasks (no concurrency limits)
            print("Scheduling regular tasks (no concurrency limits)...")
            for i in range(3):
                await docket.add(regular_task)(task_id=i)

            print("\n=== Starting worker (concurrency=8) ===")
            async with Worker(docket, concurrency=8) as worker:
                await worker.run_until_finished()

            print("\n=== All tasks completed ===")


if __name__ == "__main__":
    asyncio.run(main())
