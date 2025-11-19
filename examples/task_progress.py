"""Example demonstrating task progress tracking and real-time monitoring.

This example shows how to:
- Report progress from within a task using Progress
- Track progress with current value, total, and status messages
- Monitor task progress in real-time using the 'docket watch' command
- Schedule tasks for future execution

Key Concepts:
- Progress: Tracks task progress (current/total) and status messages
- Progress dependency: Injected into tasks via Progress() default parameter
- Real-time monitoring: Use 'docket watch' CLI to monitor running tasks
- State tracking: Tasks transition through SCHEDULED → QUEUED → RUNNING → COMPLETED

Run this example with `uv run -m examples.task_progress` and use the printed 'docket watch' command to see live progress updates.
"""

from datetime import datetime, timedelta, timezone
from docket import Docket, Progress, Worker
import asyncio
import rich.console

from .common import run_redis


async def long_task(progress: Progress = Progress()) -> None:
    """A long-running task that reports progress as it executes.

    This demonstrates the key progress tracking patterns:
    - Progress dependency injection via Progress() default parameter
    - Incremental progress updates with increment()
    - Status messages with set_message()

    The Progress object has a default total of 100, so we don't need
    to call set_total() in this example. The progress automatically increments
    from 0 to 100.

    Args:
        progress: Injected Progress tracker (automatically provided by Docket)

    Pattern for your own tasks:
        1. Add progress parameter with Progress() default
        2. Call increment() as work progresses (or set_total + increment)
        3. Optionally set_message() to show current status
        4. Monitor with: docket watch --url <redis_url> --docket <name> <task_key>
    """
    # Simulate 100 steps of work, each taking 1 second
    for i in range(1, 101):
        await asyncio.sleep(1)  # Simulate work being done

        # Increment progress by 1 (tracks that one more unit is complete)
        await progress.increment()

        # Update status message every 10 items for demonstration
        if i % 10 == 0:
            await progress.set_message(f"{i} splines retriculated")


# Export tasks for docket CLI to discover
tasks = [long_task]

# Console for printing user-friendly messages
console = rich.console.Console()


async def main():
    """Run the progress tracking example.

    This function demonstrates the complete lifecycle:
    1. Start a Redis container for testing
    2. Create a Docket (task queue)
    3. Start a Worker (executes tasks)
    4. Register and schedule a task
    5. Monitor progress with the 'docket watch' command

    The task is scheduled 20 seconds in the future to give you time to
    run the watch command and see the task transition through states:
    SCHEDULED → QUEUED → RUNNING → COMPLETED
    """
    # Start a temporary Redis container for this example
    # In production, you'd connect to your existing Redis instance
    async with run_redis("7.4.2") as redis_url:
        # Create a Docket connected to Redis
        async with Docket(name="task-progress", url=redis_url) as docket:
            # Start a Worker to execute tasks from the docket
            async with Worker(docket, name="task-progress-worker") as worker:
                # Register the task so the worker knows how to execute it
                docket.register(long_task)

                # Schedule the task to run 20 seconds from now
                # This gives you time to run the watch command before it starts
                in_twenty_seconds = datetime.now(timezone.utc) + timedelta(seconds=20)
                execution = await docket.add(
                    long_task, key="long-task", when=in_twenty_seconds
                )()

                # Print instructions for monitoring
                console.print(f"Execution {execution.key} started!")
                console.print(
                    f"Run [blue]docket watch --url {redis_url} --docket {docket.name} {execution.key}[/blue] to see the progress!"
                )

                # Run the worker until all tasks complete
                # The worker will wait for the scheduled time, then execute the task
                await worker.run_until_finished()


if __name__ == "__main__":
    asyncio.run(main())
