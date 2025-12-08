"""Signal handling integration tests for docket workers.

This module tests that workers gracefully drain in-flight tasks when receiving
SIGINT or SIGTERM signals. This is critical for Kubernetes deployments where
SIGTERM is sent during pod termination.

Run via: python -m chaos.signals
"""

import asyncio
import logging
import os
import signal
import sys
from asyncio.subprocess import Process
from uuid import uuid4

from docket import CurrentDocket, Docket
from docket.execution import ExecutionState

from .redis import run_redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("chaos.signals")

# Channel name is passed via environment variable to tasks
CHANNEL_ENV_VAR = "SIGNAL_TEST_CHANNEL"


# Task for signal testing - signals start/completion via Redis pub/sub
async def signal_test_task(
    task_id: str,
    duration: float = 5.0,
    docket: Docket = CurrentDocket(),
) -> None:
    """Task that signals start via Redis pub/sub, sleeps, then signals completion."""
    channel = os.environ.get(CHANNEL_ENV_VAR, "signal-test:events")

    async with docket.redis() as redis:
        await redis.publish(channel, f"started:{task_id}")  # type: ignore[reportUnknownMemberType]
        logger.info("Task %s started", task_id)

    await asyncio.sleep(duration)

    async with docket.redis() as redis:
        await redis.publish(channel, f"completed:{task_id}")  # type: ignore[reportUnknownMemberType]
        logger.info("Task %s completed", task_id)


signal_test_tasks = [signal_test_task]


async def spawn_worker(
    docket_name: str,
    redis_url: str,
    channel: str,
    concurrency: int = 2,
) -> Process:
    """Spawn a worker subprocess."""
    env = {**os.environ, "PYTHONUNBUFFERED": "1", CHANNEL_ENV_VAR: channel}
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "docket",
        "worker",
        "--docket",
        docket_name,
        "--url",
        redis_url,
        "--tasks",
        "chaos.signals:signal_test_tasks",
        "--concurrency",
        str(concurrency),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )


async def wait_for_tasks_via_pubsub(
    docket: Docket,
    channel: str,
    task_ids: set[str],
    event_type: str,
    timeout: float = 30.0,
) -> bool:
    """Wait for all tasks to publish their event via Redis pub/sub.

    Args:
        docket: Docket instance for Redis connection
        channel: Pub/sub channel to subscribe to
        task_ids: Set of task IDs to wait for
        event_type: Event prefix to match (e.g., "started" or "completed")
        timeout: Maximum time to wait in seconds

    Returns:
        True if all tasks published their event, False on timeout
    """
    remaining = task_ids.copy()

    async with docket.redis() as redis:
        pubsub = redis.pubsub()  # type: ignore[reportUnknownMemberType]
        await pubsub.subscribe(channel)  # type: ignore[reportUnknownMemberType]

        try:
            deadline = asyncio.get_event_loop().time() + timeout

            while remaining:
                time_left = deadline - asyncio.get_event_loop().time()
                if time_left <= 0:
                    logger.error(
                        "Timed out waiting for %s events, missing: %s",
                        event_type,
                        remaining,
                    )
                    return False

                # Wait for next message with remaining timeout
                try:
                    message: (  # type: ignore[reportUnknownVariableType]
                        dict[str, bytes | str | None] | None
                    ) = await asyncio.wait_for(
                        pubsub.get_message(  # type: ignore[reportUnknownMemberType]
                            ignore_subscribe_messages=True, timeout=1.0
                        ),
                        timeout=min(time_left, 2.0),
                    )
                except asyncio.TimeoutError:
                    continue

                if message is None:
                    continue

                data: bytes | str | None = message.get("data")
                if not isinstance(data, bytes):
                    continue

                decoded = data.decode()
                if decoded.startswith(f"{event_type}:"):
                    task_id = decoded[len(event_type) + 1 :]
                    if task_id in remaining:
                        remaining.discard(task_id)
                        logger.debug(
                            "Task %s %s (%d remaining)",
                            task_id,
                            event_type,
                            len(remaining),
                        )

            logger.info("All %d tasks have %s", len(task_ids), event_type)
            return True

        finally:
            await pubsub.unsubscribe(channel)  # type: ignore[reportUnknownMemberType]
            await pubsub.aclose()  # type: ignore[reportUnknownMemberType]


async def verify_tasks_completed(
    docket: Docket,
    task_keys: list[str],
) -> tuple[bool, list[str]]:
    """Verify all tasks completed successfully via Redis state.

    Returns:
        Tuple of (all_completed, list of failed task keys)
    """
    failed_keys: list[str] = []

    async with docket.redis() as redis:
        for key in task_keys:
            runs_key = f"{docket.name}:runs:{key}"
            state: bytes | None = await redis.hget(runs_key, "state")  # type: ignore[reportUnknownMemberType]

            if state is None:
                logger.error("Task %s has no state in Redis", key)
                failed_keys.append(key)
            elif state.decode() != ExecutionState.COMPLETED.value:
                logger.error(
                    "Task %s has state %s, expected %s",
                    key,
                    state.decode(),
                    ExecutionState.COMPLETED.value,
                )
                failed_keys.append(key)

    return len(failed_keys) == 0, failed_keys


async def run_signal_test(
    sig: signal.Signals,
    redis_url: str,
    num_workers: int = 2,
    tasks_per_worker: int = 2,
    task_duration: float = 5.0,
) -> tuple[bool, str]:
    """Run a single signal handling test.

    Args:
        sig: Signal to send (SIGTERM or SIGINT)
        redis_url: Redis connection URL
        num_workers: Number of worker processes to spawn
        tasks_per_worker: Number of tasks per worker (via concurrency)
        task_duration: How long each task runs (seconds)

    Returns:
        Tuple of (success, message)
    """
    sig_name = sig.name
    total_tasks = num_workers * tasks_per_worker
    docket_name = f"signal-test-{uuid4()}"
    channel = f"signal-test:events:{uuid4()}"

    logger.info(
        "Starting %s test with %d workers and %d tasks",
        sig_name,
        num_workers,
        total_tasks,
    )

    async with Docket(name=docket_name, url=redis_url) as docket:
        # Generate task IDs and schedule tasks
        task_ids = [f"task-{i}-{uuid4()}" for i in range(total_tasks)]
        task_keys: list[str] = []

        # Start listening for events before spawning workers
        started_future: asyncio.Task[bool] = asyncio.create_task(
            wait_for_tasks_via_pubsub(
                docket, channel, set(task_ids), "started", timeout=30.0
            )
        )

        # Give the subscription time to establish
        await asyncio.sleep(0.1)

        for task_id in task_ids:
            execution = await docket.add(signal_test_task)(
                task_id=task_id,
                duration=task_duration,
            )
            task_keys.append(execution.key)
            logger.info("Scheduled task %s with key %s", task_id, execution.key)

        # Spawn workers
        workers: list[Process] = []
        for i in range(num_workers):
            worker = await spawn_worker(
                docket_name=docket_name,
                redis_url=redis_url,
                channel=channel,
                concurrency=tasks_per_worker,
            )
            workers.append(worker)
            logger.info("Spawned worker %d with PID %s", i, worker.pid)

        # Wait for all tasks to start via pub/sub
        if not await started_future:
            # Kill workers and fail
            for worker in workers:
                if worker.returncode is None:
                    worker.kill()
            return False, "Tasks did not start within timeout"

        # Small delay to ensure tasks are mid-execution
        await asyncio.sleep(0.5)

        # Send signal to all workers
        logger.info("Sending %s to all workers", sig_name)
        for i, worker in enumerate(workers):
            if worker.returncode is None:
                assert worker.pid is not None
                os.kill(worker.pid, sig)
                logger.info("Sent %s to worker %d (PID %d)", sig_name, i, worker.pid)

        # Wait for workers to exit gracefully
        shutdown_timeout = task_duration + 10.0
        try:
            results = await asyncio.wait_for(
                asyncio.gather(
                    *[worker.communicate() for worker in workers],
                    return_exceptions=True,
                ),
                timeout=shutdown_timeout,
            )

            # Log worker outputs
            for i, result in enumerate(results):
                if isinstance(result, tuple):
                    stdout, stderr = result
                    combined = stdout.decode() + stderr.decode()
                    if combined.strip():
                        logger.debug("Worker %d output:\n%s", i, combined)
                else:
                    logger.error("Worker %d failed: %s", i, result)

        except asyncio.TimeoutError:
            logger.error("Workers did not exit within %s seconds", shutdown_timeout)
            for worker in workers:
                if worker.returncode is None:
                    worker.kill()
            return False, "Workers did not exit within timeout"

        # Verify all tasks completed via Redis state (check this BEFORE exit code
        # so we can see if tasks drained even if exit code is wrong)
        state_ok, failed_keys = await verify_tasks_completed(docket, task_keys)
        if not state_ok:
            return False, f"Tasks did not complete: {failed_keys}"

        logger.info("All %d tasks completed", total_tasks)

        # Verify all workers exited with code 0
        for i, worker in enumerate(workers):
            if worker.returncode != 0:
                return False, f"Worker {i} exited with code {worker.returncode}"

        logger.info("All workers exited with code 0")

        logger.info("%s test passed - all %d tasks completed", sig_name, total_tasks)
        return True, f"{sig_name} test passed"


async def main() -> None:
    """Run signal handling tests for both SIGTERM and SIGINT."""
    async with run_redis("7.4.2") as (redis_url, _):
        logger.info("Redis running at %s", redis_url)

        # Test SIGTERM
        success, message = await run_signal_test(signal.SIGTERM, redis_url)
        if not success:
            logger.error("SIGTERM test failed: %s", message)
            sys.exit(1)
        logger.info("SIGTERM test passed")

        # Test SIGINT
        success, message = await run_signal_test(signal.SIGINT, redis_url)
        if not success:
            logger.error("SIGINT test failed: %s", message)
            sys.exit(1)
        logger.info("SIGINT test passed")

    logger.info("All signal tests passed!")


if __name__ == "__main__":
    asyncio.run(main())
