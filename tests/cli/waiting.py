"""Test utilities for waiting on Redis state conditions."""

import asyncio
import time
from typing import cast

from docket import Docket
from docket.execution import ExecutionState


async def wait_for_progress_data(
    docket: Docket,
    key: str,
    *,
    min_current: int = 1,
    min_total: int = 1,
    timeout: float = 2.0,
    interval: float = 0.01,
) -> tuple[int, int]:
    """Wait for progress data to appear in Redis with specified values.

    Args:
        docket: Docket instance
        key: Task key
        min_current: Minimum current value to wait for
        min_total: Minimum total value to wait for
        timeout: Maximum time to wait in seconds
        interval: Sleep interval between checks in seconds

    Returns:
        Tuple of (current, total) values once condition is met

    Raises:
        TimeoutError: If condition not met within timeout
    """
    start_time = time.monotonic()
    progress_key = docket.key(f"progress:{key}")

    while time.monotonic() - start_time < timeout:
        async with docket.redis() as redis:
            data = await redis.hgetall(progress_key)  # type: ignore[misc]
            if data:  # pragma: no branch
                current = int(cast(bytes, data.get(b"current", b"0")))  # type: ignore[misc]
                total = int(cast(bytes, data.get(b"total", b"100")))  # type: ignore[misc]

                if current >= min_current and total >= min_total:  # pragma: no branch
                    return (current, total)

        await asyncio.sleep(interval)

    raise TimeoutError(  # pragma: no cover
        f"Progress data did not reach min_current={min_current}, min_total={min_total} "
        f"within {timeout}s for task {key}"
    )


async def wait_for_execution_state(
    docket: Docket,
    key: str,
    state: ExecutionState,
    *,
    timeout: float = 2.0,
    interval: float = 0.01,
) -> None:
    """Wait for execution to reach specified state.

    Args:
        docket: Docket instance
        key: Task key
        state: Target execution state
        timeout: Maximum time to wait in seconds
        interval: Sleep interval between checks in seconds

    Raises:
        TimeoutError: If state not reached within timeout
    """
    start_time = time.monotonic()
    execution_key = docket.runs_key(key)

    while time.monotonic() - start_time < timeout:
        async with docket.redis() as redis:
            data = await redis.hgetall(execution_key)  # type: ignore[misc]
            if data:  # pragma: no branch
                state_value = data.get(b"state")  # type: ignore[misc]
                if state_value:  # pragma: no branch
                    current_state = ExecutionState(cast(bytes, state_value).decode())
                    if current_state == state:  # pragma: no branch
                        return

        await asyncio.sleep(interval)

    raise TimeoutError(  # pragma: no cover
        f"Execution did not reach state {state.value} within {timeout}s for task {key}"
    )


async def wait_for_worker_assignment(
    docket: Docket,
    key: str,
    *,
    timeout: float = 2.0,
    interval: float = 0.01,
) -> str:
    """Wait for a worker to be assigned to the execution.

    Args:
        docket: Docket instance
        key: Task key
        timeout: Maximum time to wait in seconds
        interval: Sleep interval between checks in seconds

    Returns:
        Worker name once assigned

    Raises:
        TimeoutError: If no worker assigned within timeout
    """
    start_time = time.monotonic()
    execution_key = docket.runs_key(key)

    while time.monotonic() - start_time < timeout:
        async with docket.redis() as redis:
            data = await redis.hgetall(execution_key)  # type: ignore[misc]
            if data:  # pragma: no branch
                worker = data.get(b"worker")  # type: ignore[misc]
                if worker:  # pragma: no branch
                    return cast(bytes, worker).decode()

        await asyncio.sleep(interval)

    raise TimeoutError(  # pragma: no cover
        f"No worker was assigned to task {key} within {timeout}s"
    )


async def wait_for_watch_subscribed(
    docket: Docket,
    key: str,
    *,
    timeout: float = 3.0,
    interval: float = 0.01,
) -> None:
    """Wait for watch command to subscribe to state channel.

    Uses Redis PUBSUB NUMSUB to detect when watch has subscribed.
    This ensures watch won't miss state events published after subscription.

    Args:
        docket: Docket instance
        key: Task key
        timeout: Maximum time to wait in seconds
        interval: Sleep interval between checks in seconds

    Raises:
        TimeoutError: If watch doesn't subscribe within timeout
    """
    from redis.asyncio import ConnectionPool, Redis
    from redis.asyncio.cluster import RedisCluster

    start_time = time.monotonic()
    state_channel = docket.key(f"state:{key}")

    while time.monotonic() - start_time < timeout:
        async with docket.redis() as redis:
            # RedisCluster doesn't have pubsub_numsub, so we need to use a node client
            if isinstance(redis, RedisCluster):  # pragma: no cover
                # Get any node and check pubsub_numsub on it
                node = redis.get_default_node()
                pool = ConnectionPool(host=node.host, port=int(node.port))
                node_client = Redis(connection_pool=pool)
                try:
                    result = await node_client.pubsub_numsub(state_channel)  # type: ignore[reportUnknownMemberType]
                finally:
                    await node_client.aclose()
                    await pool.aclose()
            else:  # pragma: no cover
                result = await redis.pubsub_numsub(state_channel)  # type: ignore[misc]
            # Returns list of tuples: [(channel_bytes, count), ...]
            for channel, count in result:  # type: ignore[misc]
                if isinstance(channel, bytes):  # pragma: no branch
                    channel = channel.decode()
                if channel == state_channel and count > 0:  # pragma: no branch
                    return

        await asyncio.sleep(interval)

    raise TimeoutError(  # pragma: no cover
        f"Watch command did not subscribe to {state_channel} within {timeout}s"
    )
