"""Key leak detection for preventing Redis memory leaks in tests."""

from fnmatch import fnmatch
from typing import Iterable

from redis.asyncio import Redis

from docket import Docket


async def count_redis_keys_by_type(redis: Redis, prefix: str) -> dict[str, int]:
    """Count Redis keys by type for a given prefix."""
    pattern = f"{prefix}:*"
    keys: Iterable[str] = await redis.keys(pattern)  # type: ignore
    counts: dict[str, int] = {}

    for key in keys:
        key_type = await redis.type(key)
        key_type_str = (
            key_type.decode() if isinstance(key_type, bytes) else str(key_type)
        )
        counts[key_type_str] = counts.get(key_type_str, 0) + 1

    return counts


class KeyCountChecker:
    """Helper to verify Redis key counts remain consistent across operations.

    This class is used by the autouse key_leak_checker fixture to automatically
    verify that no keys without TTL leak during test execution.
    """

    def __init__(self, docket: Docket) -> None:
        self.docket = docket
        self.docket_name = docket.name
        self.redis: Redis | None = None
        self.baseline_counts: dict[str, int] = {}
        self.exemptions: set[str] = set()
        self.pattern_exemptions: set[str] = set()

        # Permanent keys that don't need TTL
        self.permanent_keys = {
            f"{docket.name}:stream",  # Task stream for ready-to-execute tasks
            f"{docket.name}:workers",  # Worker heartbeat tracking
            f"{docket.name}:strikes",  # Strike command stream
            f"{docket.name}:queue",  # Scheduled tasks sorted set
        }
        # Permanent key patterns (using simple prefix matching)
        self.permanent_patterns = [
            f"{docket.name}:worker-tasks:",  # Per-worker task capability sets
            f"{docket.name}:task-workers:",  # Per-task worker index sets
        ]

    def add_exemption(self, key_pattern: str) -> None:
        """Add a key pattern to exempt from leak checking."""
        self.exemptions.add(key_pattern)

    def add_pattern_exemption(self, pattern: str) -> None:
        """Add a wildcard pattern to exempt from leak checking.

        Example: add_pattern_exemption(f"{docket.name}:runs:*")
        """
        self.pattern_exemptions.add(pattern)

    async def capture_baseline(self) -> None:
        """Capture baseline key counts after worker priming."""
        async with self.docket.redis() as redis:
            self.baseline_counts = await count_redis_keys_by_type(
                redis, self.docket_name
            )

    async def verify_remaining_keys_have_ttl(self) -> None:
        """Verify that all remaining keys either have TTL or are explicitly permanent.

        This prevents memory leaks by ensuring that any data keys created during
        operations will eventually expire.

        Keys without TTL are allowed only for tasks that are still scheduled/queued
        (not yet executed). Completed/failed tasks should have TTL set.
        """
        async with self.docket.redis() as redis:
            # Get all keys for this docket (use :* to avoid matching dockets with suffixes)
            pattern = f"{self.docket_name}:*"
            all_keys: list[str] = await redis.keys(pattern)  # type: ignore

            keys_without_ttl: list[str] = []

            for key in all_keys:
                key_str = key.decode() if isinstance(key, bytes) else str(key)

                # Skip explicitly permanent keys
                if key_str in self.permanent_keys:
                    continue

                # Skip permanent key patterns
                if any(key_str.startswith(pat) for pat in self.permanent_patterns):
                    continue

                # Skip exempted keys
                if key_str in self.exemptions:
                    continue

                # Skip pattern-exempted keys
                if self.pattern_exemptions:
                    if any(fnmatch(key_str, pat) for pat in self.pattern_exemptions):
                        continue

                # Check TTL (-1 means no expiry, -2 means key doesn't exist)
                ttl = await redis.ttl(key_str)
                if ttl == -1:
                    # Key has no TTL - check if it's for a scheduled task
                    is_allowed = await self._is_scheduled_task_key(key_str, redis)
                    if not is_allowed:
                        keys_without_ttl.append(key_str)

            assert not keys_without_ttl, (
                f"Memory leak detected: The following keys have no TTL "
                f"and will never expire: {keys_without_ttl}. All data keys should have TTL set "
                f"to prevent permanent memory usage. Keys without TTL are only allowed for "
                f"tasks that are still scheduled/queued (not yet executed)."
            )

    async def _is_scheduled_task_key(self, key_str: str, redis: Redis) -> bool:
        """Check if a key without TTL is for a task that's still scheduled/queued.

        Args:
            key_str: The Redis key to check

        Returns:
            True if the key is for a task that's still scheduled (allowed to not have TTL),
            False if it's a completed/failed task (should have TTL).
        """
        # Extract task key from the Redis key
        # Patterns: {docket}:{task_key} (parked data) or {docket}:runs:{task_key}
        prefix = f"{self.docket_name}:"
        if not key_str.startswith(prefix):  # pragma: no cover
            return False

        suffix = key_str[len(prefix) :]

        # Handle runs keys
        if suffix.startswith("runs:"):
            task_key = suffix[5:]  # Remove "runs:" prefix
            runs_key = key_str
        else:
            # Parked task data key - the suffix is the task key
            task_key = suffix
            runs_key = f"{self.docket_name}:runs:{task_key}"

        # Check the state in the runs hash
        state: str | None = await redis.hget(runs_key, "state")  # type: ignore[assignment]
        if state is None:
            # No runs hash - this is not a valid scheduled task
            # Real scheduled tasks always have a runs hash with state
            return False

        # Decode if bytes
        if isinstance(state, bytes):  # pragma: no cover
            state = state.decode()

        # Tasks that completed/failed should have TTL
        completed_states = {"completed", "failed", "cancelled"}
        if state in completed_states:  # pragma: no cover
            return False

        # For scheduled/queued/running states, verify the task is actually present
        # in the queue or stream (catches cases where clear() left stale runs hashes)
        return await self._task_is_actually_scheduled(task_key, redis)

    async def _task_is_actually_scheduled(self, task_key: str, redis: Redis) -> bool:
        """Check if a task is actually present in the queue or stream.

        Args:
            task_key: The task key to check

        Returns:
            True if the task is in the queue (scheduled) or stream (queued/running)
        """
        queue_key = f"{self.docket_name}:queue"

        # Check if task is in the scheduled queue
        score = await redis.zscore(queue_key, task_key)
        if score is not None:
            return True

        # For immediate tasks in the stream, check if there's a stream_id in runs hash
        # or if parked data exists
        runs_key = f"{self.docket_name}:runs:{task_key}"
        stream_id: str | None = await redis.hget(runs_key, "stream_id")  # type: ignore[assignment]
        if stream_id is not None:
            # Task is in the stream (immediate task)
            return True

        # Check if parked data exists (for scheduled tasks not yet in stream)
        parked_key = f"{self.docket_name}:{task_key}"
        parked_exists = await redis.exists(parked_key)

        return bool(parked_exists)
