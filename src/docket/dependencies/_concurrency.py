"""Concurrency limiting dependency."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from .._cancellation import CANCEL_MSG_CLEANUP, cancel_task
from ._base import AdmissionBlocked, Dependency

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover
    from redis.asyncio import Redis

    from ..execution import Execution


# Lease renewal happens this many times per redelivery_timeout period.
# Concurrency slot TTLs are set to this many redelivery_timeout periods.
# A factor of 4 means we renew 4x per period and TTLs last 4 periods.
LEASE_RENEWAL_FACTOR = 4

# Minimum TTL in seconds for Redis keys to avoid immediate expiration when
# redelivery_timeout is very small (e.g., in tests with 200ms timeouts).
MINIMUM_TTL_SECONDS = 1


class ConcurrencyBlocked(AdmissionBlocked):
    """Raised when a task cannot start due to concurrency limits."""

    def __init__(self, execution: Execution, concurrency_key: str, max_concurrent: int):
        self.concurrency_key = concurrency_key
        self.max_concurrent = max_concurrent
        reason = f"concurrency limit ({max_concurrent} max) on {concurrency_key}"
        super().__init__(execution, reason=reason)


class ConcurrencyLimit(Dependency):
    """Configures concurrency limits for task execution.

    Can limit concurrency globally for a task, or per specific argument value.

    Example:

    ```python
    async def expensive_operation(
        concurrency: ConcurrencyLimit = ConcurrencyLimit(max_concurrent=3)
    ) -> None:
        # Only 3 instances of this task will run at a time
        ...

    async def process_customer(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit("customer_id", max_concurrent=1)
    ) -> None:
        # Only one task per customer_id will run at a time
        ...

    async def backup_db(
        db_name: str,
        concurrency: ConcurrencyLimit = ConcurrencyLimit("db_name", max_concurrent=3)
    ) -> None:
        # Only 3 backup tasks per database name will run at a time
        ...
    ```
    """

    single: bool = True

    def __init__(
        self,
        argument_name: str | None = None,
        max_concurrent: int = 1,
        scope: str | None = None,
    ) -> None:
        """
        Args:
            argument_name: The name of the task argument to use for concurrency grouping.
                If None, limits concurrency for the task function itself.
            max_concurrent: Maximum number of concurrent tasks
            scope: Optional scope prefix for Redis keys (defaults to docket name)
        """
        self.argument_name = argument_name
        self.max_concurrent = max_concurrent
        self.scope = scope
        self._concurrency_key: str | None = None
        self._initialized: bool = False
        self._task_key: str | None = None
        self._renewal_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> ConcurrencyLimit:
        from ._functional import _Depends

        execution = self.execution.get()
        docket = self.docket.get()
        worker = self.worker.get()

        # Build concurrency key based on argument_name (if provided) or function name
        scope = self.scope or docket.name
        if self.argument_name is not None:
            # Per-argument concurrency: limit based on specific argument value
            try:
                argument_value = execution.get_argument(self.argument_name)
            except KeyError as e:
                raise ValueError(
                    f"ConcurrencyLimit argument '{self.argument_name}' not found in "
                    f"task arguments. Available: {list(execution.kwargs.keys())}"
                ) from e
            concurrency_key = (
                f"{scope}:concurrency:{self.argument_name}:{argument_value}"
            )
        else:
            # Per-task concurrency: limit based on task function name
            concurrency_key = f"{scope}:concurrency:{execution.function_name}"

        # Create a NEW instance for this specific task execution
        # This is critical because the original instance is shared across all tasks
        # (Python default arguments are evaluated once at function definition time)
        limit = ConcurrencyLimit(self.argument_name, self.max_concurrent, self.scope)
        limit._concurrency_key = concurrency_key
        limit._initialized = True
        limit._task_key = execution.key

        # Acquire slot
        async with docket.redis() as redis:
            acquired = await limit._acquire_slot(
                redis, execution.redelivered, worker.redelivery_timeout
            )
            if not acquired:  # pragma: no branch
                raise ConcurrencyBlocked(
                    execution, concurrency_key, self.max_concurrent
                )

        # Spawn background task for lease renewal
        limit._renewal_task = asyncio.create_task(
            limit._renew_lease_loop(worker.redelivery_timeout),
            name=f"{docket.name} - concurrency lease:{execution.key}",
        )

        # Register cleanup for this new instance with the AsyncExitStack
        # (The original instance's __aexit__ will also be called but does nothing)
        # Order matters (LIFO): release slot first, then cancel renewal task
        stack = _Depends.stack.get()
        stack.push_async_callback(limit._release_slot)
        stack.push_async_callback(cancel_task, limit._renewal_task, CANCEL_MSG_CLEANUP)

        return limit

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: type[BaseException] | None,
    ) -> None:
        # No-op: The original instance (used as default argument) has no state.
        # Actual cleanup is handled by _cleanup() on the per-task instance,
        # which is registered with the AsyncExitStack via push_async_callback.
        pass

    async def _acquire_slot(
        self, redis: Redis, is_redelivery: bool, redelivery_timeout: timedelta
    ) -> bool:
        """Atomically acquire a concurrency slot.

        Uses a Redis sorted set to track concurrency slots per task. Each entry
        is keyed by task_key with the timestamp as the score.

        When XAUTOCLAIM reclaims a message (because the original worker stopped
        renewing its lease), is_redelivery=True signals that slot takeover is safe.
        If the message is NOT a redelivery and a slot already exists, we block to
        prevent duplicate execution.

        Slots are refreshed during lease renewal every redelivery_timeout/4.
        If all slots are full, we scavenge any slot older than redelivery_timeout
        (meaning it hasn't been refreshed and the worker must be dead).
        """
        # Lua script for atomic concurrency slot management.
        # KEYS[1]: concurrency_key (sorted set tracking slots)
        # ARGV[1]: max_concurrent, ARGV[2]: task_key, ARGV[3]: current_time,
        # ARGV[4]: is_redelivery (0/1), ARGV[5]: stale_threshold, ARGV[6]: key_ttl
        acquire_script = redis.register_script(
            """
            local key = KEYS[1]
            local max_concurrent = tonumber(ARGV[1])
            local task_key = ARGV[2]
            local current_time = tonumber(ARGV[3])
            local is_redelivery = tonumber(ARGV[4])
            local stale_threshold = tonumber(ARGV[5])
            local key_ttl = tonumber(ARGV[6])

            -- Check if this task already has a slot (from a previous delivery attempt)
            local slot_time = redis.call('ZSCORE', key, task_key)
            if slot_time then
                slot_time = tonumber(slot_time)
                if is_redelivery == 1 and slot_time <= stale_threshold then
                    -- Redelivery AND slot is stale: original worker stopped renewing,
                    -- safe to take over the slot.
                    redis.call('ZADD', key, current_time, task_key)
                    redis.call('EXPIRE', key, key_ttl)
                    return 1
                else
                    -- Either not a redelivery, or slot is still fresh (original worker
                    -- is just slow, not dead). Don't take over.
                    return 0
                end
            end

            -- No existing slot for this task - check if we can acquire a new one
            if redis.call('ZCARD', key) < max_concurrent then
                redis.call('ZADD', key, current_time, task_key)
                redis.call('EXPIRE', key, key_ttl)
                return 1
            end

            -- All slots are full. Scavenge any stale slot (not refreshed recently).
            -- Slots are refreshed every redelivery_timeout/4, so anything older than
            -- redelivery_timeout hasn't been refreshed and the worker must be dead.
            local stale_slots = redis.call('ZRANGEBYSCORE', key, 0, stale_threshold, 'LIMIT', 0, 1)
            if #stale_slots > 0 then
                redis.call('ZREM', key, stale_slots[1])
                redis.call('ZADD', key, current_time, task_key)
                redis.call('EXPIRE', key, key_ttl)
                return 1
            end

            return 0
            """
        )

        current_time = datetime.now(timezone.utc).timestamp()
        stale_threshold = current_time - redelivery_timeout.total_seconds()
        key_ttl = max(
            MINIMUM_TTL_SECONDS,
            int(redelivery_timeout.total_seconds() * LEASE_RENEWAL_FACTOR),
        )

        result = await acquire_script(
            keys=[self._concurrency_key],
            args=[
                self.max_concurrent,
                self._task_key,
                current_time,
                1 if is_redelivery else 0,
                stale_threshold,
                key_ttl,
            ],
        )

        return bool(result)

    async def _release_slot(self) -> None:
        """Release a concurrency slot when task completes."""
        # Note: only registered as callback for instances with valid keys
        assert self._concurrency_key and self._task_key

        docket = self.docket.get()
        async with docket.redis() as redis:
            # Remove this task from the sorted set and delete the key if empty
            # KEYS[1]: concurrency_key, ARGV[1]: task_key
            release_script = redis.register_script(
                """
                redis.call('ZREM', KEYS[1], ARGV[1])
                if redis.call('ZCARD', KEYS[1]) == 0 then
                    redis.call('DEL', KEYS[1])
                end
                """
            )
            await release_script(keys=[self._concurrency_key], args=[self._task_key])

    async def _renew_lease_loop(self, redelivery_timeout: timedelta) -> None:
        """Periodically refresh slot timestamp to prevent expiration."""
        docket = self.docket.get()
        renewal_interval = redelivery_timeout.total_seconds() / LEASE_RENEWAL_FACTOR
        key_ttl = max(
            MINIMUM_TTL_SECONDS,
            int(redelivery_timeout.total_seconds() * LEASE_RENEWAL_FACTOR),
        )

        while True:
            await asyncio.sleep(renewal_interval)
            try:
                async with docket.redis() as redis:
                    current_time = datetime.now(timezone.utc).timestamp()
                    await redis.zadd(
                        self._concurrency_key,
                        {self._task_key: current_time},  # type: ignore
                    )
                    await redis.expire(self._concurrency_key, key_ttl)  # type: ignore
            except Exception:  # pragma: no cover
                # Lease renewal is best-effort; if it fails, the slot will eventually
                # be scavenged as stale and the task can be redelivered
                logger.warning(
                    "Concurrency lease renewal failed for %s",
                    self._concurrency_key,
                    exc_info=True,
                )

    @property
    def concurrency_key(self) -> str:
        """Redis key used for tracking concurrency for this specific argument value.
        Raises RuntimeError if accessed before initialization."""
        if not self._initialized:
            raise RuntimeError(
                "ConcurrencyLimit not initialized - use within task context"
            )
        assert self._concurrency_key is not None
        return self._concurrency_key
