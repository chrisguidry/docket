"""Tests for automatic key leakage protection.

This module tests the autouse key_leak_checker fixture that ensures no Redis keys
without TTLs leak during test execution, protecting against memory leaks in
long-running Docket deployments.
"""

from datetime import datetime, timedelta, timezone

import pytest

from docket import Docket, Worker
from tests._key_leak_checker import KeyCountChecker


async def test_leak_detection_catches_keys_without_ttl(
    redis_url: str,
    docket: Docket,
    worker: Worker,
    key_leak_checker: KeyCountChecker,
) -> None:
    """Verify that the leak checker catches keys created without TTL."""

    leaked_key = docket.key("leaked-key")

    async def task_that_leaks() -> None:
        """Task that intentionally creates a key without TTL."""
        async with docket.redis() as redis:
            # Intentionally create a key without TTL
            await redis.set(leaked_key, "oops")

    docket.register(task_that_leaks)

    # Exempt the intentional leak from autouse checker
    key_leak_checker.add_exemption(leaked_key)

    await docket.add(task_that_leaks)()
    await worker.run_until_finished()

    # Manually verify it would have caught the leak (without exemption)
    async with docket.redis() as redis:
        # Verify the key actually exists without TTL
        ttl = await redis.ttl(leaked_key)
        assert ttl == -1, f"Expected leaked key to have no TTL, but got TTL={ttl}"

    # Remove exemption temporarily to verify detection works
    key_leak_checker.exemptions.remove(leaked_key)
    with pytest.raises(AssertionError, match="Memory leak detected"):
        await key_leak_checker.verify_remaining_keys_have_ttl()

    # Clean up the leaked key for teardown
    async with docket.redis() as redis:
        await redis.delete(leaked_key)


async def test_permanent_keys_are_exempt(
    docket: Docket,
    worker: Worker,
    key_leak_checker: KeyCountChecker,
) -> None:
    """Verify that permanent infrastructure keys are not flagged as leaks."""

    async def simple_task() -> None:
        pass

    docket.register(simple_task)
    await docket.add(simple_task)()
    await worker.run_until_finished()

    # Should not raise - permanent keys (stream, workers, strikes) are exempt
    await key_leak_checker.verify_remaining_keys_have_ttl()


async def test_exemption_mechanism(
    redis_url: str,
    docket: Docket,
    worker: Worker,
    key_leak_checker: KeyCountChecker,
) -> None:
    """Verify that test-specific exemptions work."""

    async def task_with_special_key() -> None:
        """Task that creates a key we want to exempt."""
        async with docket.redis() as redis:
            await redis.set(f"{docket.name}:special-key", "intentional")

    docket.register(task_with_special_key)

    # Exempt this specific key
    key_leak_checker.add_exemption(f"{docket.name}:special-key")

    await docket.add(task_with_special_key)()
    await worker.run_until_finished()

    # Should not raise - we exempted the special key
    await key_leak_checker.verify_remaining_keys_have_ttl()


async def test_multiple_exemptions(
    redis_url: str,
    docket: Docket,
    worker: Worker,
    key_leak_checker: KeyCountChecker,
) -> None:
    """Verify that multiple exemptions can be added."""

    async def task_with_multiple_keys() -> None:
        """Task that creates multiple keys we want to exempt."""
        async with docket.redis() as redis:
            await redis.set(f"{docket.name}:special-key-1", "intentional")
            await redis.set(f"{docket.name}:special-key-2", "intentional")

    docket.register(task_with_multiple_keys)

    # Exempt both keys
    key_leak_checker.add_exemption(f"{docket.name}:special-key-1")
    key_leak_checker.add_exemption(f"{docket.name}:special-key-2")

    await docket.add(task_with_multiple_keys)()
    await worker.run_until_finished()

    # Should not raise - both keys are exempted
    async with docket.redis() as redis:
        key_leak_checker.redis = redis
        await key_leak_checker.verify_remaining_keys_have_ttl()


async def test_worker_task_sets_are_exempt(
    docket: Docket,
    worker: Worker,
    key_leak_checker: KeyCountChecker,
) -> None:
    """Verify that worker-tasks and task-workers sets are properly handled.

    These sets don't have explicit TTLs but are self-cleaning via worker heartbeat
    expiration, so they should be exempt from leak detection.
    """

    async def simple_task() -> None:
        pass

    docket.register(simple_task)
    await docket.add(simple_task)()
    await worker.run_until_finished()

    # Should not raise - worker sets are exempt
    async with docket.redis() as redis:
        key_leak_checker.redis = redis
        await key_leak_checker.verify_remaining_keys_have_ttl()


async def test_queue_is_cleaned_up(
    docket: Docket,
    worker: Worker,
    key_leak_checker: KeyCountChecker,
) -> None:
    """Verify that the queue sorted set is cleaned up after tasks complete."""

    async def scheduled_task() -> None:
        pass

    docket.register(scheduled_task)

    # Schedule a task for the future
    await docket.add(
        scheduled_task, when=datetime.now(timezone.utc) + timedelta(seconds=1)
    )()

    # Wait for it to execute
    await worker.run_until_finished()

    # Queue should be empty after task completes
    async with docket.redis() as redis:
        key_leak_checker.redis = redis
        await key_leak_checker.verify_remaining_keys_have_ttl()
