"""A worker that crashes stops its infrastructure tasks and reaches its caller."""

import asyncio
import sys
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator
from unittest.mock import patch

import pytest

if sys.version_info < (3, 11):  # pragma: no cover
    from exceptiongroup import ExceptionGroup

from docket import Docket, Worker
from docket.execution import Execution
from tests._key_leak_checker import KeyCountChecker


@pytest.fixture
def listener_loses_cancels(docket: Docket):
    """On Python 3.10, redis-py bounds a pub/sub read with async_timeout.  When
    its timer fires in the same step as an outside cancel, async_timeout turns
    the one CancelledError into a TimeoutError, and redis-py returns None as if
    the read just timed out.  This pub/sub loses every cancel the same way."""
    original_pubsub = docket._pubsub  # pyright: ignore[reportPrivateUsage]

    @asynccontextmanager
    async def pubsub_that_loses_cancels() -> AsyncGenerator[Any, None]:
        async with original_pubsub() as pubsub:
            original_get_message = pubsub.get_message

            async def get_message(**kwargs: Any) -> dict[str, Any] | None:
                try:
                    return await original_get_message(**kwargs)
                except asyncio.CancelledError:
                    return None

            pubsub.get_message = get_message  # type: ignore[method-assign]
            yield pubsub

    with patch.object(docket, "_pubsub", pubsub_that_loses_cancels):
        yield


async def crash(self: Execution, *args: Any, **kwargs: Any) -> None:
    raise RuntimeError("simulated crash while recording the outcome")


async def test_worker_crash_reaches_caller_when_listener_loses_its_cancel(
    docket: Docket, listener_loses_cancels: None, key_leak_checker: KeyCountChecker
):
    async def task() -> None:
        pass

    execution = await docket.add(task)()

    # The crash leaves the task pending, so its runs and progress hashes
    # have no TTL yet.
    key_leak_checker.add_exemption(docket.runs_key(execution.key))
    key_leak_checker.add_exemption(docket.key(f"progress:{execution.key}"))

    async with Worker(docket) as worker:
        with (
            patch.object(Execution, "mark_as_completed", crash),
            patch.object(Execution, "mark_as_failed", crash),
            pytest.raises(ExceptionGroup) as caught,
        ):
            await asyncio.wait_for(worker.run_until_finished(), timeout=5)

    assert any("simulated crash" in str(e) for e in caught.value.exceptions)
