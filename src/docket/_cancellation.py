"""Cancellation utilities for distinguishing internal vs external cancellation.

When cancelling asyncio tasks, we often need to distinguish between cancellations
we initiated (e.g., for timeout or cleanup) vs external cancellations (e.g., from
a shutdown signal). Python 3.9+ supports passing a message to task.cancel(msg=...)
which we use as a sentinel to identify our own cancellations.

Note on Python version differences:
- Python 3.11+: The cancel message propagates to the awaiter's CancelledError
- Python 3.10: The message is stored but does NOT propagate to the awaiter

The cancel_task() helper handles this by tracking that we initiated the cancel.
"""

import asyncio
import sys
from typing import Any

# Sentinel message for internal cancellation during cleanup
CANCEL_MSG_CLEANUP = "docket:cleanup"


def is_our_cancellation(exc: asyncio.CancelledError, expected_msg: str) -> bool:
    """Check if we initiated this cancellation (vs. someone cancelling us).

    When we cancel a task with task.cancel(msg), the CancelledError will have
    args[0] set to that message. External cancellations (from TaskGroup, signals,
    etc.) typically have no message or a different message.

    Note: In Python 3.10, the message does NOT propagate to the awaiter, so this
    function will return False even for our own cancellations. Use cancel_task()
    instead for reliable cross-version behavior.

    Args:
        exc: The CancelledError to check
        expected_msg: The sentinel message we used when cancelling

    Returns:
        True if the cancellation was initiated by us with the expected message
    """
    return bool(exc.args and exc.args[0] == expected_msg)


async def cancel_task(task: "asyncio.Task[Any]", reason: str) -> None:
    """Cancel a task and await its completion, suppressing our own cancellation.

    This handles Python 3.10/3.11+ compatibility: in Python 3.10, the cancel
    message doesn't propagate to the awaiter, so we can't rely on checking
    the message. Instead, we track that we initiated the cancellation.

    In Python 3.11+, we verify the message matches as an extra check.

    Args:
        task: The task to cancel
        reason: A description of why we're cancelling (e.g., CANCEL_MSG_CLEANUP)
    """
    task.cancel(reason)
    try:
        await task
    except asyncio.CancelledError as e:
        if is_our_cancellation(e, reason):
            return  # pragma: no cover - only on 3.11+ when message propagates
        # Python 3.10: message doesn't propagate, but we just called cancel()
        # so this CancelledError is probably from our cancel() call
        if sys.version_info < (3, 11):  # pragma: no cover
            return
        # External cancellation - propagate it
        raise  # pragma: no cover - race condition between cancel() and await
