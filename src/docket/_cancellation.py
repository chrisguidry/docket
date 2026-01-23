"""Cancellation utilities for distinguishing internal vs external cancellation.

When cancelling asyncio tasks, we often need to distinguish between cancellations
we initiated (e.g., for timeout or cleanup) vs external cancellations (e.g., from
a shutdown signal). Python 3.9+ supports passing a message to task.cancel(msg=...)
which we use as a sentinel to identify our own cancellations.
"""

import asyncio

# Sentinel messages for internal cancellation
CANCEL_MSG_TIMEOUT = "docket:timeout"
CANCEL_MSG_CLEANUP = "docket:cleanup"


def is_our_cancellation(exc: asyncio.CancelledError, expected_msg: str) -> bool:
    """Check if we initiated this cancellation (vs. someone cancelling us).

    When we cancel a task with task.cancel(msg), the CancelledError will have
    args[0] set to that message. External cancellations (from TaskGroup, signals,
    etc.) typically have no message or a different message.

    Args:
        exc: The CancelledError to check
        expected_msg: The sentinel message we used when cancelling

    Returns:
        True if the cancellation was initiated by us with the expected message
    """
    return bool(exc.args and exc.args[0] == expected_msg)
