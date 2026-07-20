"""Cancellation utilities for cleaning up background tasks.

When we cancel one of our own background tasks (a heartbeat, a monitor, a
renewal loop) and await it, the task's CancelledError surfaces in the awaiter
and must not propagate: we asked for it.  A CancelledError should only
propagate when it was delivered to the *caller* -- an external cancellation
that arrived while we were waiting.

Telling the two apart by the cancel message (task.cancel(msg=...)) is
unreliable: CPython drops the message when the task's awaited future completes
in the same event-loop tick as the cancellation (python/cpython#91048), and on
Python 3.10 the message never propagates at all.  The awaited task's final
state is reliable on every supported version, so cancel_task() checks that
instead.
"""

import asyncio
from typing import Any

# Sentinel message for internal cancellation during cleanup
CANCEL_MSG_CLEANUP = "docket:cleanup"


async def cancel_task(task: "asyncio.Task[Any]", reason: str) -> None:
    """Cancel a task and await its completion, suppressing its cancellation.

    If the awaited task ended cancelled, the CancelledError came from it, and
    we swallow it -- we initiated that cancellation.  If the task did not end
    cancelled, the CancelledError was delivered to the caller instead, so it
    propagates.

    Args:
        task: The task to cancel
        reason: A description of why we're cancelling (e.g., CANCEL_MSG_CLEANUP)
    """
    task.cancel(reason)
    try:
        await task
    except asyncio.CancelledError:
        if not task.cancelled():  # pragma: no cover - caller cancelled mid-await
            raise
