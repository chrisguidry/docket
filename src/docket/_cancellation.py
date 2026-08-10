"""Cancellation-correct waiting primitives.

When we cancel one of our own background tasks (a heartbeat, a monitor, a
renewal loop) and await it, the task's CancelledError surfaces in the awaiter
and must not propagate: we asked for it.  A CancelledError should only
propagate when it was delivered to the *caller* -- an external cancellation
that arrived while we were waiting.  The helpers here guarantee both halves
of that contract, which plain awaits and asyncio.wait_for do not.

Awaiting the task directly cannot tell those two apart: the cancel message
(task.cancel(msg=...)) is dropped when the awaited future completes in the
same event-loop tick as the cancellation (python/cpython#91048), and the
task's final state is ambiguous when both cancellations happen at once.
asyncio.wait() never raises the awaited task's CancelledError at all, so the
only CancelledError that can escape cancel_task() is one delivered to the
caller, which must propagate.
"""

import asyncio
from typing import Any

# Sentinel message for internal cancellation during cleanup
CANCEL_MSG_CLEANUP = "docket:cleanup"


async def _wait_for_event(event: asyncio.Event, timeout: float) -> bool:
    """Wait up to ``timeout`` seconds for ``event``; True if it was set.

    asyncio.wait_for on Python 3.10 and 3.11 swallows a cancellation that is
    delivered to the caller in the same event-loop tick that the inner future
    completes (python/cpython#86296; the 3.12 rewrite fixed it).  The worker
    waits on events between polls, so a worker cancelled at that moment would
    lose the cancellation and run forever.  asyncio.wait never returns the
    inner task's result through the caller and never absorbs the caller's
    cancellation.
    """
    if event.is_set():
        return True
    waiter = asyncio.ensure_future(event.wait())
    try:
        done, _ = await asyncio.wait([waiter], timeout=timeout)
        return bool(done)
    finally:
        waiter.cancel()


async def cancel_task(task: "asyncio.Task[Any]", reason: str) -> None:
    """Cancel a task and await its completion, suppressing its cancellation.

    A CancelledError raised here was delivered to the caller, not the awaited
    task, and propagates.  If the task ended with a real exception instead of
    cancelling, that exception is re-raised.

    Args:
        task: The task to cancel
        reason: A description of why we're cancelling (e.g., CANCEL_MSG_CLEANUP)
    """
    task.cancel(reason)
    await asyncio.wait([task])
    if not task.cancelled() and (error := task.exception()) is not None:
        raise error
