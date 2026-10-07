"""A task whose admission check can't run fails, and its retry runs it again.

The task's concurrency limit counts by a ``customer`` argument that the task
doesn't have, so the limit's check can't run.  Each attempt fails before the
task's body runs, and the task's retry allows three attempts, so the task ends
as failed after the third.  The body never runs, so the driver counts the
attempts on the task's state channel, where each attempt's claim publishes
``running``.
"""

from ..harness import Harness

TIMEOUT = 30
KEY = "unlimited"


async def run(harness: Harness) -> None:
    async with harness.published_states(KEY) as states:
        await harness.start("worker")
        await harness.produce()
        state = await harness.settled_state(KEY, timeout=15)

    await harness.events.poll(0.5)
    assert state == "failed", f"The task ended as {state}"
    assert states.count("running") == 3, f"Expected three attempts, saw {states}"
    assert not harness.events.matching("ran"), "The task ran past its admission check"
