"""A perpetual task that stops itself keeps a replace made while it ran.

The first run of ``stopping`` replaces its own key 1 s out, then stops
itself.  The stop must leave the replacement alone, so the replacement runs
once, about 1 s after the replace, and stops the task for good.
"""

from ..harness import Harness

TIMEOUT = 30
REPLACE_SECONDS = 1.0

# The task records "replaced" just after its replace, so the replacement can
# run a little less than 1 s after that record.
EARLY = 0.3
LATE = 1.0


async def run(harness: Harness) -> None:
    await harness.start("worker")
    await harness.produce()
    await harness.events.wait_for(
        lambda: bool(harness.events.matching("replacement")),
        timeout=10,
        waiting_for="the replacement to run",
    )
    # A second run of either, if there were one, would come soon after.
    await harness.events.poll(2)

    first = harness.events.matching("first")
    assert len(first) == 1, f"Expected one first run, saw {first}"
    replacement = harness.events.matching("replacement")
    assert len(replacement) == 1, f"Expected one replacement run, saw {replacement}"

    (replaced,) = harness.events.matching("replaced")
    delay = replacement[0].time - replaced.time
    assert REPLACE_SECONDS - EARLY <= delay <= REPLACE_SECONDS + LATE, (
        f"Expected the replacement about {REPLACE_SECONDS} s after the replace, "
        f"saw {delay:.2f} s"
    )
    assert await harness.settled_state("stopping") == "completed"
