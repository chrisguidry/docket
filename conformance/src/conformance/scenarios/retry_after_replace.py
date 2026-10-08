"""A failed run that was replaced while it ran does not retry.

The first run of ``flaky`` replaces its own key 1 s out, then fails.  Its
retry must give way to the replacement, so the first run never runs again
and the replacement runs once, about 1 s after the replace.
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
    # A retry or a second replacement, if there were one, would come soon after.
    await harness.events.poll(2)

    first = harness.events.matching("first")
    assert [event.attempt for event in first] == [1], (
        f"Expected only the first attempt of the first run, saw {first}"
    )
    replacement = harness.events.matching("replacement")
    assert len(replacement) == 1, f"Expected one replacement run, saw {replacement}"

    (replaced,) = harness.events.matching("replaced")
    delay = replacement[0].time - replaced.time
    assert REPLACE_SECONDS - EARLY <= delay <= REPLACE_SECONDS + LATE, (
        f"Expected the replacement about {REPLACE_SECONDS} s after the replace, "
        f"saw {delay:.2f} s"
    )
    assert await harness.settled_state("flaky") == "completed"
