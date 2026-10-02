"""Producers that add and replace the same key get exactly one run.

Two producers each add ``shared`` 2 s out, then replace it 4 s out.  The add
that comes second does nothing, and the replace that comes last sets the
time of the one run.
"""

from ..harness import Harness

TIMEOUT = 40
PRODUCERS = 2
REPLACE_SECONDS = 4.0

# The producer records "replaced" just after its replace, so the run can come
# a little less than 4 s after that record.
EARLY = 0.3
LATE = 1.0


async def run(harness: Harness) -> None:
    await harness.start("worker")
    await harness.produce(count=PRODUCERS)
    await harness.events.wait_for(
        lambda: bool(harness.events.matching("ran")),
        timeout=15,
        waiting_for="the shared task to run",
    )
    # A second run, if there were one, would come soon after the first.
    await harness.events.poll(3)

    replaced = sorted(event.time for event in harness.events.matching("replaced"))
    assert len(replaced) == PRODUCERS, f"Expected {PRODUCERS} replaces, saw {replaced}"
    assert replaced[-1] - replaced[0] < 2, (
        "The producers ran too far apart to share the key: "
        f"{replaced[-1] - replaced[0]:.2f} s"
    )

    ran = harness.events.matching("ran")
    assert len(ran) == 1, f"Expected one run of the shared key, saw {ran}"
    delay = ran[0].time - replaced[-1]
    assert REPLACE_SECONDS - EARLY <= delay <= REPLACE_SECONDS + LATE, (
        f"Expected the run about {REPLACE_SECONDS} s after the last replace, "
        f"saw {delay:.2f} s"
    )
