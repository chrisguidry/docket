"""A task fails every attempt and retries with exponential backoff.

The task allows four attempts with a minimum delay of 0.5 s, so the delays
double: 0.5 s, 1 s, then 2 s.  After the fourth attempt, it stops.
"""

from itertools import pairwise

from ..harness import Harness

TIMEOUT = 40
EXPECTED_DELAYS = [0.5, 1.0, 2.0]

# A worker checks for due tasks a few times each second, so a retry can start
# late but never early.  The lower bound leaves room only for clock skew.
EARLY = 0.9
LATE = 1.0


async def run(harness: Harness) -> None:
    await harness.start("worker")
    await harness.produce()

    await harness.events.wait_for(
        lambda: len(harness.events.matching("attempt")) >= 4,
        timeout=20,
        waiting_for="four attempts",
    )
    # A fifth attempt, if there were one, would come 4 s after the fourth.
    await harness.events.poll(5)

    attempts = harness.events.matching("attempt")
    assert [event.attempt for event in attempts] == [1, 2, 3, 4], attempts
    assert {event.key for event in attempts} == {"flaky"}, attempts

    delays = [later.time - earlier.time for earlier, later in pairwise(attempts)]
    for delay, expected in zip(delays, EXPECTED_DELAYS):
        assert expected * EARLY <= delay <= expected + LATE, (
            f"Expected delays near {EXPECTED_DELAYS}, saw {delays}"
        )
