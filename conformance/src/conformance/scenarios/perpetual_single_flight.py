"""Workers that share an automatic perpetual task never run it twice at once.

Three workers schedule the same 0.2 s task every 0.5 s.  While a run is in
progress, the driver kills all three with SIGKILL and starts three new ones.
The new workers schedule the task when they start, and the dead worker's
run comes back once its lease runs out, but the runs must never overlap.
"""

import signal
import time
from itertools import pairwise

from ..events import most_at_once
from ..harness import Harness

TIMEOUT = 60
WORKERS = 3
EVERY = 0.5
RUNS = 6

# A gap is late by the scheduling delay.  Across the kill, it also includes
# the new workers' start and the 2 s lease of the dead worker's run.
LATE = 1.0
LATE_ACROSS_KILL = 6.0


async def run(harness: Harness) -> None:
    workers = [await harness.start("worker") for _ in range(WORKERS)]
    await harness.events.wait_for(
        lambda: len(harness.events.matching("finished")) >= RUNS
        and harness.events.seen[-1].event == "started",
        timeout=20,
        waiting_for=f"a run in progress after {RUNS} runs",
    )

    for worker in workers:
        harness.signal(worker, signal.SIGKILL)
    killed_at = time.time()
    await harness.exited(workers, timeout=5)

    for _ in range(WORKERS):
        await harness.start("worker")
    await harness.events.wait_for(
        lambda: sum(
            1 for e in harness.events.matching("finished") if e.time > killed_at
        )
        >= RUNS,
        timeout=30,
        waiting_for=f"{RUNS} runs on the new workers",
    )

    starts = [e.time for e in harness.events.matching("started")]
    ends = [e.time for e in harness.events.matching("finished")]
    # The run that the kill cut short never finished, so it ends at the kill.
    cut_short = sum(t < killed_at for t in starts) - sum(t < killed_at for t in ends)
    peak = most_at_once(starts, ends + [killed_at] * cut_short)
    assert peak == 1, f"{peak} runs were in progress at once"

    for earlier, later in pairwise(sorted(starts)):
        late = LATE_ACROSS_KILL if earlier < killed_at < later else LATE
        gap = later - earlier
        assert gap <= EVERY + late, f"Runs {gap:.2f} s apart, expected about {EVERY} s"
