"""An automatic perpetual task runs every 0.5 s, and survives its worker.

The worker schedules the task when it starts, without a producer.  The
driver kills that worker with SIGKILL, and a new worker takes over the task.
"""

import signal
from itertools import pairwise

from ..events import Event
from ..harness import Harness

TIMEOUT = 40
EVERY = 0.5
RUNS_BEFORE_KILL = 5
RUNS_AFTER_KILL = 3


def runs_by_worker(harness: Harness) -> dict[str, list[Event]]:
    runs: dict[str, list[Event]] = {}
    for event in harness.events.matching("ran"):
        runs.setdefault(event.worker, []).append(event)
    return runs


async def run(harness: Harness) -> None:
    first = await harness.start("worker")
    await harness.events.wait_for(
        lambda: len(harness.events.matching("ran")) >= RUNS_BEFORE_KILL,
        timeout=15,
        waiting_for=f"{RUNS_BEFORE_KILL} runs",
    )

    harness.signal(first, signal.SIGKILL)
    await harness.exited([first], timeout=5)
    (killed,) = runs_by_worker(harness)

    await harness.start("worker")
    await harness.events.wait_for(
        lambda: sum(1 for e in harness.events.matching("ran") if e.worker != killed)
        >= RUNS_AFTER_KILL,
        timeout=20,
        waiting_for=f"{RUNS_AFTER_KILL} runs on the new worker",
    )

    runs = runs_by_worker(harness)
    assert len(runs) == 2, f"Expected the task on two workers, saw {list(runs)}"
    assert {event.key for event in harness.events.matching("ran")} == {"tick"}

    for worker, events in runs.items():
        gaps = [later.time - earlier.time for earlier, later in pairwise(events)]
        assert all(EVERY * 0.8 <= gap <= EVERY + 1.0 for gap in gaps), (
            f"Expected runs about {EVERY} s apart on {worker}, saw {gaps}"
        )
