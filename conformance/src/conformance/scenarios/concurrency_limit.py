"""No more than two tasks under one concurrency limit run at once.

Three workers with a concurrency of 4 each take twelve tasks of 0.5 s that
share ``ConcurrencyLimit(max_concurrent=2)``.  The workers have room for
twelve at once, so only the limit holds them to two.
"""

from ..events import most_at_once
from ..harness import Harness

TIMEOUT = 60
WORKERS = 3
TASKS = 12
LIMIT = 2


async def run(harness: Harness) -> None:
    for _ in range(WORKERS):
        await harness.start("worker")
    await harness.produce()

    await harness.events.wait_for(
        lambda: len(harness.events.matching("finished")) >= TASKS,
        timeout=40,
        waiting_for=f"{TASKS} tasks to finish",
    )

    started = harness.events.matching("started")
    finished = harness.events.matching("finished")
    keys = [event.key for event in started]
    assert len(keys) == len(set(keys)) == TASKS, f"Expected each task once, saw {keys}"

    peak = most_at_once([e.time for e in started], [e.time for e in finished])
    assert peak <= LIMIT, f"{peak} tasks ran at once, over the limit of {LIMIT}"
    assert peak == LIMIT, (
        f"At most {peak} tasks ran at once, so the limit was never reached"
    )
