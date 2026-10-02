"""A task cancelled before it is due never runs.

The producer schedules ``doomed`` 1.5 s out and ``sentinel`` 2.5 s out, then
cancels ``doomed``.  When ``sentinel`` runs, the time for ``doomed`` is past.
"""

from ..harness import Harness

TIMEOUT = 30


async def run(harness: Harness) -> None:
    await harness.start("worker")
    await harness.produce()

    await harness.events.wait_for(
        lambda: bool(harness.events.matching("ran", task="sentinel")),
        timeout=15,
        waiting_for="the sentinel to run",
    )

    assert harness.events.matching("cancelled", task="doomed")
    assert not harness.events.matching("ran", task="doomed"), "The cancelled task ran"
