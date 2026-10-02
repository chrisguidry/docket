"""A task whose worker dies while it runs finishes on another worker.

The driver kills the first worker with SIGKILL while the 3 s task runs.  The
second worker takes the task over once the first worker's lease runs out.
"""

import signal

from ..harness import Harness

TIMEOUT = 60


async def run(harness: Harness) -> None:
    first = await harness.start("worker")
    await harness.produce()
    await harness.events.wait_for(
        lambda: bool(harness.events.matching("started")),
        timeout=15,
        waiting_for="the task to start",
    )

    harness.signal(first, signal.SIGKILL)
    await harness.exited([first], timeout=5)
    await harness.start("worker")

    await harness.events.wait_for(
        lambda: bool(harness.events.matching("finished")),
        timeout=20,
        waiting_for="the task to finish on the second worker",
    )

    started = harness.events.matching("started")
    finished = harness.events.matching("finished")
    assert len(started) == 2, f"Expected two starts, saw {started}"
    killed, survivor = (event.worker for event in started)
    assert killed != survivor, f"Expected the task on two workers, saw {started}"
    assert [event.worker for event in finished] == [survivor], finished
    # A redelivery is not a retry: the second run keeps the first attempt's
    # number, so a task's retry count does not include worker deaths.
    attempts = [event.attempt for event in started]
    assert attempts == [1, 1], f"Expected attempt 1 on both runs, saw {attempts}"
    state = await harness.settled_state("interrupted")
    assert state == "completed", f"The task's run state is {state}"
