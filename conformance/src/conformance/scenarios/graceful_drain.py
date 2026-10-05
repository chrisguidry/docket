"""Workers finish the tasks they hold when SIGTERM or SIGINT stops them.

Two workers with a concurrency of 2 take four tasks of 3 s each.  When all
four have started, the driver signals both workers.  Each worker must finish
its tasks, record them complete, and exit 0.
"""

import asyncio
import signal

from ..harness import Harness

TIMEOUT = 90
WORKERS = 2
TASKS = 4
TASK_SECONDS = 3


async def run(harness: Harness) -> None:
    for signum in (signal.SIGTERM, signal.SIGINT):
        await drain(harness, signum)


async def drain(harness: Harness, signum: signal.Signals) -> None:
    already_started = {event.key for event in harness.events.matching("started")}

    def started() -> set[str]:
        return {
            event.key for event in harness.events.matching("started")
        } - already_started

    workers = [await harness.start("worker") for _ in range(WORKERS)]
    await harness.produce()
    await harness.events.wait_for(
        lambda: len(started()) >= TASKS,
        timeout=30,
        waiting_for=f"{TASKS} tasks to start",
    )

    # Let the tasks get into their sleep before the signal.
    await asyncio.sleep(0.5)
    for worker in workers:
        harness.signal(worker, signum)
    await harness.exited(workers, timeout=TASK_SECONDS + 10)

    codes = [worker.process.returncode for worker in workers]
    assert codes == [0] * WORKERS, f"Workers exited {codes} after {signum.name}"

    await harness.events.poll(0.5)
    finished = {event.key for event in harness.events.matching("finished")}
    assert started() <= finished, (
        f"After {signum.name}, {started() - finished} did not finish"
    )
    for key in started():
        state = await harness.run_state(key)
        assert state == "completed", f"After {signum.name}, {key} is {state}"
