"""Every task added runs, while the driver kills workers and restarts Redis.

Five producers add tasks while ten workers run them.  The workers come from
all the implementations given, so with ``python@main`` and ``python@release``
this also shows that the working tree and the last release can share a
docket.
"""

import asyncio
import logging
import random
import signal

from ..harness import Agent, Harness

logger = logging.getLogger(__name__)

TIMEOUT = 270
PRODUCERS = 5
WORKERS = 10

# Each producer connects before it can retry a lost connection, so the chaos
# waits until they have started.
CALM_SECONDS = 5
RESTART_CHANCE = 0.02
KILL_CHANCE = 0.08


async def run(harness: Harness) -> None:
    producers = [await harness.start("produce") for _ in range(PRODUCERS)]
    workers = [await harness.start("worker") for _ in range(WORKERS)]

    loop = asyncio.get_running_loop()
    calm_until = loop.time() + CALM_SECONDS
    next_report = loop.time()

    while True:
        await harness.events.poll(0.25)
        added = {event.key for event in harness.events.matching("added")}
        ran = {event.key for event in harness.events.matching("ran")}

        failed = [p.process.returncode for p in producers if p.process.returncode]
        assert not failed, f"Producers exited {failed}"
        if added and added <= ran and not any(p.running for p in producers):
            break

        if loop.time() >= next_report:
            logger.info("added: %d, ran: %d", len(added), len(ran & added))
            next_report = loop.time() + 5

        if loop.time() >= calm_until:
            await cause_chaos(harness, workers)

        for index, worker in enumerate(workers):
            if not worker.running:
                workers[index] = await harness.start("worker")

    times = [event.time for event in harness.events.matching("ran")]
    seconds = max(times) - min(times)
    logger.info(
        "Ran %d tasks in %.1f s, %.0f each second",
        len(added),
        seconds,
        len(added) / seconds,
    )


async def cause_chaos(harness: Harness, workers: list[Agent]) -> None:
    chance = random.random()
    if chance < RESTART_CHANCE:
        logger.warning("CHAOS: restarting Redis")
        await harness.restart_redis()
    elif chance < RESTART_CHANCE + KILL_CHANCE:
        worker = random.choice(workers)
        logger.warning("CHAOS: killing a %s worker", worker.implementation.name)
        harness.signal(worker, signal.SIGKILL)
