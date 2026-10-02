"""Load while the driver kills workers and restarts Redis.

Each producer adds ``TASKS_PER_PRODUCE`` tasks with jitter of five seconds
either way, a toxic task now and then, and some strikes that never match, so
the workers evaluate strikes on every task.
"""

import asyncio
import logging
import random
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import redis.exceptions
from docket import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    Depends,
    Docket,
    Retry,
    Worker,
)
from docket.execution import Execution
from docket.strikelist import Operator
from events import record

logger = logging.getLogger(__name__)

SCENARIO = "chaos"
WORKER: dict[str, Any] = {"redelivery_timeout": timedelta(seconds=5)}

TASKS_PER_PRODUCE = 4000
STRIKES_PER_PRODUCE = 20
TOXIC_CHANCE = 0.01


async def greeting() -> str:
    return "Hello, world"


async def emphatic_greeting(greeting: str = Depends(greeting)) -> str:
    return greeting + "!"


async def hello(
    greeting: str = Depends(emphatic_greeting),
    docket: Docket = CurrentDocket(),
    execution: Execution = CurrentExecution(),
    worker: Worker = CurrentWorker(),
    retry: Retry = Retry(attempts=sys.maxsize),
) -> None:
    await record(
        docket,
        SCENARIO,
        "ran",
        task="hello",
        key=execution.key,
        attempt=execution.attempt,
        worker=worker.name,
    )


async def toxic() -> None:
    if random.random() < 0.25:
        sys.exit(42)
    elif random.random() < 0.5:
        raise Exception("Boom")
    else:
        await asyncio.sleep(random.uniform(0.01, 0.05))


tasks = [hello, toxic]


async def produce(docket: Docket) -> None:
    strikes = 0
    sent = 0
    while sent < TASKS_PER_PRODUCE:
        try:
            while strikes < STRIKES_PER_PRODUCE:
                await docket.strike(
                    "rando",
                    f"param_{random.randint(1, 100)}",
                    random.choice(list(Operator)),
                    f"val_{random.randint(1, 1000)}",
                )
                strikes += 1

            jitter = timedelta(seconds=random.uniform(-5, 5))
            when = datetime.now(timezone.utc) + jitter
            execution = await docket.add(hello, when=when)()
            await record(docket, SCENARIO, "added", task="hello", key=execution.key)
            sent += 1
            if random.random() < TOXIC_CHANCE:
                await docket.add(toxic)()
        except redis.exceptions.ConnectionError:
            logger.warning("Redis went away after %d tasks, retrying", sent)
            await asyncio.sleep(1)
