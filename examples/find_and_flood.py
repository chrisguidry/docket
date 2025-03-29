import asyncio
import random
from datetime import timedelta
from logging import Logger, LoggerAdapter
from typing import Annotated

from docket import Docket
from docket.annotations import Logged
from docket.dependencies import CurrentDocket, Perpetual, TaskLogger

from .common import run_example_workers


async def find(
    docket: Docket = CurrentDocket(),
    logger: LoggerAdapter[Logger] = TaskLogger(),
    perpetual: Perpetual = Perpetual(every=timedelta(seconds=3), automatic=True),
) -> None:
    for i in range(1, 10 + 1):
        await docket.add(flood)(i)


async def flood(
    item: Annotated[int, Logged],
    logger: LoggerAdapter[Logger] = TaskLogger(),
) -> None:
    logger.info("Working on %s", item)
    await asyncio.sleep(random.uniform(0.5, 2))


tasks = [find, flood]


if __name__ == "__main__":
    asyncio.run(
        run_example_workers(
            workers=3,
            concurrency=8,
            tasks="examples.find_and_flood:tasks",
        )
    )
