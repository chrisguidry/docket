import asyncio
import random
from datetime import datetime, timedelta, timezone
from logging import Logger, LoggerAdapter

from docket import Docket
from docket.dependencies import CurrentDocket, Perpetual, TaskLogger

from .common import run_example_workers


def now() -> datetime:
    return datetime.now(timezone.utc)


async def bouncey(
    docket: Docket = CurrentDocket(),
    logger: LoggerAdapter[Logger] = TaskLogger(),
    perpetual: Perpetual = Perpetual(every=timedelta(seconds=3), automatic=True),
) -> None:
    seconds = random.randint(1, 10)
    perpetual.every = timedelta(seconds=seconds)
    logger.info("See you in %s seconds at %s", seconds, now() + perpetual.every)


tasks = [bouncey]


if __name__ == "__main__":
    asyncio.run(
        run_example_workers(
            workers=3,
            concurrency=8,
            tasks="examples.self_perpetuating:tasks",
        )
    )
