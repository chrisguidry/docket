import asyncio
import logging
import os
import sys
import time

import redis.exceptions

from docket import Docket

from .tasks import hello

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("chaos.producer")


async def main(tasks_to_produce: int):
    docket = Docket(
        name=os.environ["DOCKET_NAME"],
        url=os.environ["DOCKET_URL"],
    )
    tasks_sent = 0
    while tasks_sent < tasks_to_produce:
        try:
            async with docket:
                async with docket.redis() as r:
                    for _ in range(tasks_sent, tasks_to_produce):
                        execution = await docket.add(hello)()
                        await r.zadd("hello:sent", {execution.key: time.time()})
                        logger.info("Added task %s", execution.key)
                        tasks_sent += 1
        except redis.exceptions.ConnectionError:
            logger.warning(
                "producer: Redis connection error, retrying in 5s... "
                f"({tasks_sent}/{tasks_to_produce} tasks sent)"
            )
            await asyncio.sleep(5)


if __name__ == "__main__":
    tasks = int(sys.argv[1])
    asyncio.run(main(tasks))
