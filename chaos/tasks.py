import logging
import sys
import time

from docket import CurrentDocket, Docket, Retry, TaskKey

logger = logging.getLogger(__name__)


async def hello(
    key: str = TaskKey(),
    docket: Docket = CurrentDocket(),
    retry: Retry = Retry(attempts=sys.maxsize),
):
    logger.info("Starting task %s", key)
    async with docket.redis() as redis:
        await redis.zadd("hello:received", {key: time.time()})
    logger.info("Finished task %s", key)


async def toxic():
    sys.exit(42)


chaos_tasks = [hello, toxic]
