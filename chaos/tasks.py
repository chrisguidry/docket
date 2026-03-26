import asyncio
import logging
import random
import sys
import time

from docket import CurrentDocket, Depends, Docket, Retry, TaskKey

logger = logging.getLogger(__name__)


async def greeting() -> str:
    return "Hello, world"


async def emphatic_greeting(greeting: str = Depends(greeting)) -> str:
    return greeting + "!"


async def hello(
    greeting: str = Depends(emphatic_greeting),
    key: str = TaskKey(),
    docket: Docket = CurrentDocket(),
    retry: Retry = Retry(attempts=sys.maxsize),
):
    logger.info("Starting task %s", key)
    logger.info("Greeting: %s", greeting)
    async with docket.redis() as redis:
        await redis.zadd("hello:received", {key: time.time()})
    logger.info("Finished task %s", key)


async def toxic():
    if random.random() < 0.25:
        sys.exit(42)
    elif random.random() < 0.5:
        raise Exception("Boom")
    else:
        await asyncio.sleep(random.uniform(0.01, 0.05))


chaos_tasks = [hello, toxic]
