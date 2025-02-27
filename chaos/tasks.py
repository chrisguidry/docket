import sys
import time

from docket import CurrentDocket, Docket, Retry, TaskKey


async def hello(
    key: str = TaskKey(),
    docket: Docket = CurrentDocket(),
    retry: Retry = Retry(attempts=sys.maxsize),
):
    async with docket.redis() as redis:
        await redis.zadd("hello:received", {key: time.time()})


async def toxic():
    sys.exit(42)
