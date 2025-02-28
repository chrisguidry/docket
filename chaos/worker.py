import asyncio
import logging
import os
from datetime import timedelta

from docket import Docket, Worker

from .tasks import hello, toxic

logging.getLogger().setLevel(logging.INFO)


async def main():
    docket = Docket(
        name=os.environ["CHAOS_DOCKET_NAME"],
        host=os.environ["CHAOS_REDIS_HOST"],
        port=int(os.environ["CHAOS_REDIS_PORT"]),
        db=int(os.environ["CHAOS_REDIS_DB"]),
    )
    async with docket:
        docket.register(hello)
        docket.register(toxic)

        async with Worker(docket, redelivery_timeout=timedelta(seconds=5)) as worker:
            await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
