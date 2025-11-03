from datetime import datetime, timedelta, timezone
from docket import Docket, Progress, Worker
import asyncio
import rich.console

from docket.execution import ExecutionProgress

from .common import run_redis


async def long_task(progress: ExecutionProgress = Progress()) -> None:
    for i in range(1, 101):
        await asyncio.sleep(1)
        await progress.increment()
        if i % 10 == 0:
            await progress.set_message(f"{i} splines retriculated")


tasks = [long_task]
console = rich.console.Console()


async def main():
    async with run_redis("7.4.2") as redis_url:
        async with Docket(name="task-progress", url=redis_url) as docket:
            async with Worker(docket, name="task-progress") as worker:
                docket.register(long_task)
                in_twenty_seconds = datetime.now(timezone.utc) + timedelta(seconds=20)
                execution = await docket.add(
                    long_task, key="long-task", when=in_twenty_seconds
                )()
                console.print(f"Execution {execution.key} started!")
                console.print(
                    f"Run [blue]docket watch --url {redis_url} --docket {docket.name} {execution.key}[/blue] to see the progress!"
                )
                await worker.run_until_finished()


if __name__ == "__main__":
    asyncio.run(main())
