import asyncio
import os
import socket
from contextlib import asynccontextmanager
from datetime import timedelta
from logging import Logger, LoggerAdapter
from typing import Annotated, AsyncGenerator

from docker import DockerClient

from docket import Docket
from docket.annotations import Logged
from docket.dependencies import CurrentDocket, Perpetual, TaskLogger


@asynccontextmanager
async def run_redis(version: str) -> AsyncGenerator[str, None]:
    def get_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            return s.getsockname()[1]

    port = get_free_port()

    client = DockerClient.from_env()
    container = client.containers.run(
        f"redis:{version}",
        detach=True,
        ports={"6379/tcp": port},
        auto_remove=True,
    )

    # Wait for Redis to be ready
    for line in container.logs(stream=True):
        if b"Ready to accept connections" in line:
            break

    try:
        yield f"redis://localhost:{port}/0"
    finally:
        container.stop()


async def main():
    async with run_redis("7.4.2") as redis_url:
        print("***** Redis is running on %s", redis_url)
        processes = [
            await asyncio.create_subprocess_exec(
                "docket",
                "worker",
                "--name",
                f"worker-{i}",
                "--url",
                redis_url,
                "--tasks",
                "find_and_flood:tasks",
                "--concurrency",
                "5",
                env={
                    **os.environ,
                    "PYTHONPATH": os.path.abspath(
                        os.path.join(os.path.dirname(__file__))
                    ),
                },
            )
            for i in range(3)
        ]
        try:
            await asyncio.gather(*[p.wait() for p in processes])
        except asyncio.CancelledError:
            for p in processes:
                p.kill()
                try:
                    await p.wait()
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass


async def find(
    docket: Docket = CurrentDocket(),
    logger: LoggerAdapter[Logger] = TaskLogger(),
    perpetual: Perpetual = Perpetual(every=timedelta(seconds=10), automatic=True),
) -> None:
    for i in range(1, 10 + 1):
        await docket.add(flood, key=str(i))(i)


async def flood(
    item: Annotated[int, Logged],
    logger: LoggerAdapter[Logger] = TaskLogger(),
) -> None:
    logger.info("Working on %s", item)


tasks = [find, flood]


if __name__ == "__main__":
    asyncio.run(main())
