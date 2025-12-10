import asyncio
import os
import socket
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from docker import DockerClient


@asynccontextmanager
async def run_redis(version: str) -> AsyncGenerator[str, None]:
    def get_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
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

    url = f"redis://localhost:{port}/0"
    print(f"***** Redis is running on {url} *****")
    try:
        yield url
    finally:
        container.stop()


async def run_example_workers(workers: int, concurrency: int, tasks: str):
    async with run_redis("7.4.2") as redis_url:
        processes = [
            await asyncio.create_subprocess_exec(
                "docket",
                "worker",
                "--url",
                redis_url,
                "--tasks",
                tasks,
                "--concurrency",
                str(concurrency),
                env={
                    **os.environ,
                    "PYTHONPATH": os.path.abspath(
                        os.path.join(os.path.dirname(__file__), "..")
                    ),
                },
            )
            for i in range(workers)
        ]
        try:
            await asyncio.gather(*[p.wait() for p in processes])
        except asyncio.CancelledError:
            for p in processes:
                p.kill()
        finally:
            await asyncio.gather(*[p.wait() for p in processes])
