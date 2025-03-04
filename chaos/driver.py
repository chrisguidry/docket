import asyncio
import logging
import os
import random
import socket
import sys
from asyncio import subprocess
from asyncio.subprocess import Process
from contextlib import contextmanager
from datetime import timedelta
from typing import Any, Generator, Literal, Sequence
from uuid import uuid4

import redis.exceptions
from docker import DockerClient
from docker.models.containers import Container
from opentelemetry import trace

from docket import Docket
from docket.execution import Operator

from .tasks import toxic

logging.getLogger().setLevel(logging.INFO)

# Quiets down the testcontainers logger
testcontainers_logger = logging.getLogger("testcontainers.core.container")
testcontainers_logger.setLevel(logging.ERROR)
testcontainers_logger = logging.getLogger("testcontainers.core.waiting_utils")
testcontainers_logger.setLevel(logging.ERROR)

console = logging.StreamHandler(stream=sys.stdout)
console.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logging.getLogger().addHandler(console)


logger = logging.getLogger("chaos.driver")
tracer = trace.get_tracer("chaos.driver")


def python_entrypoint() -> list[str]:
    if os.environ.get("OTEL_DISTRO"):
        return ["opentelemetry-instrument", sys.executable]
    return [sys.executable]


@contextmanager
def run_redis(version: str) -> Generator[tuple[str, Container], None, None]:
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
        yield f"redis://localhost:{port}/0", container
    finally:
        container.stop()


async def main(
    mode: Literal["performance", "chaos"] = "chaos",
    tasks: int = 5000,
    producers: int = 4,
    workers: int = 7,
):
    with run_redis("7.4.2") as (redis_url, redis_container):
        logger.info("Redis running at %s", redis_url)
        docket = Docket(
            name=f"test-docket-{uuid4()}",
            url=redis_url,
        )
        environment = {
            **os.environ,
            "DOCKET_NAME": docket.name,
            "DOCKET_URL": redis_url,
        }

        # Add in some random strikes to performance test
        async with docket:
            for _ in range(100):
                parameter = f"param_{random.randint(1, 100)}"
                operator: Operator = random.choice(
                    ["==", "!=", ">", ">=", "<", "<=", "between"]
                )
                value = f"val_{random.randint(1, 1000)}"
                await docket.strike("rando", parameter, operator, value)

        if tasks % producers != 0:
            raise ValueError("total_tasks must be divisible by total_producers")

        tasks_per_producer = tasks // producers

        logger.info(
            "Spawning %d producers with %d tasks each...", producers, tasks_per_producer
        )

        async def spawn_producer() -> Process:
            return await asyncio.create_subprocess_exec(
                *python_entrypoint(),
                "-m",
                "chaos.producer",
                str(tasks_per_producer),
                env=environment | {"OTEL_SERVICE_NAME": "chaos-producer"},
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        producer_processes: list[Process] = []
        for _ in range(producers):
            producer_processes.append(await spawn_producer())

        logger.info("Spawning %d workers...", workers)

        async def spawn_worker() -> Process:
            return await asyncio.create_subprocess_exec(
                *python_entrypoint(),
                "-m",
                "docket",
                "worker",
                "--docket",
                docket.name,
                "--url",
                redis_url,
                "--tasks",
                "chaos.tasks:chaos_tasks",
                env=environment
                | {
                    "OTEL_SERVICE_NAME": "chaos-worker",
                    "DOCKET_WORKER_REDELIVERY_TIMEOUT": "5s",
                },
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        worker_processes: list[Process] = []
        for _ in range(workers):
            worker_processes.append(await spawn_worker())

        while True:
            try:
                async with docket.redis() as r:
                    info: dict[str, Any] = await r.info()
                    connected_clients = int(info.get("connected_clients", 0))

                    sent_tasks = await r.zcard("hello:sent")
                    received_tasks = await r.zcard("hello:received")

                    logger.info(
                        "sent: %d, received: %d, clients: %d",
                        sent_tasks,
                        received_tasks,
                        connected_clients,
                    )
                    if sent_tasks >= tasks:
                        break
            except redis.exceptions.ConnectionError as e:
                logger.error(
                    "driver: Redis connection error (%s), retrying in 5s...", e
                )
                await asyncio.sleep(5)

            # Now apply some chaos to the system:

            if mode in ("chaos",):
                chaos_chance = random.random()
                if chaos_chance < 0.02:
                    logger.warning("CHAOS: Restarting redis server...")
                    redis_container.restart(timeout=2)

                elif chaos_chance < 0.10:
                    worker_index = random.randrange(len(worker_processes))
                    worker_to_kill = worker_processes.pop(worker_index)

                    logger.warning("CHAOS: Killing worker %d...", worker_index)
                    try:
                        worker_to_kill.terminate()
                    except ProcessLookupError:
                        logger.warning("  What is dead may never die!")

                    logger.warning("CHAOS: Replacing worker %d...", worker_index)
                    worker_processes.append(await spawn_worker())
                elif chaos_chance < 0.15:
                    logger.warning("CHAOS: Queuing a toxic task...")
                    try:
                        async with docket:
                            await docket.add(toxic)()
                    except redis.exceptions.ConnectionError:
                        pass

            await asyncio.sleep(0.25)

        async with docket.redis() as r:
            first_entries: Sequence[tuple[bytes, float]] = await r.zrange(
                "hello:received", 0, 0, withscores=True
            )
            last_entries: Sequence[tuple[bytes, float]] = await r.zrange(
                "hello:received", -1, -1, withscores=True
            )

            _, min_score = first_entries[0]
            _, max_score = last_entries[0]
            total_time = timedelta(seconds=max_score - min_score)

            logger.info(
                "Processed %d tasks in %s, averaging %.2f/s",
                tasks,
                total_time,
                tasks / total_time.total_seconds(),
            )

        for process in producer_processes + worker_processes:
            try:
                process.kill()
            except ProcessLookupError:
                continue
            await process.wait()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "chaos"
    assert mode in ("performance", "chaos")
    asyncio.run(main(mode=mode))
