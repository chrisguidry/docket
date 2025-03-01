import asyncio
import logging
import os
import random
import sys
from asyncio import subprocess
from asyncio.subprocess import Process
from datetime import timedelta
from typing import Any, Literal, Sequence
from uuid import uuid4

import redis.exceptions
from opentelemetry import trace
from testcontainers.redis import RedisContainer

from docket import Docket

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


async def main(
    mode: Literal["chaos", "performance"] = "chaos",
    tasks: int = 2000,
    producers: int = 2,
    workers: int = 4,
):
    with RedisContainer("redis:7.4.2") as redis_server:
        redis_url = f"redis://{redis_server.get_container_host_ip()}:{redis_server.get_exposed_port(6379)}"
        docket = Docket(
            name=f"test-docket-{uuid4()}",
            url=redis_url,
        )
        environment = {
            **os.environ,
            "DOCKET_NAME": docket.name,
            "DOCKET_URL": redis_url,
        }

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

            if mode == "chaos":
                chaos_chance = random.random()
                if chaos_chance < 0.01:
                    logger.warning("CHAOS: Killing redis server...")
                    redis_server.stop()

                    await asyncio.sleep(5)

                    logger.warning("CHAOS: Starting redis server...")
                    while True:
                        try:
                            redis_server.start()
                            break
                        except Exception:
                            logger.warning("  Redis server failed, retrying in 5s...")
                            await asyncio.sleep(5)

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
    if len(sys.argv) > 1 and sys.argv[1] == "performance":
        asyncio.run(main(mode="performance"))
    else:
        asyncio.run(main())
