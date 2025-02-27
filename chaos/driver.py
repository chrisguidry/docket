import asyncio
import random
import sys
from asyncio import subprocess
from asyncio.subprocess import Process
from datetime import timedelta
from typing import Any, Literal, Sequence
from uuid import uuid4

import redis.exceptions
from testcontainers.redis import RedisContainer

from docket import Docket

from .tasks import toxic


async def main(
    mode: Literal["chaos", "performance"] = "chaos",
    tasks: int = 10000,
    producers: int = 5,
    workers: int = 10,
):
    with RedisContainer("redis:7.4.2") as redis_server:
        docket = Docket(
            name=f"test-docket-{uuid4()}",
            host=redis_server.get_container_host_ip(),
            port=redis_server.get_exposed_port(6379),
            db=0,
        )
        environment = {
            "CHAOS_DOCKET_NAME": docket.name,
            "CHAOS_REDIS_HOST": docket.host,
            "CHAOS_REDIS_PORT": str(docket.port),
            "CHAOS_REDIS_DB": str(docket.db),
        }

        if tasks % producers != 0:
            raise ValueError("total_tasks must be divisible by total_producers")

        tasks_per_producer = tasks // producers

        print(f"Spawning {producers} producers with {tasks_per_producer} tasks each...")

        async def spawn_producer() -> Process:
            return await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "chaos.producer",
                str(tasks_per_producer),
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        producer_processes: list[Process] = []
        for _ in range(producers):
            producer_processes.append(await spawn_producer())

        print(f"Spawning {workers} workers...")

        async def spawn_worker() -> Process:
            return await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "chaos.worker",
                env=environment,
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

                    print(
                        f"sent: {sent_tasks}, "
                        f"received: {received_tasks}, "
                        f"clients: {connected_clients}"
                    )
                    if sent_tasks >= tasks:
                        break
            except redis.exceptions.ConnectionError as e:
                print(f"driver: Redis connection error ({e}), retrying in 5s...")
                await asyncio.sleep(5)

            # Now apply some chaos to the system:

            if mode == "chaos":
                chaos_chance = random.random()
                if chaos_chance < 0.01:
                    print("CHAOS: Killing redis server...")
                    redis_server.stop()

                    await asyncio.sleep(5)

                    print("CHAOS: Starting redis server...")
                    while True:
                        try:
                            redis_server.start()
                            break
                        except Exception:
                            print("  Redis server failed, retrying in 5s...")
                            await asyncio.sleep(5)

                elif chaos_chance < 0.10:
                    worker_index = random.randrange(len(worker_processes))
                    worker_to_kill = worker_processes.pop(worker_index)

                    print(f"CHAOS: Killing worker {worker_index}...")
                    try:
                        worker_to_kill.terminate()
                    except ProcessLookupError:
                        print("  What is dead may never die!")

                    print(f"CHAOS: Replacing worker {worker_index}...")
                    worker_processes.append(await spawn_worker())
                elif chaos_chance < 0.15:
                    print("CHAOS: Queuing a toxic task...")
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

            print(
                f"Processed {tasks} tasks in {total_time}, "
                f"averaging {tasks / total_time.total_seconds():.2f}/s"
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
