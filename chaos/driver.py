import asyncio
import logging
import os
import random
import sys
import urllib.request
from asyncio import subprocess
from asyncio.subprocess import Process
from datetime import timedelta
from typing import Any, Literal, Sequence
from urllib.error import HTTPError
from uuid import uuid4

import redis.exceptions
from opentelemetry import trace

from docket import Docket
from docket.strikelist import Operator

from .redis import run_redis
from .tasks import toxic


def package_exists_on_pypi(package: str, version: str) -> bool:
    """Check if a package version exists on PyPI."""
    url = f"https://pypi.org/pypi/{package}/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.status == 200
    except HTTPError as e:
        if e.code == 404:
            return False
        raise


logging.getLogger().setLevel(logging.INFO)

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


async def setup_environments(
    base_version: str,
) -> tuple[list[str], list[str]]:
    """Create two virtual environments: one for base version, one for main.

    Returns:
        Tuple of (base_python_command, main_python_command) lists ready for use with create_subprocess_exec.
    """
    import tempfile
    from pathlib import Path

    temp_dir = Path(tempfile.gettempdir()) / f"docket-chaos-{uuid4()}"
    temp_dir.mkdir(parents=True, exist_ok=True)

    base_venv = temp_dir / "base"
    main_venv = temp_dir / "main"

    logger.info("Setting up base environment with pydocket %s...", base_version)
    process = await asyncio.create_subprocess_exec(
        "uv",
        "venv",
        str(base_venv),
        "--python",
        sys.executable,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    await process.wait()

    process = await asyncio.create_subprocess_exec(
        "uv",
        "pip",
        "install",
        "--python",
        str(base_venv / "bin" / "python"),
        f"pydocket=={base_version}",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    await process.wait()

    logger.info("Setting up main environment with current pydocket...")
    process = await asyncio.create_subprocess_exec(
        "uv",
        "venv",
        str(main_venv),
        "--python",
        sys.executable,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    await process.wait()

    process = await asyncio.create_subprocess_exec(
        "uv",
        "pip",
        "install",
        "--python",
        str(main_venv / "bin" / "python"),
        "-e",
        ".",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    await process.wait()

    base_python = python_entrypoint()
    if base_python[0] == "opentelemetry-instrument":
        base_command = [base_python[0], str(base_venv / "bin" / "python")]
        main_command = [base_python[0], str(main_venv / "bin" / "python")]
    else:
        base_command = [str(base_venv / "bin" / "python")]
        main_command = [str(main_venv / "bin" / "python")]

    logger.info("Environment setup complete")
    return base_command, main_command


async def main(
    mode: Literal["performance", "chaos"] = "chaos",
    tasks: int = 20000,
    producers: int = 5,
    workers: int = 10,
    base_version: str | None = None,
):
    if base_version is None:
        process = await asyncio.create_subprocess_exec(
            "git",
            "describe",
            "--tags",
            "--abbrev=0",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, _ = await process.communicate()
        base_version = stdout.decode("utf-8").strip()

    if not package_exists_on_pypi("pydocket", base_version):
        logger.error(
            "pydocket %s is not available on PyPI yet - "
            "cannot run chaos tests until the release is published",
            base_version,
        )
        sys.exit(1)

    base_python_command, main_python_command = await setup_environments(base_version)

    async with (
        run_redis("7.4.2") as (redis_url, redis_container),
        Docket(
            name=f"test-docket-{uuid4()}",
            url=redis_url,
        ) as docket,
    ):
        logger.info("Redis running at %s", redis_url)
        environment = {
            **os.environ,
            "DOCKET_NAME": docket.name,
            "DOCKET_URL": redis_url,
        }

        # Add in some random strikes to performance test
        for _ in range(100):
            parameter = f"param_{random.randint(1, 100)}"
            operator = random.choice(list(Operator))
            value = f"val_{random.randint(1, 1000)}"
            await docket.strike("rando", parameter, operator, value)

        if tasks % producers != 0:
            raise ValueError("total_tasks must be divisible by total_producers")

        tasks_per_producer = tasks // producers

        logger.info(
            "Spawning %d producers with %d tasks each...", producers, tasks_per_producer
        )

        async def spawn_producer() -> Process:
            use_base = random.random() < 0.5
            python_command = base_python_command if use_base else main_python_command
            version_label = base_version if use_base else "main"
            logger.info("Using pydocket %s for producer", version_label)

            command = [*python_command, "-m", "chaos.producer", str(tasks_per_producer)]
            return await asyncio.create_subprocess_exec(
                *command,
                env=environment | {"OTEL_SERVICE_NAME": "chaos-producer"},
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        producer_processes: list[Process] = []
        for _ in range(producers):
            producer_processes.append(await spawn_producer())

        logger.info("Spawning %d workers...", workers)

        async def spawn_worker() -> Process:
            use_base = random.random() < 0.5
            python_command = base_python_command if use_base else main_python_command
            version_label = base_version if use_base else "main"
            logger.info("Using pydocket %s for worker", version_label)

            command = [
                *python_command,
                "-m",
                "docket",
                "worker",
                "--docket",
                docket.name,
                "--url",
                redis_url,
                "--tasks",
                "chaos.tasks:chaos_tasks",
                "--redelivery-timeout",
                "5s",
            ]
            return await asyncio.create_subprocess_exec(
                *command,
                env=environment | {"OTEL_SERVICE_NAME": "chaos-worker"},
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        worker_processes: list[Process] = []
        for _ in range(workers):
            worker_processes.append(await spawn_worker())

        while True:
            try:
                async with docket.redis() as r:
                    info: dict[str, Any] = await r.info()  # type: ignore[reportUnknownMemberType]
                    connected_clients = int(info.get("connected_clients", 0))

                    sent_tasks = await r.zcard("hello:sent")
                    received_tasks = await r.zcard("hello:received")

                    stream_length = await r.xlen(docket.stream_key)
                    pending = await r.xpending(
                        docket.stream_key, docket.worker_group_name
                    )

                    logger.info(
                        "sent: %d, received: %d, stream: %d, pending: %d, clients: %d",
                        sent_tasks,
                        received_tasks,
                        stream_length,
                        pending["pending"],
                        connected_clients,
                    )
                    if sent_tasks >= tasks and received_tasks >= sent_tasks:
                        break
            except redis.exceptions.ConnectionError as e:
                logger.error(
                    "driver: Redis connection error (%s), retrying in 5s...", e
                )
                await asyncio.sleep(5)
            except redis.exceptions.ResponseError as e:
                if "NOGROUP" in str(e):
                    # Consumer group not created yet, workers haven't started
                    logger.debug("driver: Consumer group not yet created, waiting...")
                    await asyncio.sleep(1)
                else:
                    raise

            # Now apply some chaos to the system:

            if mode in ("chaos",):
                chaos_chance = random.random()
                if chaos_chance < 0.02:
                    logger.warning("CHAOS: Restarting redis server...")
                    redis_container.restart(timeout=2)  # type: ignore[reportUnknownMemberType]

                elif chaos_chance < 0.10:
                    worker_index = random.randrange(len(worker_processes))
                    worker_to_kill = worker_processes[worker_index]

                    logger.warning("CHAOS: Killing worker %d...", worker_index)
                    try:
                        worker_to_kill.kill()
                    except ProcessLookupError:
                        logger.warning("  What is dead may never die!")
                elif chaos_chance < 0.15:
                    logger.warning("CHAOS: Queuing a toxic task...")
                    try:
                        await docket.add(toxic)()
                    except redis.exceptions.ConnectionError:
                        pass

            # Check if any worker processes have died and replace them
            for i in range(len(worker_processes)):
                process = worker_processes[i]
                if process.returncode is not None:
                    logger.warning(
                        "Worker %d has died with code %d, replacing it...",
                        i,
                        process.returncode,
                    )
                    worker_processes[i] = await spawn_worker()

            await asyncio.sleep(0.25)

        async with docket.redis() as r:
            first_entries: Sequence[tuple[bytes, float]] = await r.zrange(  # type: ignore[reportUnknownMemberType]
                "hello:received", 0, 0, withscores=True
            )
            last_entries: Sequence[tuple[bytes, float]] = await r.zrange(  # type: ignore[reportUnknownMemberType]
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
    tasks = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
    assert mode in ("performance", "chaos")
    asyncio.run(main(mode=mode, tasks=tasks))
