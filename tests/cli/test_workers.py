import asyncio
import sys
from datetime import timedelta

from docket.docket import Docket
from docket.worker import Worker


async def test_list_workers_command(docket: Docket):
    """Should list all active workers"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    async with Worker(docket, name="worker-1"), Worker(docket, name="worker-2"):
        await asyncio.sleep(heartbeat.total_seconds() * 5)

        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "docket",
            "workers",
            "ls",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await process.wait()

        assert process.stderr
        stderr = await process.stderr.read()
        assert process.returncode == 0, stderr.decode()

        assert process.stdout
        output = await process.stdout.read()
        output_text = output.decode()

        assert "worker-1" in output_text
        assert "worker-2" in output_text


async def test_list_workers_for_task(docket: Docket):
    """Should list workers that can handle a specific task"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    async with Worker(docket, name="worker-1"), Worker(docket, name="worker-2"):
        await asyncio.sleep(heartbeat.total_seconds() * 5)

        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "docket",
            "workers",
            "for-task",
            "trace",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await process.wait()

        assert process.stderr
        stderr = await process.stderr.read()
        assert process.returncode == 0, stderr.decode()

        assert process.stdout
        output = await process.stdout.read()
        output_text = output.decode()

        assert "worker-1" in output_text
        assert "worker-2" in output_text
