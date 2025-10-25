import asyncio
from datetime import timedelta

from typer.testing import CliRunner

from docket.cli import app
from docket.docket import Docket
from docket.worker import Worker


async def test_list_workers_command(docket: Docket, runner: CliRunner):
    """Should list all active workers"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    async with Worker(docket, name="worker-1"), Worker(docket, name="worker-2"):
        await asyncio.sleep(heartbeat.total_seconds() * 5)

        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                "workers",
                "ls",
                "--url",
                docket.url,
                "--docket",
                docket.name,
            ],
        )
        assert result.exit_code == 0, result.output

        assert "worker-1" in result.output
        assert "worker-2" in result.output


async def test_list_workers_for_task(docket: Docket, runner: CliRunner):
    """Should list workers that can handle a specific task"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    async with Worker(docket, name="worker-1"), Worker(docket, name="worker-2"):
        await asyncio.sleep(heartbeat.total_seconds() * 5)

        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                "workers",
                "for-task",
                "trace",
                "--url",
                docket.url,
                "--docket",
                docket.name,
            ],
        )
        assert result.exit_code == 0, result.output

        assert "worker-1" in result.output
        assert "worker-2" in result.output
