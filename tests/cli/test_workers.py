import asyncio
import os
from datetime import timedelta

import pytest

from docket.docket import Docket
from docket.worker import Worker

from tests.cli.run import run_cli

# Skip CLI tests when using memory backend since CLI rejects memory:// URLs
pytestmark = pytest.mark.skipif(
    os.environ.get("REDIS_VERSION") == "memory",
    reason="CLI commands require a persistent Redis backend",
)


async def test_list_workers_command(docket: Docket):
    """Should list all active workers"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    async with Worker(docket, name="worker-1"), Worker(docket, name="worker-2"):
        await asyncio.sleep(heartbeat.total_seconds() * 5)

        result = await run_cli(
            "workers",
            "ls",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        )
        assert result.exit_code == 0, result.output

        assert "worker-1" in result.output
        assert "worker-2" in result.output


async def test_list_workers_for_task(docket: Docket):
    """Should list workers that can handle a specific task"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat
    docket.missed_heartbeats = 3

    async with Worker(docket, name="worker-1"), Worker(docket, name="worker-2"):
        await asyncio.sleep(heartbeat.total_seconds() * 5)

        result = await run_cli(
            "workers",
            "for-task",
            "trace",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        )
        assert result.exit_code == 0, result.output

        assert "worker-1" in result.output
        assert "worker-2" in result.output
