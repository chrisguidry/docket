import asyncio
import subprocess
from uuid import uuid4

from docket.docket import Docket


async def test_strike(redis_url: str):
    """Should strike a task"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        process = await asyncio.create_subprocess_exec(
            "docket",
            "strike",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            "example_task",
            "some_parameter",
            "==",
            "some_value",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert await process.wait() == 0

        await asyncio.sleep(0.25)

        assert "example_task" in docket.strike_list.task_strikes


async def test_restore(redis_url: str):
    """Should restore a task"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        await docket.strike("example_task", "some_parameter", "==", "some_value")
        assert "example_task" in docket.strike_list.task_strikes

        process = await asyncio.create_subprocess_exec(
            "docket",
            "restore",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            "example_task",
            "some_parameter",
            "==",
            "some_value",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert await process.wait() == 0

        await asyncio.sleep(0.25)

        assert "example_task" not in docket.strike_list.task_strikes
