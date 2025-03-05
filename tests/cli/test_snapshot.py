import asyncio
import sys
from datetime import datetime, timedelta, timezone

import pytest
from pytest import MonkeyPatch

from docket import tasks
from docket.cli import relative_time
from docket.docket import Docket
from docket.worker import Worker


@pytest.fixture(autouse=True)
async def empty_docket(docket: Docket):
    """Ensure that the docket has been created"""
    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add(tasks.trace, key="initial", when=future)("hi")
    await docket.cancel("initial")


async def test_snapshot_empty_docket(docket: Docket):
    """Should show an empty snapshot when no tasks are scheduled"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "docket",
        "snapshot",
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

    assert "0 workers, 0/0 running" in output_text


async def test_snapshot_with_scheduled_tasks(docket: Docket):
    """Should show scheduled tasks in the snapshot"""
    when = datetime.now(timezone.utc) + timedelta(seconds=5)
    await docket.add(tasks.trace, when=when, key="future-task")("hiya!")

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "docket",
        "snapshot",
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

    assert "0 workers, 0/1 running" in output_text
    assert "future-task" in output_text


async def test_snapshot_with_running_tasks(docket: Docket):
    """Should show running tasks in the snapshot"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat

    await docket.add(tasks.sleep)(2)

    async with Worker(docket, name="test-worker") as worker:
        worker_running = asyncio.create_task(worker.run_until_finished())

        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "docket",
            "snapshot",
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

        assert "1 workers, 1/1 running" in output_text
        assert "sleep" in output_text
        assert "test-worker" in output_text

        worker_running.cancel()
        await worker_running


async def test_snapshot_with_mixed_tasks(docket: Docket):
    """Should show both running and scheduled tasks in the snapshot"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat

    future = datetime.now(timezone.utc) + timedelta(seconds=5)
    await docket.add(tasks.trace, when=future)("hi!")
    for _ in range(5):  # more than the concurrency allows
        await docket.add(tasks.sleep)(2)

    async with Worker(docket, name="test-worker", concurrency=2) as worker:
        worker_running = asyncio.create_task(worker.run_until_finished())

        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "docket",
            "snapshot",
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

        print(output_text)

        assert "1 workers, 2/6 running" in output_text
        assert "sleep" in output_text
        assert "test-worker" in output_text
        assert "trace" in output_text

        worker_running.cancel()
        await worker_running


@pytest.mark.parametrize(
    "now, when, expected",
    [
        # Near future
        (
            datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 1, 12, 15, 0, tzinfo=timezone.utc),
            "in 0:15:00",
        ),
        # Distant future
        (
            datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            "at 2023-01-02 12:00:00 +0000",
        ),
        # Recent past
        (
            datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 1, 11, 45, 0, tzinfo=timezone.utc),
            "0:15:00 ago",
        ),
        # Distant past
        (
            datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            "at 2023-01-01 10:00:00 +0000",
        ),
    ],
)
def test_relative_time(
    now: datetime, when: datetime, expected: str, monkeypatch: MonkeyPatch
):
    """Should format relative times correctly based on the time difference"""

    def consistent_format(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S %z")

    monkeypatch.setattr("docket.cli.local_time", consistent_format)

    assert relative_time(now, when) == expected
