import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from pytest import MonkeyPatch
from typer.testing import CliRunner

from docket import tasks
from docket.cli import app, relative_time
from docket.docket import Docket
from docket.worker import Worker


@pytest.fixture(autouse=True)
async def empty_docket(docket: Docket):
    """Ensure that the docket has been created"""
    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add(tasks.trace, key="initial", when=future)("hi")
    await docket.cancel("initial")


async def test_snapshot_empty_docket(docket: Docket, runner: CliRunner):
    """Should show an empty snapshot when no tasks are scheduled"""
    result = await asyncio.get_running_loop().run_in_executor(
        None,
        runner.invoke,
        app,
        [
            "snapshot",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        ],
    )
    assert result.exit_code == 0, result.output

    assert "0 workers, 0/0 running" in result.output


async def test_snapshot_with_scheduled_tasks(docket: Docket, runner: CliRunner):
    """Should show scheduled tasks in the snapshot"""
    when = datetime.now(timezone.utc) + timedelta(seconds=5)
    await docket.add(tasks.trace, when=when, key="future-task")("hiya!")

    result = await asyncio.get_running_loop().run_in_executor(
        None,
        runner.invoke,
        app,
        [
            "snapshot",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        ],
    )
    assert result.exit_code == 0, result.output

    assert "0 workers, 0/1 running" in result.output
    assert "future-task" in result.output


async def test_snapshot_with_running_tasks(docket: Docket, runner: CliRunner):
    """Should show running tasks in the snapshot"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat

    await docket.add(tasks.sleep)(1)

    async with Worker(docket, name="test-worker") as worker:
        worker_running = asyncio.create_task(worker.run_until_finished())

        await asyncio.sleep(0.1)

        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                "snapshot",
                "--url",
                docket.url,
                "--docket",
                docket.name,
            ],
        )
        assert result.exit_code == 0, result.output

        assert "1 workers, 1/1 running" in result.output
        assert "sleep" in result.output
        assert "test-worker" in result.output

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
