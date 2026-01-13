from datetime import datetime, timedelta, timezone
import os
from types import TracebackType

import asyncio
import pytest
from pytest import MonkeyPatch
from rich.table import Table

from docket import Execution, tasks
from docket.cli import relative_time
from docket.cli import snapshot as snapshot_command
from docket.docket import Docket, DocketSnapshot
from docket.worker import Worker
from tests._key_leak_checker import KeyCountChecker
from tests.cli.run import run_cli

# Skip CLI tests when using memory backend since CLI rejects memory:// URLs
pytestmark = pytest.mark.skipif(
    os.environ.get("REDIS_VERSION") == "memory",
    reason="CLI commands require a persistent Redis backend",
)


@pytest.fixture(autouse=True)
async def empty_docket(docket: Docket):
    """Ensure that the docket has been created"""
    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add(tasks.trace, key="initial", when=future)("hi")
    await docket.cancel("initial")


async def test_snapshot_empty_docket(docket: Docket):
    """Should show an empty snapshot when no tasks are scheduled"""
    result = await run_cli(
        "snapshot",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output

    assert "0 workers, 0/0 running" in result.output, result.output


async def test_snapshot_with_scheduled_tasks(docket: Docket):
    """Should show scheduled tasks in the snapshot"""
    when = datetime.now(timezone.utc) + timedelta(seconds=5)
    await docket.add(tasks.trace, when=when, key="future-task")("hiya!")

    result = await run_cli(
        "snapshot",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output

    assert "0 workers, 0/1 running" in result.output, result.output
    assert "future-task" in result.output, result.output


async def test_snapshot_with_running_tasks(
    docket: Docket, key_leak_checker: KeyCountChecker
):
    """Should show running tasks in the snapshot"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat

    # Use tasks.sleep for CLI tests since the subprocess needs importable tasks
    execution = await docket.add(tasks.sleep)(5)

    # This test cancels worker mid-execution, leaving incomplete tasks
    key_leak_checker.add_exemption(f"{docket.name}:runs:{execution.key}")

    async with Worker(docket, name="test-worker") as worker:
        worker_running = asyncio.create_task(worker.run_until_finished())

        await asyncio.sleep(0.05)  # Let worker pick up task

        result = await run_cli(
            "snapshot",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        )
        assert result.exit_code == 0, result.output

        assert "1 workers, 1/1 running" in result.output, result.output
        assert "sleep" in result.output, result.output
        assert "test-worker" in result.output, result.output

        worker_running.cancel()
        await worker_running


async def test_snapshot_with_mixed_tasks(
    docket: Docket, key_leak_checker: KeyCountChecker
):
    """Should show both running and scheduled tasks in the snapshot"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat

    future = datetime.now(timezone.utc) + timedelta(seconds=5)
    execution1 = await docket.add(tasks.trace, when=future)("hi!")

    # Use tasks.sleep for CLI tests since the subprocess needs importable tasks
    executions: list[Execution] = []
    for _ in range(5):
        executions.append(await docket.add(tasks.sleep)(4))

    # This test cancels worker mid-execution, leaving incomplete tasks
    key_leak_checker.add_exemption(f"{docket.name}:runs:{execution1.key}")
    for ex in executions:
        key_leak_checker.add_exemption(f"{docket.name}:runs:{ex.key}")

    async with Worker(docket, name="test-worker", concurrency=2) as worker:
        worker_running = asyncio.create_task(worker.run_until_finished())

        await asyncio.sleep(0.1)  # Let worker pick up tasks

        result = await run_cli(
            "snapshot",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        )
        assert result.exit_code == 0, result.output

        assert "1 workers, 2/6 running" in result.output, result.output
        assert "sleep" in result.output, result.output
        assert "test-worker" in result.output, result.output
        assert "trace" in result.output, result.output

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

    monkeypatch.setattr("docket._cli_support.local_time", consistent_format)

    assert relative_time(now, when) == expected


async def test_snapshot_with_stats_flag_empty(docket: Docket):
    """Should show empty stats when no tasks are scheduled"""
    result = await run_cli(
        "snapshot",
        "--stats",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output

    # Should still show the normal summary
    assert "0 workers, 0/0 running" in result.output, result.output
    # With empty docket, stats table shouldn't appear since there are no tasks


async def test_snapshot_with_stats_flag_mixed_tasks(
    docket: Docket, key_leak_checker: KeyCountChecker
):
    """Should show task count statistics when --stats flag is used"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat

    # Add multiple tasks of different types
    future = datetime.now(timezone.utc) + timedelta(seconds=5)
    await docket.add(tasks.trace, when=future, key="trace-1")("hi!")
    await docket.add(tasks.trace, when=future, key="trace-2")("hello!")

    # This test cancels worker mid-execution, leaving incomplete tasks
    key_leak_checker.add_exemption(f"{docket.name}:runs:trace-1")
    key_leak_checker.add_exemption(f"{docket.name}:runs:trace-2")

    # Use tasks.sleep for CLI tests since the subprocess needs importable tasks
    for i in range(3):
        await docket.add(tasks.sleep, key=f"sleep-{i}")(4)
        key_leak_checker.add_exemption(f"{docket.name}:runs:sleep-{i}")

    async with Worker(docket, name="test-worker", concurrency=2) as worker:
        worker_running = asyncio.create_task(worker.run_until_finished())

        await asyncio.sleep(0.1)  # Let worker pick up tasks

        result = await run_cli(
            "snapshot",
            "--stats",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        )
        assert result.exit_code == 0, result.output

        # Should show the normal summary
        assert "1 workers, 2/5 running" in result.output, result.output

        # Should show task statistics table with enhanced columns
        assert "Task Count Statistics by Function" in result.output
        assert "Function" in result.output
        assert "Total" in result.output
        assert "Running" in result.output
        assert "Queued" in result.output
        assert "Oldest Queued" in result.output
        assert "Latest Queued" in result.output

        # Should show the task counts
        assert "sleep" in result.output
        assert "trace" in result.output

        worker_running.cancel()
        await worker_running


async def test_snapshot_with_stats_shows_timestamp_columns(docket: Docket):
    """Should show oldest and latest queued timestamps in stats table"""
    # Add multiple tasks with different scheduled times
    now = datetime.now(timezone.utc)
    early_time = now + timedelta(seconds=1)
    late_time = now + timedelta(minutes=2)

    await docket.add(tasks.trace, when=early_time)("early task")
    await docket.add(tasks.trace, when=late_time)("late task")
    await docket.add(tasks.sleep, when=early_time)(1)

    result = await run_cli(
        "snapshot",
        "--stats",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output

    # Should show enhanced stats table with timestamp columns
    assert "Task Count Statistics by Function" in result.output
    assert "Oldest Queued" in result.output
    assert "Latest Queued" in result.output

    # Should show the task functions
    assert "trace" in result.output
    assert "sleep" in result.output


async def test_snapshot_stats_with_running_tasks_only(docket: Docket):
    """Should handle stats display correctly when tasks are running but none queued"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat

    # Add tasks that will be picked up immediately by worker
    # Use tasks.sleep for CLI tests since the subprocess needs importable tasks
    await docket.add(tasks.sleep)(5)
    await docket.add(tasks.sleep)(5)

    async with Worker(docket, name="test-worker", concurrency=2) as worker:
        worker_running = asyncio.create_task(worker.run_until_finished())

        await asyncio.sleep(0.1)  # Let worker pick up tasks

        result = await run_cli(
            "snapshot",
            "--stats",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        )
        assert result.exit_code == 0, result.output

        # Should show stats table even with no queued tasks
        assert "Task Count Statistics by Function" in result.output
        assert "Oldest Queued" in result.output
        assert "Latest Queued" in result.output
        assert "sleep" in result.output

        worker_running.cancel()
        await worker_running


class _RecordingConsole:
    def __init__(self) -> None:
        self.objects: list[object] = []

    def print(self, obj: object = "") -> None:
        self.objects.append(obj)


def test_snapshot_cli_stats_skips_table_when_empty(monkeypatch: MonkeyPatch) -> None:
    """If stats requested but no tasks, the stats table should not render."""

    now = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    snapshot_obj = DocketSnapshot(
        taken=now,
        total_tasks=0,
        future=[],
        running=[],
        workers=[],
    )

    class EmptyDocket:
        def __init__(self, name: str, url: str) -> None:
            self.name = name
            self.url = url

        async def __aenter__(self) -> "EmptyDocket":
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> bool:
            return False

        def register_collection(self, task_path: str) -> None:
            pass

        async def snapshot(self) -> DocketSnapshot:
            return snapshot_obj

    monkeypatch.setattr("docket.cli.Docket", EmptyDocket)

    recorder = _RecordingConsole()
    monkeypatch.setattr("docket.cli.Console", lambda: recorder)

    snapshot_command(stats=True, docket_="demo", url="memory://")

    tables = [obj for obj in recorder.objects if isinstance(obj, Table)]
    assert len(tables) == 1
    assert str(tables[0].title).startswith("Docket:")
