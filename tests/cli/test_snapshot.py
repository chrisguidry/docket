import asyncio
from datetime import datetime, timedelta, timezone
from types import TracebackType

import pytest
from pytest import MonkeyPatch
from rich.table import Table
from typer.testing import CliRunner

from docket import tasks
from docket.cli import app, relative_time, snapshot as snapshot_command
from docket.docket import Docket, DocketSnapshot, RunningExecution, WorkerInfo
from docket.execution import Execution
from docket.worker import Worker


@pytest.fixture(autouse=True)
async def empty_docket(docket: Docket, aiolib: str):
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

        worker_running.cancel()
        await worker_running


async def test_snapshot_with_mixed_tasks(docket: Docket, runner: CliRunner):
    """Should show both running and scheduled tasks in the snapshot"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat

    future = datetime.now(timezone.utc) + timedelta(seconds=5)
    await docket.add(tasks.trace, when=future)("hi!")
    for _ in range(5):  # more than the concurrency allows
        await docket.add(tasks.sleep)(2)

    async with Worker(docket, name="test-worker", concurrency=2) as worker:
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

        assert "1 workers, 2/6 running" in result.output
        assert "sleep" in result.output
        assert "test-worker" in result.output
        assert "trace" in result.output

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


async def test_snapshot_with_stats_flag_empty(docket: Docket, runner: CliRunner):
    """Should show empty stats when no tasks are scheduled"""
    result = await asyncio.get_running_loop().run_in_executor(
        None,
        runner.invoke,
        app,
        [
            "snapshot",
            "--stats",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        ],
    )
    assert result.exit_code == 0, result.output

    # Should still show the normal summary
    assert "0 workers, 0/0 running" in result.output
    # With empty docket, stats table shouldn't appear since there are no tasks


async def test_snapshot_with_stats_flag_mixed_tasks(docket: Docket, runner: CliRunner):
    """Should show task count statistics when --stats flag is used"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat

    # Add multiple tasks of different types
    future = datetime.now(timezone.utc) + timedelta(seconds=5)
    await docket.add(tasks.trace, when=future)("hi!")
    await docket.add(tasks.trace, when=future)("hello!")
    for _ in range(3):
        await docket.add(tasks.sleep)(2)

    async with Worker(docket, name="test-worker", concurrency=2) as worker:
        worker_running = asyncio.create_task(worker.run_until_finished())

        await asyncio.sleep(0.1)

        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                "snapshot",
                "--stats",
                "--url",
                docket.url,
                "--docket",
                docket.name,
            ],
        )
        assert result.exit_code == 0, result.output

        # Should show the normal summary
        assert "1 workers, 2/5 running" in result.output

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


async def test_snapshot_with_stats_shows_timestamp_columns(
    docket: Docket, runner: CliRunner
):
    """Should show oldest and latest queued timestamps in stats table"""
    # Add multiple tasks with different scheduled times
    now = datetime.now(timezone.utc)
    early_time = now + timedelta(seconds=1)
    late_time = now + timedelta(minutes=2)

    await docket.add(tasks.trace, when=early_time)("early task")
    await docket.add(tasks.trace, when=late_time)("late task")
    await docket.add(tasks.sleep, when=early_time)(1)

    result = await asyncio.get_running_loop().run_in_executor(
        None,
        runner.invoke,
        app,
        [
            "snapshot",
            "--stats",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        ],
    )
    assert result.exit_code == 0, result.output

    # Should show enhanced stats table with timestamp columns
    assert "Task Count Statistics by Function" in result.output
    assert "Oldest Queued" in result.output
    assert "Latest Queued" in result.output

    # Should show the task functions
    assert "trace" in result.output
    assert "sleep" in result.output


async def test_snapshot_stats_with_running_tasks_only(
    docket: Docket, runner: CliRunner
):
    """Should handle stats display correctly when tasks are running but none queued"""
    heartbeat = timedelta(milliseconds=20)
    docket.heartbeat_interval = heartbeat

    # Add tasks that will be picked up immediately by worker
    await docket.add(tasks.sleep)(0.5)
    await docket.add(tasks.sleep)(0.5)

    async with Worker(docket, name="test-worker", concurrency=2) as worker:
        worker_running = asyncio.create_task(worker.run_until_finished())

        await asyncio.sleep(0.1)  # Let tasks start running

        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                "snapshot",
                "--stats",
                "--url",
                docket.url,
                "--docket",
                docket.name,
            ],
        )
        assert result.exit_code == 0, result.output

        # Should show stats table even with no queued tasks
        assert "Task Count Statistics by Function" in result.output
        assert "Oldest Queued" in result.output
        assert "Latest Queued" in result.output
        assert "sleep" in result.output

        worker_running.cancel()
        await worker_running


def _build_mock_snapshot(now: datetime) -> DocketSnapshot:
    async def dummy_task(*_: object, **__: object) -> None:
        return None

    running_execution = Execution(
        dummy_task,
        args=(),
        kwargs={},
        when=now - timedelta(seconds=30),
        key="running-task",
        attempt=1,
    )
    running = RunningExecution(
        execution=running_execution,
        worker="worker-1",
        started=now - timedelta(seconds=5),
    )

    queued_execution = Execution(
        dummy_task,
        args=(),
        kwargs={},
        when=now + timedelta(seconds=10),
        key="future-task",
        attempt=1,
    )

    return DocketSnapshot(
        taken=now,
        total_tasks=2,
        future=[queued_execution],
        running=[running],
        workers=[
            WorkerInfo(
                name="worker-1",
                last_seen=now - timedelta(seconds=2),
                tasks={"dummy_task"},
            )
        ],
    )


class _RecordingConsole:
    def __init__(self) -> None:
        self.objects: list[object] = []

    def print(self, obj: object = "") -> None:
        self.objects.append(obj)


def test_snapshot_cli_renders_stats_without_backend(monkeypatch: MonkeyPatch) -> None:
    """Ensure snapshot stats table renders even when using a stubbed docket."""

    now = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    snapshot_obj = _build_mock_snapshot(now)
    registered: list[str] = []

    class FakeDocket:
        def __init__(self, name: str, url: str) -> None:
            self.name = name
            self.url = url

        async def __aenter__(self) -> "FakeDocket":
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> bool:
            return False

        def register_collection(self, task_path: str) -> None:
            registered.append(task_path)

        async def snapshot(self) -> DocketSnapshot:
            return snapshot_obj

    monkeypatch.setattr("docket.cli.Docket", FakeDocket)

    recorder = _RecordingConsole()
    monkeypatch.setattr("docket.cli.Console", lambda: recorder)

    snapshot_command(stats=True, docket_="demo", url="memory://")

    assert registered == ["docket.tasks:standard_tasks"]
    tables = [obj for obj in recorder.objects if isinstance(obj, Table)]
    titles = [str(table.title) for table in tables]
    assert any(title.startswith("Docket:") for title in titles)
    assert "Task Count Statistics by Function" in titles


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
