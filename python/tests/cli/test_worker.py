import asyncio
import inspect
import json
import logging
import os
import signal
import sys
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

import pytest
import watchfiles

from docket.docket import Docket
from docket.tasks import sleep, trace
from docket.worker import Worker
from tests._key_leak_checker import KeyCountChecker
from tests.cli.run import run_cli


@pytest.fixture(autouse=True)
def reset_logging() -> None:
    logging.basicConfig(force=True)


def test_worker_command_exposes_all_the_options_of_worker():
    """Should expose all the options of Worker.run in the CLI command"""
    from docket.cli import worker as worker_cli_command

    cli_signature = inspect.signature(worker_cli_command)
    worker_run_signature = inspect.signature(Worker.run)

    cli_params = {
        name: (param.default, param.annotation)
        for name, param in cli_signature.parameters.items()
    }

    # Remove CLI-only parameters
    cli_params.pop("logging_level")

    worker_params = {
        name: (param.default, param.annotation)
        for name, param in worker_run_signature.parameters.items()
    }

    for name, (default, _) in worker_params.items():
        cli_name = name if name != "docket_name" else "docket_"

        assert cli_name in cli_params, f"Parameter {name} missing from CLI"

        cli_default, _ = cli_params[cli_name]

        if name == "name":
            # Skip hostname check for the 'name' parameter as it's machine-specific
            continue

        assert cli_default == default, (
            f"Default for {name} doesn't match: CLI={cli_default}, Worker.run={default}"
        )


def test_message_batch_option_reaches_the_worker(monkeypatch: pytest.MonkeyPatch):
    """`--message-batch` sets the cap the worker applies to its Redis reads."""
    from docket.cli import worker as worker_cli_command

    passed: dict[str, object] = {}

    class RecordingWorker:
        @staticmethod
        async def run(**kwargs: object) -> None:
            passed.update(kwargs)

    monkeypatch.setattr("docket.cli.Worker", RecordingWorker)

    worker_cli_command(message_batch=17)

    assert passed["message_batch"] == 17


def test_reload_runs_worker_with_cli_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from docket.cli import worker
    from docket.cli._support import LogFormat, LogLevel

    passed: dict[str, object] = {}
    formats: list[LogFormat] = []
    levels: list[LogLevel] = []

    async def record_worker(**kwargs: object) -> None:
        passed.update(kwargs)

    def simulate_reload(
        path: str,
        *,
        target: Callable[..., None],
        args: tuple[object, ...],
        watch_filter: watchfiles.PythonFilter,
    ) -> None:
        assert path == "."
        assert watch_filter(watchfiles.Change.modified, "tasks.py")
        assert not watch_filter(watchfiles.Change.modified, "notes.txt")
        target(*args)

    monkeypatch.setattr(Worker, "run", record_worker)
    monkeypatch.setattr("docket.cli._reload.run_process", simulate_reload)
    monkeypatch.setattr("docket.cli._reload.set_logging_format", formats.append)
    monkeypatch.setattr("docket.cli._reload.set_logging_level", levels.append)
    worker(
        reload=True,
        tasks=["app.tasks:registry"],
        logging_level=LogLevel.WARNING,
        logging_format=LogFormat.PLAIN,
        concurrency=3,
    )

    assert passed["tasks"] == ["app.tasks:registry"]
    assert passed["concurrency"] == 3
    assert passed["until_finished"] is False
    assert formats == [LogFormat.PLAIN]
    assert levels == [LogLevel.WARNING]


async def test_reload_rejects_until_finished() -> None:
    result = await run_cli(
        "worker", "--reload", "--until-finished", env={"TERM": "dumb"}
    )

    assert result.exit_code == 2
    assert "--reload cannot be used with --until-finished" in result.output


async def test_reload_imports_updated_tasks(tmp_path: Path) -> None:
    module = tmp_path / "dev_tasks.py"
    module.write_text("async def before(): pass\ntasks = [before]\n")
    proc = await asyncio.create_subprocess_exec(
        str(Path(sys.executable).with_name("docket")),
        "worker",
        "--reload",
        "--url",
        "memory://",
        "--tasks",
        "dev_tasks:tasks",
        "--logging-format",
        "json",
        "--no-schedule-automatic-tasks",
        cwd=tmp_path,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join([str(tmp_path), os.getenv("PYTHONPATH", "")]),
            "WATCHFILES_FORCE_POLLING": "true",
        },
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert proc.stdout is not None
    stdout = proc.stdout
    output: list[str] = []

    async def wait_for_log(message: str) -> None:
        async with asyncio.timeout(8):
            while True:
                line = await stdout.readline()
                assert line, "".join(output)
                output.append(line.decode())
                if message in output[-1]:
                    record: dict[str, str] = json.loads(line)
                    assert record["message"] == message
                    return

    try:
        await wait_for_log("* before()")
        # Ensure polling sees a new modification time, even during a fast restart.
        timestamp = module.stat().st_mtime + 2
        replacement = tmp_path / "dev_tasks.tmp"
        replacement.write_text("async def edited(): pass\ntasks = [edited]\n")
        os.utime(replacement, (timestamp, timestamp))
        replacement.replace(module)
        await wait_for_log("* edited()")
    finally:
        proc.send_signal(signal.SIGINT)
        remainder, _ = await asyncio.wait_for(proc.communicate(), timeout=8)

    logs = "".join(output) + remainder.decode()
    assert proc.returncode == 0, logs
    assert logs.count("initiating graceful shutdown") == 2, logs


@pytest.mark.skipif(
    os.environ.get("REDIS_VERSION") == "memory",
    reason="Memory backend doesn't share state across processes",
)
@pytest.mark.parametrize("from_env", [False, True])
async def test_worker_loads_dependencies(docket: Docket, from_env: bool) -> None:
    """A fresh CLI process loads dependencies and applies them to queued tasks."""
    from tests.cli.worker_dependencies import task_with_context

    await docket.add(task_with_context, key="cli-dependency")()

    path = "tests.cli.worker_dependencies:dependencies"
    args = [
        "worker",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "--tasks",
        "tests.cli.worker_dependencies:tasks",
        "--logging-level",
        "WARNING",
        "--until-finished",
        "--no-schedule-automatic-tasks",
    ]
    env = {"DOCKET_WORKER_DEPENDENCIES": path}
    if from_env:
        result = await run_cli(*args, env=env)
    else:
        # An explicit flag takes precedence over an environment default.
        env["DOCKET_WORKER_DEPENDENCIES"] = "missing_module:dependencies"
        result = await run_cli(*args, "--dependencies", path, env=env)

    assert result.exit_code == 0, result.output
    assert "ERROR" not in result.output
    assert result.stdout.splitlines() == [
        "enter cli-dependency",
        "task cli-dependency",
        "exit cli-dependency",
    ]


def test_no_schedule_automatic_tasks_flag_reaches_the_worker(
    monkeypatch: pytest.MonkeyPatch,
):
    """`--no-schedule-automatic-tasks` disables automatic task scheduling."""
    from docket.cli import worker as worker_cli_command

    passed: dict[str, object] = {}

    class RecordingWorker:
        @staticmethod
        async def run(**kwargs: object) -> None:
            passed.update(kwargs)

    monkeypatch.setattr("docket.cli.Worker", RecordingWorker)

    worker_cli_command(schedule_automatic_tasks=False)

    assert passed["schedule_automatic_tasks"] is False


async def test_message_batch_below_one_is_rejected():
    """The CLI fails at startup rather than running a worker that reads no
    messages."""
    # Rich styles each segment of the flag separately, so on a color-capable
    # terminal the name arrives split by escape codes. TERM=dumb turns the
    # color system off and leaves the flag as one plain string.
    result = await run_cli("worker", "--message-batch", "0", env={"TERM": "dumb"})

    assert result.exit_code != 0
    assert "--message-batch" in result.output


async def test_no_schedule_automatic_tasks_flag_is_accepted(docket: Docket):
    """`--no-schedule-automatic-tasks` should be a recognized CLI flag."""
    result = await run_cli(
        "worker",
        "--until-finished",
        "--no-schedule-automatic-tasks",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output


async def test_worker_command(
    docket: Docket,
):
    """Should run a worker until there are no more tasks to process"""
    result = await run_cli(
        "worker",
        "--until-finished",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0

    assert "Starting worker" in result.output
    assert "trace" in result.output


@pytest.mark.skipif(
    os.environ.get("REDIS_VERSION") == "memory",
    reason="Memory backend doesn't share state across processes",
)
async def test_worker_command_with_fallback_task(
    docket: Docket,
):
    """Should accept --fallback-task option and use it for unknown tasks"""

    # Schedule a task that won't be registered with the worker
    async def unregistered_task() -> None:
        pass  # pragma: no cover

    docket.register(unregistered_task)
    await docket.add(unregistered_task)()
    docket.tasks.pop("unregistered_task")

    # Use the default fallback from docket.worker
    result = await run_cli(
        "worker",
        "--until-finished",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "--fallback-task",
        "docket.worker:default_fallback_task",
    )
    assert result.exit_code == 0

    assert "Starting worker" in result.output
    assert "Unknown task 'unregistered_task'" in result.output


async def test_rich_logging_format(docket: Docket):
    """Should use rich formatting for logs by default"""
    await docket.add(trace)("hello")

    logging.basicConfig(force=True)

    result = await run_cli(
        "worker",
        "--until-finished",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "--logging-format",
        "rich",
    )

    assert result.exit_code == 0, result.output

    assert "Starting worker" in result.output
    assert "trace" in result.output


async def test_plain_logging_format(docket: Docket):
    """Should use plain formatting for logs when specified"""
    await docket.add(trace)("hello")

    result = await run_cli(
        "worker",
        "--until-finished",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "--logging-format",
        "plain",
    )

    assert result.exit_code == 0, result.output

    assert "Starting worker" in result.output
    assert "trace" in result.output


async def test_json_logging_format(docket: Docket):
    """Should use JSON formatting for logs when specified"""
    await docket.add(trace)("hello")

    start = datetime.now(timezone.utc)

    result = await run_cli(
        "worker",
        "--until-finished",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "--logging-format",
        "json",
    )

    assert result.exit_code == 0, result.output

    # All output lines should be valid JSON
    for line in result.output.strip().split("\n"):
        parsed: dict[str, str] = json.loads(line)

        assert isinstance(parsed, dict)

        assert parsed["name"].startswith("docket.")
        assert parsed["levelname"] in ("INFO", "WARNING", "ERROR", "CRITICAL")
        assert "message" in parsed
        assert "exc_info" in parsed

        timestamp = datetime.strptime(parsed["asctime"], "%Y-%m-%d %H:%M:%S,%f")
        timestamp = timestamp.astimezone()
        assert timestamp >= start
        assert timestamp.tzinfo is not None


async def _test_signal_graceful_shutdown(
    docket: Docket, key_leak_checker: KeyCountChecker, sig: signal.Signals
) -> None:
    """Helper: verify worker gracefully drains in-flight tasks on signal."""
    # The subprocess worker creates progress keys that won't be cleaned up by
    # the in-process key leak checker
    key_leak_checker.add_pattern_exemption(f"{docket.name}:progress:*")
    key_leak_checker.add_pattern_exemption(f"{docket.name}:runs:*")

    docket.register(sleep)
    await docket.add(sleep)(3)

    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "docket",
        "worker",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "--logging-format",
        "plain",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )

    # Wait for the sleep task to start (logs go to stdout with plain format)
    output_so_far = ""
    assert proc.stdout is not None

    while "Sleeping for" not in output_so_far:
        chunk = await asyncio.wait_for(proc.stdout.read(1024), timeout=30)
        output_so_far += chunk.decode()

    # Send signal
    assert proc.pid is not None
    os.kill(proc.pid, sig)

    # Wait for graceful exit
    stdout_rest, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
    output = output_so_far + stdout_rest.decode() + stderr.decode()

    assert proc.returncode == 0, (
        f"Expected exit code 0, got {proc.returncode}\n{output}"
    )
    assert f"Received {sig.name}, initiating graceful shutdown" in output, (
        f"Missing shutdown message for {sig.name}\n{output}"
    )
    assert "Shutdown requested, finishing" in output, (
        f"Missing 'Shutdown requested' message\n{output}"
    )
    assert "↩" in output or "↫" in output, f"Task did not complete\n{output}"


@pytest.mark.skipif(
    os.environ.get("REDIS_VERSION") == "memory",
    reason="Memory backend doesn't share state across processes",
)
async def test_sigterm_gracefully_drains_inflight_tasks(
    docket: Docket, key_leak_checker: KeyCountChecker
) -> None:
    """Worker should finish in-flight tasks before exiting on SIGTERM.

    This is critical for Kubernetes deployments where SIGTERM is sent during
    pod termination. Without graceful handling, in-flight tasks are abruptly
    killed and must be redelivered after redelivery_timeout.
    """
    await _test_signal_graceful_shutdown(docket, key_leak_checker, signal.SIGTERM)


@pytest.mark.skipif(
    os.environ.get("REDIS_VERSION") == "memory",
    reason="Memory backend doesn't share state across processes",
)
async def test_sigint_gracefully_drains_inflight_tasks(
    docket: Docket, key_leak_checker: KeyCountChecker
) -> None:
    """Worker should finish in-flight tasks before exiting on SIGINT."""
    await _test_signal_graceful_shutdown(docket, key_leak_checker, signal.SIGINT)
