import asyncio
import inspect
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

import pytest

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
    """Worker should finish in-flight tasks before exiting on SIGINT.

    This ensures consistent graceful shutdown behavior across Python 3.10+.
    """
    await _test_signal_graceful_shutdown(docket, key_leak_checker, signal.SIGINT)
