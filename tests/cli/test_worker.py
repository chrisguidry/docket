import asyncio
import inspect
import json
import logging
from datetime import datetime, timezone
from functools import partial

import pytest
from typer.testing import CliRunner

from docket.cli import app
from docket.docket import Docket
from docket.tasks import trace
from docket.worker import Worker


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


def test_worker_command(
    runner: CliRunner,
    docket: Docket,
):
    """Should run a worker until there are no more tasks to process"""
    result = runner.invoke(
        app,
        [
            "worker",
            "--until-finished",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        ],
        color=True,
    )
    assert result.exit_code == 0

    assert "Starting worker" in result.output
    assert "trace" in result.output


async def test_rich_logging_format(runner: CliRunner, docket: Docket):
    """Should use rich formatting for logs by default"""
    await docket.add(trace)("hello")

    logging.basicConfig(force=True)

    result = await asyncio.get_running_loop().run_in_executor(
        None,
        partial(
            runner.invoke,
            app,
            [
                "worker",
                "--until-finished",
                "--url",
                docket.url,
                "--docket",
                docket.name,
                "--logging-format",
                "rich",
            ],
            color=True,
        ),
    )

    assert result.exit_code == 0, result.output

    assert "Starting worker" in result.output
    assert "trace" in result.output


async def test_plain_logging_format(runner: CliRunner, docket: Docket):
    """Should use plain formatting for logs when specified"""
    await docket.add(trace)("hello")

    result = await asyncio.get_running_loop().run_in_executor(
        None,
        partial(
            runner.invoke,
            app,
            [
                "worker",
                "--until-finished",
                "--url",
                docket.url,
                "--docket",
                docket.name,
                "--logging-format",
                "plain",
            ],
            color=True,
        ),
    )

    assert result.exit_code == 0, result.output

    assert "Starting worker" in result.output
    assert "trace" in result.output


async def test_json_logging_format(runner: CliRunner, docket: Docket):
    """Should use JSON formatting for logs when specified"""
    await docket.add(trace)("hello")

    start = datetime.now(timezone.utc)

    result = await asyncio.get_running_loop().run_in_executor(
        None,
        runner.invoke,
        app,
        [
            "worker",
            "--until-finished",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            "--logging-format",
            "json",
        ],
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
