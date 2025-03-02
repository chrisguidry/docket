import asyncio
import inspect
import json
import logging
import sys

import pytest
from typer.testing import CliRunner

from docket.cli import app
from docket.docket import Docket
from docket.tasks import fail, trace
from docket.worker import Worker


def test_worker_command(
    runner: CliRunner,
    docket: Docket,
    caplog: pytest.LogCaptureFixture,
):
    """Should run a worker until there are no more tasks to process"""
    with caplog.at_level(logging.INFO):
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
        )
        assert result.exit_code == 0

    assert "Starting worker" in caplog.text
    assert "trace" in caplog.text


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


async def test_rich_logging_format(
    docket: Docket,
):
    """Should log in rich format"""
    await docket.add(trace)("hiya!")
    await docket.add(fail)("womp womp")

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "docket",
        "worker",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "--logging-format",
        "rich",
        "--until-finished",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await process.wait()

    assert process.returncode == 0

    assert process.stdout
    output = await process.stdout.read()

    assert "INFO" in output.decode()
    assert "hiya!" in output.decode()


async def test_plain_logging_format(
    docket: Docket,
):
    """Should log in plain format"""
    await docket.add(trace)("hiya!")
    await docket.add(fail)("womp womp")

    process = await asyncio.create_subprocess_exec(
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
        "--until-finished",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await process.wait()

    assert process.returncode == 0

    assert process.stdout
    output = await process.stdout.read()

    assert "INFO" in output.decode()
    assert "hiya!" in output.decode()


async def test_json_logging_format(
    docket: Docket,
):
    """Should log in JSON format"""
    await docket.add(trace)("hiya!")
    await docket.add(fail)("womp womp")

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "docket",
        "worker",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "--logging-format",
        "json",
        "--until-finished",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    await process.wait()

    assert process.returncode == 0

    assert process.stdout
    output = await process.stdout.read()

    for line in output.decode().splitlines():
        log = json.loads(line)
        assert "levelname" in log
        assert "asctime" in log
        assert "message" in log
        assert "exc_info" in log
