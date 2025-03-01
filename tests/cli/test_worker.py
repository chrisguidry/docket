import asyncio
import inspect
import logging

import pytest
from typer.testing import CliRunner

from docket.cli import app
from docket.docket import Docket
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


def test_trace_command(
    runner: CliRunner,
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
):
    """Should add a trace task to the docket"""
    result = runner.invoke(
        app,
        [
            "trace",
            "hiya!",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        ],
    )
    assert result.exit_code == 0
    assert "Added trace task" in result.stdout.strip()

    with caplog.at_level(logging.INFO):
        asyncio.run(worker.run_until_finished())

    assert "hiya!" in caplog.text
