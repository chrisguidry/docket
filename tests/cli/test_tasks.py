import asyncio
import logging

import pytest
from typer.testing import CliRunner

from docket.cli import app
from docket.docket import Docket
from docket.worker import Worker


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
            "tasks",
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
    assert "ERROR" not in caplog.text


def test_fail_command(
    runner: CliRunner,
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
):
    """Should add a trace task to the docket"""
    result = runner.invoke(
        app,
        ["tasks", "fail", "hiya!", "--url", docket.url, "--docket", docket.name],
    )
    assert result.exit_code == 0
    assert "Added fail task" in result.stdout.strip()

    with caplog.at_level(logging.INFO):
        asyncio.run(worker.run_until_finished())

    assert "hiya!" in caplog.text
    assert "ERROR" in caplog.text


def test_sleep_command(
    runner: CliRunner,
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
):
    """Should add a trace task to the docket"""
    result = runner.invoke(
        app,
        [
            "tasks",
            "sleep",
            "0.1",
            "--url",
            docket.url,
            "--docket",
            docket.name,
        ],
    )
    assert result.exit_code == 0
    assert "Added sleep task" in result.stdout.strip()

    with caplog.at_level(logging.INFO):
        asyncio.run(worker.run_until_finished())

    assert "Sleeping for 0.1 seconds" in caplog.text
    assert "ERROR" not in caplog.text
