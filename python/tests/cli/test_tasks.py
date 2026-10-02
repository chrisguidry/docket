import logging
import os

import pytest

from docket.docket import Docket
from docket.worker import Worker
from tests.cli.run import run_cli

# Skip CLI tests when using memory backend since CLI rejects memory:// URLs
pytestmark = pytest.mark.skipif(
    os.environ.get("REDIS_VERSION") == "memory",
    reason="CLI commands require a persistent Redis backend",
)


async def test_trace_command(
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
):
    """Should add a trace task to the docket"""
    result = await run_cli(
        "tasks",
        "trace",
        "hiya!",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0
    assert "Added trace task" in result.stdout.strip()

    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

    assert "hiya!" in caplog.text
    assert "ERROR" not in caplog.text


async def test_fail_command(
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
):
    """Should add a trace task to the docket"""
    result = await run_cli(
        "tasks",
        "fail",
        "hiya!",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0
    assert "Added fail task" in result.stdout.strip()

    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

    assert "hiya!" in caplog.text
    assert "ERROR" in caplog.text


async def test_sleep_command(
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
):
    """Should add a trace task to the docket"""
    result = await run_cli(
        "tasks",
        "sleep",
        "0.1",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0
    assert "Added sleep task" in result.stdout.strip()

    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

    assert "Sleeping for 0.1 seconds" in caplog.text
    assert "ERROR" not in caplog.text
