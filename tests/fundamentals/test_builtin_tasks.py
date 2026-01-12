"""Tests for built-in tasks (trace, fail)."""

import logging

import pytest

from docket import Docket, Worker, tasks


async def test_all_dockets_have_a_trace_task(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """All dockets should have a trace task"""

    await docket.add(tasks.trace)("Hello, world!")

    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

        assert "Hello, world!" in caplog.text


async def test_all_dockets_have_a_fail_task(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """All dockets should have a fail task"""

    await docket.add(tasks.fail)("Hello, world!")

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

        assert "Hello, world!" in caplog.text
