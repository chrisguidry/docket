"""Tests for task logging features."""

import logging
from logging import LoggerAdapter
from typing import Annotated

import pytest

from docket import CurrentDocket, Docket, Logged, TaskLogger, Worker


async def test_tasks_can_opt_into_argument_logging(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """Tasks can opt into argument logging for specific arguments"""

    async def the_task(
        a: Annotated[str, Logged],
        b: str,
        c: Annotated[str, Logged()] = "c",
        d: Annotated[str, "nah chief"] = "d",
        docket: Docket = CurrentDocket(),
    ):
        pass

    await docket.add(the_task)("value-a", b="value-b", c="value-c", d="value-d")

    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

        # Filter to only docket logs (exclude fakeredis DEBUG logs which contain raw pickle data)
        docket_logs = "\n".join(
            r.message for r in caplog.records if r.name.startswith("docket")
        )
        assert "the_task('value-a', b=..., c='value-c', d=...)" in docket_logs
        assert "value-b" not in docket_logs
        assert "value-d" not in docket_logs


async def test_tasks_can_opt_into_logging_collection_lengths(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """Tasks can opt into logging the length of collections"""

    async def the_task(
        a: Annotated[list[str], Logged(length_only=True)],
        b: Annotated[dict[str, str], Logged(length_only=True)],
        c: Annotated[tuple[str, ...], Logged(length_only=True)],
        d: Annotated[set[str], Logged(length_only=True)],
        e: Annotated[int, Logged(length_only=True)],
        docket: Docket = CurrentDocket(),
    ):
        pass

    await docket.add(the_task)(
        ["a", "b"], b={"d": "e", "f": "g"}, c=("h", "i"), d={"a", "b", "c"}, e=123
    )

    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

        assert (
            "the_task([len 2], b={len 2}, c=(len 2), d={len 3}, e=123)" in caplog.text
        )


async def test_logging_inside_of_task(
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
):
    """docket should support providing a logger with task context"""
    called = False

    async def the_task(
        a: str, b: str, logger: "LoggerAdapter[logging.Logger]" = TaskLogger()
    ):
        assert a == "a"
        assert b == "c"

        logger.info("Task is running")

        nonlocal called
        called = True

    await docket.add(the_task, key="my-cool-task:123")("a", b="c")

    with caplog.at_level(logging.INFO):
        await worker.run_until_finished()

    assert called
    assert "Task is running" in caplog.text
    assert "docket.task.the_task" in caplog.text
