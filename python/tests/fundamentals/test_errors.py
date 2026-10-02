"""Tests for error cases."""

import logging

import pytest

from docket import Docket, Worker


async def test_adding_task_by_name_when_not_registered(docket: Docket):
    """docket should raise an error when attempting to add a task by name that isn't registered"""

    with pytest.raises(KeyError, match="unregistered_task"):
        await docket.add("unregistered_task")()


async def test_adding_task_with_unbindable_arguments(
    docket: Docket,
    worker: Worker,
    caplog: pytest.LogCaptureFixture,
):
    """Should not raise an error when a task is scheduled or executed with
    incorrect arguments."""

    async def task_with_specific_args(a: str, b: int, c: bool = False) -> None:
        pass  # pragma: no cover

    await docket.add(task_with_specific_args)("a", 2, d="unexpected")  # type: ignore[arg-type]

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert "got an unexpected keyword argument 'd'" in caplog.text
