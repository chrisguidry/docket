import asyncio
import decimal
from datetime import timedelta
import os
from typing import Any
from uuid import UUID

import pytest

from docket.cli import interpret_python_value
from docket.docket import Docket
from tests.cli.run import run_cli

# Skip CLI tests when using memory backend since CLI rejects memory:// URLs
pytestmark = pytest.mark.skipif(
    os.environ.get("REDIS_VERSION") == "memory",
    reason="CLI commands require a persistent Redis backend",
)


async def test_strike(docket: Docket):
    """Should strike a task"""
    result = await run_cli(
        "strike",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "example_task",
        "some_parameter",
        "==",
        "some_value",
    )

    assert result.exit_code == 0, result.output

    assert "Striking example_task some_parameter == 'some_value'" in result.output

    await asyncio.sleep(0.25)

    assert "example_task" in docket.strike_list.task_strikes


async def test_restore(docket: Docket):
    """Should restore a task"""
    await docket.strike("example_task", "some_parameter", "==", "some_value")
    assert "example_task" in docket.strike_list.task_strikes

    result = await run_cli(
        "restore",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "example_task",
        "some_parameter",
        "==",
        "some_value",
    )

    assert result.exit_code == 0, result.output

    assert "Restoring example_task some_parameter == 'some_value'" in result.output

    await asyncio.sleep(0.25)

    assert "example_task" not in docket.strike_list.task_strikes


async def test_task_only_strike(docket: Docket):
    """Should strike a task without specifying parameter conditions"""
    result = await run_cli(
        "strike",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "example_task",
    )

    assert result.exit_code == 0, result.output
    assert "Striking example_task" in result.output

    await asyncio.sleep(0.25)

    assert "example_task" in docket.strike_list.task_strikes


async def test_task_only_restore(docket: Docket):
    """Should restore a task without specifying parameter conditions"""
    await docket.strike("example_task")

    result = await run_cli(
        "restore",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "example_task",
    )

    assert result.exit_code == 0, result.output
    assert "Restoring example_task" in result.output

    await asyncio.sleep(0.25)

    assert "example_task" not in docket.strike_list.task_strikes


async def test_parameter_only_strike(docket: Docket):
    """Should strike tasks with matching parameter conditions regardless of task name"""
    result = await run_cli(
        "strike",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "",
        "some_parameter",
        "==",
        "some_value",
    )

    assert result.exit_code == 0, result.output
    assert "Striking (all tasks) some_parameter == 'some_value'" in result.output

    await asyncio.sleep(0.25)

    assert "some_parameter" in docket.strike_list.parameter_strikes
    parameter_strikes = docket.strike_list.parameter_strikes["some_parameter"]
    assert ("==", "some_value") in parameter_strikes


async def test_parameter_only_restore(docket: Docket):
    """Should restore tasks with matching parameter conditions regardless of task
    name"""
    await docket.strike("", "some_parameter", "==", "some_value")

    result = await run_cli(
        "restore",
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "",
        "some_parameter",
        "==",
        "some_value",
    )

    assert result.exit_code == 0, result.output
    assert "Restoring (all tasks) some_parameter == 'some_value'" in result.output

    await asyncio.sleep(0.25)

    assert "some_parameter" not in docket.strike_list.parameter_strikes


@pytest.mark.parametrize("operation", ["strike", "restore"])
async def test_strike_with_no_function_or_parameter(docket: Docket, operation: str):
    """Should fail when neither function nor parameter is provided"""
    result = await run_cli(
        operation,
        "--url",
        docket.url,
        "--docket",
        docket.name,
        "",
    )

    assert result.exit_code != 0, result.output


@pytest.mark.parametrize(
    "input_value,expected_result",
    [
        (None, None),
        ("hello", "hello"),
        ("int:42", 42),
        ("float:3.14", 3.14),
        ("decimal.Decimal:3.14", decimal.Decimal("3.14")),
        ("bool:True", True),
        ("bool:False", False),
        ("datetime.timedelta:10", timedelta(seconds=10)),
        (
            "uuid.UUID:123e4567-e89b-12d3-a456-426614174000",
            UUID("123e4567-e89b-12d3-a456-426614174000"),
        ),
    ],
)
def test_interpret_python_value(input_value: str | None, expected_result: Any):
    """Should interpret Python values correctly from strings"""
    result = interpret_python_value(input_value)
    assert result == expected_result
