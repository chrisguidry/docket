import asyncio
import decimal
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from typer.testing import CliRunner

from docket.cli import app, interpret_python_value
from docket.docket import Docket


async def test_strike(runner: CliRunner, redis_url: str):
    """Should strike a task"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                "strike",
                "--url",
                docket.url,
                "--docket",
                docket.name,
                "example_task",
                "some_parameter",
                "==",
                "some_value",
            ],
        )

        assert result.exit_code == 0, result.output

        assert "Striking example_task some_parameter == 'some_value'" in result.output

        await asyncio.sleep(0.25)

        assert "example_task" in docket.strike_list.task_strikes


async def test_restore(runner: CliRunner, redis_url: str):
    """Should restore a task"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        await docket.strike("example_task", "some_parameter", "==", "some_value")
        assert "example_task" in docket.strike_list.task_strikes

        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                "restore",
                "--url",
                docket.url,
                "--docket",
                docket.name,
                "example_task",
                "some_parameter",
                "==",
                "some_value",
            ],
        )

        assert result.exit_code == 0, result.output

        assert "Restoring example_task some_parameter == 'some_value'" in result.output

        await asyncio.sleep(0.25)

        assert "example_task" not in docket.strike_list.task_strikes


async def test_task_only_strike(runner: CliRunner, redis_url: str):
    """Should strike a task without specifying parameter conditions"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                "strike",
                "--url",
                docket.url,
                "--docket",
                docket.name,
                "example_task",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Striking example_task" in result.output

        await asyncio.sleep(0.25)

        assert "example_task" in docket.strike_list.task_strikes


async def test_task_only_restore(runner: CliRunner, redis_url: str):
    """Should restore a task without specifying parameter conditions"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        await docket.strike("example_task")

    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                "restore",
                "--url",
                docket.url,
                "--docket",
                docket.name,
                "example_task",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Restoring example_task" in result.output

        await asyncio.sleep(0.25)

        assert "example_task" not in docket.strike_list.task_strikes


async def test_parameter_only_strike(runner: CliRunner, redis_url: str):
    """Should strike tasks with matching parameter conditions regardless of task name"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                "strike",
                "--url",
                docket.url,
                "--docket",
                docket.name,
                "",
                "some_parameter",
                "==",
                "some_value",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Striking (all tasks) some_parameter == 'some_value'" in result.output

        await asyncio.sleep(0.25)

        assert "some_parameter" in docket.strike_list.parameter_strikes
        parameter_strikes = docket.strike_list.parameter_strikes["some_parameter"]
        assert ("==", "some_value") in parameter_strikes


async def test_parameter_only_restore(runner: CliRunner, redis_url: str):
    """Should restore tasks with matching parameter conditions regardless of task
    name"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        await docket.strike("", "some_parameter", "==", "some_value")

        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                "restore",
                "--url",
                docket.url,
                "--docket",
                docket.name,
                "",
                "some_parameter",
                "==",
                "some_value",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Restoring (all tasks) some_parameter == 'some_value'" in result.output

        await asyncio.sleep(0.25)

        assert "some_parameter" not in docket.strike_list.parameter_strikes


@pytest.mark.parametrize("operation", ["strike", "restore"])
async def test_strike_with_no_function_or_parameter(
    runner: CliRunner, redis_url: str, operation: str
):
    """Should fail when neither function nor parameter is provided"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        result = await asyncio.get_running_loop().run_in_executor(
            None,
            runner.invoke,
            app,
            [
                operation,
                "--url",
                docket.url,
                "--docket",
                docket.name,
                "",
            ],
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
async def test_interpret_python_value(input_value: str | None, expected_result: Any):
    """Should interpret Python values correctly from strings"""
    result = interpret_python_value(input_value)
    assert result == expected_result
