import asyncio
import decimal
import subprocess
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

from docket.cli import interpret_python_value
from docket.docket import Docket


async def test_strike(redis_url: str):
    """Should strike a task"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        process = await asyncio.create_subprocess_exec(
            "docket",
            "strike",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            "example_task",
            "some_parameter",
            "==",
            "some_value",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert await process.wait() == 0

        await asyncio.sleep(0.25)

        assert "example_task" in docket.strike_list.task_strikes


async def test_restore(redis_url: str):
    """Should restore a task"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        await docket.strike("example_task", "some_parameter", "==", "some_value")
        assert "example_task" in docket.strike_list.task_strikes

        process = await asyncio.create_subprocess_exec(
            "docket",
            "restore",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            "example_task",
            "some_parameter",
            "==",
            "some_value",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert await process.wait() == 0

        await asyncio.sleep(0.25)

        assert "example_task" not in docket.strike_list.task_strikes


async def test_task_only_strike(redis_url: str):
    """Should strike a task"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        process = await asyncio.create_subprocess_exec(
            "docket",
            "strike",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            "example_task",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert await process.wait() == 0

        await asyncio.sleep(0.25)

        assert "example_task" in docket.strike_list.task_strikes

        process = await asyncio.create_subprocess_exec(
            "docket",
            "restore",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            "example_task",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert await process.wait() == 0

        await asyncio.sleep(0.25)

        assert "example_task" not in docket.strike_list.task_strikes


async def test_parameter_only_strike(redis_url: str):
    """Should strike a task with only a parameter"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        await docket.strike("example_task", "some_parameter", "==", "some_value")
        assert "example_task" in docket.strike_list.task_strikes

        process = await asyncio.create_subprocess_exec(
            "docket",
            "strike",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            "*",
            "some_parameter",
            "==",
            "some_value",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert await process.wait() == 0

        await asyncio.sleep(0.25)

        assert "*" not in docket.strike_list.task_strikes
        assert "some_parameter" in docket.strike_list.parameter_strikes

        process = await asyncio.create_subprocess_exec(
            "docket",
            "restore",
            "--url",
            docket.url,
            "--docket",
            docket.name,
            "*",
            "some_parameter",
            "==",
            "some_value",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert await process.wait() == 0

        await asyncio.sleep(0.25)

        assert "*" not in docket.strike_list.task_strikes
        assert "" not in docket.strike_list.task_strikes
        assert None not in docket.strike_list.task_strikes
        assert "some_parameter" not in docket.strike_list.parameter_strikes


@pytest.mark.parametrize("operation", ["strike", "restore"])
async def test_strike_with_no_function_or_parameter(redis_url: str, operation: str):
    """Should return an error when neither a function nor a parameter are specified"""
    async with Docket(name=f"test-docket-{uuid4()}", url=redis_url) as docket:
        process = await asyncio.create_subprocess_exec(
            "docket",
            operation,
            "--url",
            docket.url,
            "--docket",
            docket.name,
            "",
            "",
            "==",
            "some_value",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert await process.wait() != 0

        _, stderr = await process.communicate()

        assert b"Must provide either a function and/or a parameter" in stderr


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
    """Should interpret Python values correctly based on type hints"""
    result = interpret_python_value(input_value)

    assert isinstance(result, type(expected_result))
    assert result == expected_result
