from datetime import datetime, timedelta, timezone
import os
from unittest.mock import AsyncMock

import pytest

from docket.docket import Docket
from tests.cli.run import run_cli

# Skip CLI tests when using memory backend since CLI rejects memory:// URLs
pytestmark = pytest.mark.skipif(
    os.environ.get("REDIS_VERSION") == "memory",
    reason="CLI commands require a persistent Redis backend",
)


@pytest.fixture(autouse=True)
async def empty_docket(docket: Docket):
    """Ensure that the docket starts empty"""
    await docket.clear()


async def test_clear_command_empty_docket(docket: Docket):
    """Should clear empty docket and report 0 tasks cleared"""
    result = await run_cli(
        "clear",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output
    assert "Cleared 0 tasks" in result.output


async def test_clear_command_with_immediate_tasks(docket: Docket, the_task: AsyncMock):
    """Should clear immediate tasks and report count"""
    docket.register(the_task)

    await docket.add(the_task)("arg1")
    await docket.add(the_task)("arg2")
    await docket.add(the_task)("arg3")

    result = await run_cli(
        "clear",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output
    assert "Cleared 3 tasks" in result.output

    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 0
    assert len(snapshot.running) == 0


async def test_clear_command_with_scheduled_tasks(docket: Docket, the_task: AsyncMock):
    """Should clear scheduled tasks and report count"""
    docket.register(the_task)

    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add(the_task, when=future)("scheduled1")
    await docket.add(the_task, when=future + timedelta(seconds=1))("scheduled2")

    result = await run_cli(
        "clear",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output
    assert "Cleared 2 tasks" in result.output

    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 0
    assert len(snapshot.running) == 0


async def test_clear_command_with_mixed_tasks(
    docket: Docket, the_task: AsyncMock, another_task: AsyncMock
):
    """Should clear both immediate and scheduled tasks"""
    docket.register(the_task)
    docket.register(another_task)

    future = datetime.now(timezone.utc) + timedelta(seconds=60)

    await docket.add(the_task)("immediate1")
    await docket.add(another_task)("immediate2")
    await docket.add(the_task, when=future)("scheduled1")
    await docket.add(another_task, when=future + timedelta(seconds=1))("scheduled2")

    result = await run_cli(
        "clear",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output
    assert "Cleared 4 tasks" in result.output

    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 0
    assert len(snapshot.running) == 0


async def test_clear_command_with_keyed_tasks(docket: Docket, the_task: AsyncMock):
    """Should clear tasks with keys"""
    docket.register(the_task)

    await docket.add(the_task, key="task1")("arg1")
    await docket.add(the_task, key="task2")("arg2")

    result = await run_cli(
        "clear",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output
    assert "Cleared 2 tasks" in result.output

    snapshot = await docket.snapshot()
    assert len(snapshot.future) == 0


async def test_clear_command_basic_functionality(docket: Docket, the_task: AsyncMock):
    """Should clear tasks via CLI command"""
    docket.register(the_task)

    # Add some tasks to clear
    await docket.add(the_task)("task1")
    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add(the_task, when=future)("scheduled_task")

    result = await run_cli(
        "clear",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output
    assert "Cleared" in result.output

    snapshot_after_clear = await docket.snapshot()
    assert len(snapshot_after_clear.future) == 0


async def test_clear_command_preserves_strikes(docket: Docket, the_task: AsyncMock):
    """Should not affect strikes when clearing"""
    docket.register(the_task)

    await docket.strike("the_task")
    await docket.add(the_task)("arg1")

    result = await run_cli(
        "clear",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output
    assert "Cleared" in result.output

    # Strikes should still be in effect - clear doesn't affect strikes


async def test_clear_command_with_custom_url():
    """Should handle custom Redis URL"""
    result = await run_cli(
        "clear",
        "--url",
        "redis://nonexistent:12345/0",
        "--docket",
        "test-docket",
    )
    assert result.exit_code != 0


async def test_clear_command_with_custom_docket_name(
    docket: Docket, the_task: AsyncMock
):
    """Should handle custom docket name"""
    docket.register(the_task)
    await docket.add(the_task)("test")

    result = await run_cli(
        "clear",
        "--url",
        docket.url,
        "--docket",
        docket.name,
    )
    assert result.exit_code == 0, result.output
    assert "Cleared 1 tasks" in result.output
