from unittest.mock import patch

import pytest
from pytest import MonkeyPatch

from docket import Docket, Worker

# Skip all tests in this file if fakeredis is not installed
pytest.importorskip("fakeredis")


async def test_docket_backend_fake(monkeypatch: MonkeyPatch):
    """Test using fakeredis backend via DOCKET_BACKEND env var."""
    monkeypatch.setenv("DOCKET_BACKEND", "fake")

    async with Docket(name="test-fake-docket") as docket:
        result_value = None

        async def simple_task(value: str) -> str:
            nonlocal result_value
            result_value = value
            return value

        docket.register(simple_task)

        # Add and run a task
        execution = await docket.add(simple_task)("test-value")
        assert execution.key

        # Run the task with a worker to exercise the fakeredis polling path
        async with Worker(docket, concurrency=1) as worker:
            await worker.run_until_finished()

        # Verify the task actually ran
        assert result_value == "test-value"

        # Verify snapshot works
        snapshot = await docket.snapshot()
        assert snapshot.total_tasks == 0  # All tasks should be done


async def test_docket_backend_fake_missing_dependency(monkeypatch: MonkeyPatch):
    """Test that using fake backend without fakeredis installed raises helpful error."""
    monkeypatch.setenv("DOCKET_BACKEND", "fake")

    # Mock the import to simulate fakeredis not being installed
    with patch.dict("sys.modules", {"fakeredis.aioredis": None}):
        with pytest.raises(
            ImportError, match="fakeredis is required for DOCKET_BACKEND=fake"
        ):
            async with Docket(name="test-fake-docket"):
                pass  # pragma: no cover
