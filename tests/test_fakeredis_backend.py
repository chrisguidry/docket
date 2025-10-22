from unittest.mock import patch

import pytest
from pytest import MonkeyPatch

from docket import Docket

# Skip all tests in this file if fakeredis is not installed
pytest.importorskip("fakeredis")


async def test_docket_backend_fake(monkeypatch: MonkeyPatch):
    """Test using fakeredis backend via DOCKET_BACKEND env var."""
    monkeypatch.setenv("DOCKET_BACKEND", "fake")

    async with Docket(name="test-fake-docket") as docket:

        async def simple_task(value: str) -> str:
            return value

        docket.register(simple_task)

        # Add and run a task
        execution = await docket.add(simple_task)("test-value")
        assert execution.key

        # Verify snapshot works
        snapshot = await docket.snapshot()
        assert snapshot.total_tasks > 0


async def test_docket_backend_fake_missing_dependency(monkeypatch: MonkeyPatch):
    """Test that using fake backend without fakeredis installed raises helpful error."""
    monkeypatch.setenv("DOCKET_BACKEND", "fake")

    # Mock the import to simulate fakeredis not being installed
    with patch.dict("sys.modules", {"fakeredis.aioredis": None}):
        with pytest.raises(
            ImportError, match="fakeredis is required for DOCKET_BACKEND=fake"
        ):
            async with Docket(name="test-fake-docket"):
                pass
