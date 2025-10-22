import os
from unittest.mock import patch

import pytest

from docket import Docket


async def test_docket_backend_fake():
    """Test using fakeredis backend via DOCKET_BACKEND env var."""
    original_value = os.environ.get("DOCKET_BACKEND")
    try:
        os.environ["DOCKET_BACKEND"] = "fake"

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
    finally:
        if original_value is None:
            os.environ.pop("DOCKET_BACKEND", None)
        else:
            os.environ["DOCKET_BACKEND"] = original_value


async def test_docket_backend_fake_missing_dependency():
    """Test that using fake backend without fakeredis installed raises helpful error."""
    original_value = os.environ.get("DOCKET_BACKEND")

    try:
        os.environ["DOCKET_BACKEND"] = "fake"

        # Mock the import to simulate fakeredis not being installed
        with patch.dict("sys.modules", {"fakeredis.aioredis": None}):
            with pytest.raises(
                ImportError, match="fakeredis is required for DOCKET_BACKEND=fake"
            ):
                async with Docket(name="test-fake-docket"):
                    pass
    finally:
        if original_value is None:
            os.environ.pop("DOCKET_BACKEND", None)
        else:
            os.environ["DOCKET_BACKEND"] = original_value
