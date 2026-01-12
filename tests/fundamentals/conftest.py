"""Shared fixtures for fundamentals tests."""

import inspect
from unittest.mock import AsyncMock

import pytest


@pytest.fixture
def the_task() -> AsyncMock:
    task = AsyncMock()
    task.__name__ = "the_task"
    task.__signature__ = inspect.signature(lambda *args, **kwargs: None)
    return task
