"""Tests for OpenTelemetry metrics: histograms, gauges, and Prometheus export."""

import asyncio
import http.client
import socket
from datetime import datetime, timedelta, timezone
from unittest import mock
from unittest.mock import AsyncMock, Mock

import pytest
from opentelemetry.metrics import Histogram, UpDownCounter
from opentelemetry.metrics import _Gauge as Gauge

from docket import Docket, Worker
from docket.instrumentation import healthcheck_server, metrics_server


@pytest.fixture
def worker_labels(
    docket: Docket, worker: Worker, the_task: AsyncMock
) -> dict[str, str]:
    """Create labels dictionary for worker-side metrics."""
    return {
        "docket.name": docket.name,
        "docket.worker": worker.name,
        "docket.task": the_task.__name__,
    }


@pytest.fixture
def TASK_DURATION(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASK_DURATION histogram."""
    mock_obj = Mock(spec=Histogram.record)
    monkeypatch.setattr("docket.instrumentation.TASK_DURATION.record", mock_obj)
    return mock_obj


async def test_task_duration_is_measured(
    docket: Docket, worker: Worker, worker_labels: dict[str, str], TASK_DURATION: Mock
):
    """Should record the duration of task execution in the TASK_DURATION histogram."""

    async def the_task():
        await asyncio.sleep(0.1)

    await docket.add(the_task)()
    await worker.run_until_finished()

    # We can't check the exact value since it depends on actual execution time
    TASK_DURATION.assert_called_once_with(mock.ANY, worker_labels)
    duration: float = TASK_DURATION.call_args.args[0]
    assert isinstance(duration, float)
    assert 0.1 <= duration <= 0.2


@pytest.fixture
def TASK_PUNCTUALITY(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASK_PUNCTUALITY histogram."""
    mock_obj = Mock(spec=Histogram.record)
    monkeypatch.setattr("docket.instrumentation.TASK_PUNCTUALITY.record", mock_obj)
    return mock_obj


async def test_task_punctuality_is_measured(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    worker_labels: dict[str, str],
    TASK_PUNCTUALITY: Mock,
):
    """Should record TASK_PUNCTUALITY values for scheduled tasks."""
    when = datetime.now(timezone.utc) + timedelta(seconds=0.1)
    await docket.add(the_task, when=when)()
    await asyncio.sleep(0.4)
    await worker.run_until_finished()

    # We can't check the exact value since it depends on actual timing
    TASK_PUNCTUALITY.assert_called_once_with(mock.ANY, worker_labels)
    punctuality: float = TASK_PUNCTUALITY.call_args.args[0]
    assert isinstance(punctuality, float)
    assert 0.3 <= punctuality <= 0.5


@pytest.fixture
def TASKS_RUNNING(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the TASKS_RUNNING up-down counter."""
    mock_obj = Mock(spec=UpDownCounter.add)
    monkeypatch.setattr("docket.instrumentation.TASKS_RUNNING.add", mock_obj)
    return mock_obj


async def test_task_running_gauge_is_incremented(
    docket: Docket, worker: Worker, worker_labels: dict[str, str], TASKS_RUNNING: Mock
):
    """Should increment and decrement the TASKS_RUNNING gauge appropriately."""
    inside_task = False

    async def the_task():
        nonlocal inside_task
        inside_task = True

        TASKS_RUNNING.assert_called_once_with(1, worker_labels)

    await docket.add(the_task)()

    await worker.run_until_finished()

    assert inside_task is True

    TASKS_RUNNING.assert_has_calls(
        [
            mock.call(1, worker_labels),
            mock.call(-1, worker_labels),
        ]
    )


@pytest.fixture
def metrics_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def test_exports_metrics_as_prometheus_metrics(
    docket: Docket,
    worker: Worker,
    the_task: AsyncMock,
    metrics_port: int,
):
    """Should export metrics as Prometheus metrics, translating dots in labels to
    underscores for Prometheus."""

    with metrics_server(port=metrics_port):
        await docket.add(the_task)()
        await worker.run_until_finished()

        await asyncio.sleep(0.1)

        def read_metrics(port: int) -> tuple[http.client.HTTPResponse, str]:
            conn = http.client.HTTPConnection(f"localhost:{port}")
            conn.request("GET", "/")
            response = conn.getresponse()
            return response, response.read().decode()

        response, body = await asyncio.get_running_loop().run_in_executor(
            None,
            read_metrics,
            metrics_port,
        )

        assert response.status == 200, body

        assert (
            response.headers["Content-Type"]
            == "text/plain; version=0.0.4; charset=utf-8"
        )

        assert "docket_tasks_added" in body
        assert "docket_tasks_completed" in body

        assert f'docket_name="{docket.name}"' in body
        assert 'docket_task="the_task"' in body
        assert f'docket_worker="{worker.name}"' in body


@pytest.fixture
def QUEUE_DEPTH(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the QUEUE_DEPTH counter."""
    mock_obj = Mock(spec=Gauge.set)
    monkeypatch.setattr("docket.instrumentation.QUEUE_DEPTH.set", mock_obj)
    return mock_obj


@pytest.fixture
def SCHEDULE_DEPTH(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock for the SCHEDULE_DEPTH counter."""
    mock_obj = Mock(spec=Gauge.set)
    monkeypatch.setattr("docket.instrumentation.SCHEDULE_DEPTH.set", mock_obj)
    return mock_obj


@pytest.fixture
def docket_labels(docket: Docket) -> dict[str, str]:
    """Create labels dictionary for the Docket client-side metrics."""
    return {"docket.name": docket.name}


async def test_worker_publishes_depth_gauges(
    docket: Docket,
    docket_labels: dict[str, str],
    the_task: AsyncMock,
    QUEUE_DEPTH: Mock,
    SCHEDULE_DEPTH: Mock,
):
    """Should publish depth gauges for due and scheduled tasks."""
    await docket.add(the_task)()
    await docket.add(the_task)()

    future = datetime.now(timezone.utc) + timedelta(seconds=60)
    await docket.add(the_task, when=future)()
    await docket.add(the_task, when=future)()
    await docket.add(the_task, when=future)()

    docket.heartbeat_interval = timedelta(seconds=0.1)
    async with Worker(docket):
        await asyncio.sleep(0.2)  # enough for a heartbeat to be published

    QUEUE_DEPTH.assert_called_with(2, docket_labels)
    SCHEDULE_DEPTH.assert_called_with(3, docket_labels)


@pytest.fixture
def healthcheck_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_healthcheck_server_returns_ok(healthcheck_port: int):
    """Should return 200 and OK body from the liveness endpoint."""
    with healthcheck_server(port=healthcheck_port):
        conn = http.client.HTTPConnection(f"localhost:{healthcheck_port}")
        conn.request("GET", "/")
        response = conn.getresponse()

        assert response.status == 200
        assert response.headers["Content-Type"] == "text/plain"
        assert response.read().decode() == "OK"


def test_metrics_server_raises_import_error_without_sdk(
    monkeypatch: pytest.MonkeyPatch, metrics_port: int
):
    """Should raise ImportError with helpful message when SDK is not installed."""
    import builtins
    from typing import Any

    original_import = builtins.__import__

    def mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "opentelemetry.sdk.metrics":
            raise ImportError("No module named 'opentelemetry.sdk'")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)

    with pytest.raises(ImportError, match="pip install pydocket\\[metrics\\]"):
        with metrics_server(port=metrics_port):
            ...
