"""Tests for Docket key/prefix methods."""

import pytest
import redis.exceptions

from docket.docket import Docket


# Tests for prefix property and key() method


def test_prefix_returns_name():
    """prefix property should return the docket name."""
    docket = Docket(name="my-docket", url="memory://")
    assert docket.prefix == "my-docket"


def test_key_builds_correct_key():
    """key() should build keys with the prefix."""
    docket = Docket(name="my-docket", url="memory://")
    assert docket.key("queue") == "my-docket:queue"
    assert docket.key("stream") == "my-docket:stream"
    assert docket.key("runs:task-123") == "my-docket:runs:task-123"


def test_queue_key_uses_key_method():
    """queue_key should use the key() method."""
    docket = Docket(name="test", url="memory://")
    assert docket.queue_key == "test:queue"


def test_stream_key_uses_key_method():
    """stream_key should use the key() method."""
    docket = Docket(name="test", url="memory://")
    assert docket.stream_key == "test:stream"


def test_workers_set_uses_key_method():
    """workers_set should use the key() method."""
    docket = Docket(name="test", url="memory://")
    assert docket.workers_set == "test:workers"


def test_known_task_key_uses_key_method():
    """known_task_key should use the key() method."""
    docket = Docket(name="test", url="memory://")
    assert docket.known_task_key("task-123") == "test:known:task-123"


def test_parked_task_key_uses_key_method():
    """parked_task_key should use the key() method."""
    docket = Docket(name="test", url="memory://")
    assert docket.parked_task_key("task-123") == "test:task-123"


def test_stream_id_key_uses_key_method():
    """stream_id_key should use the key() method."""
    docket = Docket(name="test", url="memory://")
    assert docket.stream_id_key("task-123") == "test:stream-id:task-123"


def test_runs_key_uses_key_method():
    """runs_key should use the key() method."""
    docket = Docket(name="test", url="memory://")
    assert docket.runs_key("task-123") == "test:runs:task-123"


def test_cancel_channel_uses_key_method():
    """cancel_channel should use the key() method."""
    docket = Docket(name="test", url="memory://")
    assert docket.cancel_channel("task-123") == "test:cancel:task-123"


def test_results_collection_uses_key_method():
    """results_collection should use the key() method."""
    docket = Docket(name="test", url="memory://")
    assert docket.results_collection == "test:results"


def test_worker_tasks_set_uses_key_method():
    """worker_tasks_set should use the key() method."""
    docket = Docket(name="test", url="memory://")
    assert docket.worker_tasks_set("worker-1") == "test:worker-tasks:worker-1"


def test_task_workers_set_uses_key_method():
    """task_workers_set should use the key() method."""
    docket = Docket(name="test", url="memory://")
    assert docket.task_workers_set("my_task") == "test:task-workers:my_task"


def test_worker_group_name_not_prefixed():
    """worker_group_name is not prefixed because consumer groups are stream-scoped.

    Consumer groups are namespaced to their parent stream, so "docket-workers" on
    stream "app1:stream" is completely separate from "docket-workers" on "app2:stream".
    The group name doesn't need a prefix for isolation, and isn't validated against
    ACL key patterns (it's passed as ARGV in Lua scripts, not KEYS).
    """
    docket = Docket(name="test", url="memory://")
    assert docket.worker_group_name == "docket-workers"


# Tests for connection handling


async def test_docket_propagates_connection_errors_on_operation():
    """Connection errors should propagate when operations are attempted."""
    docket = Docket(name="test-docket", url="redis://nonexistent-host:12345/0")

    # __aenter__ succeeds because it doesn't actually connect to Redis
    # (connection is lazy - happens when operations are performed)
    await docket.__aenter__()

    # But actual operations should fail with connection errors
    async def some_task(): ...

    docket.register(some_task)
    with pytest.raises(redis.exceptions.RedisError):
        await docket.add(some_task)()

    await docket.__aexit__(None, None, None)
