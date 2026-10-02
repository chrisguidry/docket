"""Tests for Docket key/prefix methods."""

# pyright: reportPrivateUsage=false

import pytest
import redis.exceptions

from docket._redis import RedisConnection
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


# Tests for cluster URL handling


@pytest.mark.parametrize(
    "url,expected",
    [
        ("redis://localhost:6379/0", False),
        ("rediss://localhost:6379/0", False),
        ("memory://", False),
        ("redis+cluster://localhost:6379/0", True),
        ("rediss+cluster://localhost:6379/0", True),
        ("redis+cluster://user:pass@localhost:6379/0", True),
        ("rediss+cluster://user:pass@localhost:6379/0", True),
    ],
)
def test_is_cluster_url(url: str, expected: bool):
    """RedisConnection.is_cluster should correctly identify cluster URLs."""
    connection = RedisConnection(url)
    assert connection.is_cluster == expected


@pytest.mark.parametrize(
    "url,expected",
    [
        ("redis+cluster://localhost:6379/0", "redis://localhost:6379/0"),
        ("rediss+cluster://localhost:6379/0", "rediss://localhost:6379/0"),
        (
            "redis+cluster://user:pass@localhost:6379/0",
            "redis://user:pass@localhost:6379/0",
        ),
        (
            "rediss+cluster://user:pass@localhost:6379/0",
            "rediss://user:pass@localhost:6379/0",
        ),
    ],
)
def test_normalize_cluster_url(url: str, expected: str):
    """RedisConnection._normalized_url should remove +cluster from scheme."""
    connection = RedisConnection(url)
    assert connection._normalized_url() == expected


def test_prefix_returns_hash_tagged_name_for_cluster():
    """prefix property should return hash-tagged name for cluster URLs."""
    docket = Docket(name="my-docket", url="redis+cluster://localhost:6379/0")
    assert docket.prefix == "{my-docket}"


def test_key_builds_hash_tagged_key_for_cluster():
    """key() should build hash-tagged keys for cluster URLs."""
    docket = Docket(name="my-docket", url="redis+cluster://localhost:6379/0")
    assert docket.key("queue") == "{my-docket}:queue"
    assert docket.key("stream") == "{my-docket}:stream"
    assert docket.key("runs:task-123") == "{my-docket}:runs:task-123"


def test_strikelist_prefix_returns_hash_tagged_name_for_cluster():
    """StrikeList prefix should return hash-tagged name for cluster URLs."""
    from docket.strikelist import StrikeList

    strikes = StrikeList(name="my-docket", url="redis+cluster://localhost:6379/0")
    assert strikes.prefix == "{my-docket}"
    assert strikes.strike_key == "{my-docket}:strikes"


def test_strikelist_prefix_without_redis():
    """StrikeList prefix should return name when no Redis URL provided."""
    from docket.strikelist import StrikeList

    strikes = StrikeList(name="local-strikes")
    assert strikes._redis is None
    assert strikes.prefix == "local-strikes"


# Tests for RedisConnection edge cases


async def test_redis_connection_aenter_is_not_reentrant():
    """RedisConnection.__aenter__ raises on re-entry."""
    connection = RedisConnection("memory://")
    await connection.__aenter__()

    # Second enter should raise
    with pytest.raises(AssertionError, match="not reentrant"):
        await connection.__aenter__()

    await connection.__aexit__(None, None, None)


def test_redis_connection_cluster_client_returns_none_when_not_cluster():
    """cluster_client property should return None for non-cluster connections."""
    connection = RedisConnection("memory://")
    assert connection.cluster_client is None


def test_redis_connection_normalized_url_returns_original_for_non_cluster():
    """_normalized_url should return original URL for non-cluster connections."""
    connection = RedisConnection("redis://localhost:6379/0")
    assert connection._normalized_url() == "redis://localhost:6379/0"

    connection2 = RedisConnection("memory://")
    assert connection2._normalized_url() == "memory://"
