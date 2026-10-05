from __future__ import annotations

import asyncio
import inspect
import logging
import os
import socket
import sys
from datetime import datetime, timedelta, timezone
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Callable, Generator
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from docket import Docket, Worker
from docket._redis import RedisConnection
from docket.strikelist import StrikeList
from tests._container import (
    ACL_ENABLED,
    ACLCredentials,
    BASE_VERSION,
    CLUSTER_ENABLED,
    PROVIDER_ENABLED,
    sync_redis,
)
from tests._key_leak_checker import KeyCountChecker
from tests._rotating_provider import RotatingProvider

# Skip condition for tests that need Redis-specific features unavailable in
# the in-memory backend (key enumeration via keys/scan_iter, TTL queries, etc.)
skip_memory = pytest.mark.skipif(
    BASE_VERSION == "memory",
    reason="requires real Redis (keys/scan_iter/ttl not available in memory backend)",
)

# Skip condition for tests whose mechanics don't translate to a clustered
# topology (e.g. server-wide admin commands that need explicit target_nodes,
# or assumptions about a single shared script cache).  The broader suite
# still exercises clustered behaviour end-to-end.
skip_cluster = pytest.mark.skipif(
    CLUSTER_ENABLED,
    reason="requires a non-clustered Redis (test bypasses cluster routing)",
)


async def wait_until(
    predicate: Callable[[], bool | Awaitable[bool]],
    *,
    timeout: float = 5.0,
    description: str = "condition",
) -> None:
    """Poll ``predicate()`` until it returns truthy, or fail with a timeout.

    Accepts either a sync or async predicate.  Async predicates are useful
    for "wait until the docket snapshot shows the expected running count"
    style checks that need to round-trip to Redis on each poll.

    Replaces fixed-duration ``asyncio.sleep`` waits for "let some
    out-of-band background work catch up before I assert on it" -- a
    monitor task draining a Redis stream, a heartbeat firing, a worker
    picking up the next message, etc.  Short-circuits the moment the
    condition holds (no wasted wall-clock on fast runners) and fails
    with a descriptive message on slow ones (no silent flakes).
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{description} never became truthy within {timeout}s")


async def wait_for_event(
    messages: list[dict[str, Any]],
    predicate: Callable[[dict[str, Any]], bool],
    *,
    timeout: float = 5.0,
    description: str = "matching event",
) -> dict[str, Any]:
    """Poll ``messages`` until one satisfies ``predicate``, or raise.

    The pubsub-collector pattern in the test suite appends each received
    event to a list as it arrives.  Tests that need to assert on those
    events shouldn't depend on a fixed-duration ``asyncio.sleep`` to
    "let the subscriber drain" -- that race-tunes the test to whoever's
    runner is fastest.  Use this helper instead: it short-circuits as
    soon as the expected event appears (no wasted wall-clock on fast
    runners) and fails with the full ``messages`` snapshot on slow
    runners (no silent flakes).

    The 10 ms yield is bounded by ``timeout`` and serves only to give
    the collector task a chance to run; the same shape as
    ``await_retry_parked`` and other polling helpers in the suite.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        for msg in messages:
            if predicate(msg):
                return msg
        await asyncio.sleep(0.01)
    raise AssertionError(
        f"no {description} arrived within {timeout}s; saw: {messages!r}"
    )


if sys.platform != "win32" or TYPE_CHECKING:
    from docker import DockerClient
    from docker.models.containers import Container

    from tests._container import (
        build_cluster_image,
        cleanup_stale_containers,
        run_cluster_container,
        setup_acl,
        setup_cluster_acl,
        wait_for_cluster,
        wait_for_redis,
        with_image_retry,
    )


@pytest.fixture(scope="session")
def acl_credentials(worker_id: str) -> ACLCredentials:
    """Session-scoped ACL credentials for consistent test isolation."""
    return ACLCredentials(worker_id)


@pytest.fixture(scope="session", autouse=True)
def credentials_from_a_provider(
    acl_credentials: ACLCredentials, redis_port: int
) -> Generator[None, None, None]:
    """On the provider legs, every Docket, StrikeList, and RedisConnection that
    a test builds for the test Redis without a credential provider gets this
    one, as an application would pass it.  redis_url carries no credentials
    there, so a connection that doesn't use the provider is refused.  URLs
    with credentials, or for other hosts, keep whatever the test gave them."""
    if not PROVIDER_ENABLED:
        yield
        return

    provider = RotatingProvider(acl_credentials.username, acl_credentials.password)

    def with_provider(init: Callable[..., None]) -> Callable[..., None]:
        signature = inspect.signature(init)

        @wraps(init)
        def __init__(self: object, *args: Any, **kwargs: Any) -> None:
            # Docket hands RedisConnection its provider positionally
            bound = signature.bind(self, *args, **kwargs)
            url = bound.arguments.get("url") or ""
            for_the_test_redis = f"://localhost:{redis_port}" in url
            if (
                for_the_test_redis
                and bound.arguments.get("credential_provider") is None
            ):
                bound.arguments["credential_provider"] = provider
            init(*bound.args, **bound.kwargs)

        return __init__

    with pytest.MonkeyPatch.context() as patch:
        for cls in (Docket, StrikeList, RedisConnection):
            patch.setattr(cls, "__init__", with_provider(cls.__init__))
        yield


@pytest.fixture(autouse=True)
def log_level(caplog: pytest.LogCaptureFixture) -> Generator[None, None, None]:
    with caplog.at_level(logging.DEBUG):
        yield


@pytest.fixture
def now() -> Callable[[], datetime]:
    return partial(datetime.now, timezone.utc)


@pytest.fixture(scope="session")
def redis_server(
    worker_id: str,
    acl_credentials: ACLCredentials,
) -> Generator[Container | None, None, None]:
    """Each xdist worker gets its own Redis container.

    This eliminates cross-worker coordination complexity and allows using
    FLUSHALL between tests since each worker owns its Redis instance.
    """
    if BASE_VERSION == "memory":
        yield None
        return

    docker_client = DockerClient.from_env()

    # Clean up stale containers from previous runs
    cleanup_stale_containers(docker_client)

    # Unique label per worker
    container_label = f"docket-test-{worker_id or 'main'}-{os.getpid()}"

    # Determine base image
    if BASE_VERSION.startswith("valkey-"):
        base_image = f"valkey/valkey:{BASE_VERSION.replace('valkey-', '')}"
    else:
        base_image = f"redis:{BASE_VERSION}"

    container: Container
    cluster_ports: tuple[int, int, int] | None = None

    if CLUSTER_ENABLED:
        cluster_image = build_cluster_image(docker_client, base_image)
        container, cluster_ports = run_cluster_container(
            docker_client,
            cluster_image,
            labels={
                "source": "docket-unit-tests",
                "container_label": container_label,
            },
        )

        wait_for_cluster(cluster_ports[0])

        if ACL_ENABLED:
            setup_cluster_acl(cluster_ports, acl_credentials)
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            redis_port = s.getsockname()[1]

        container = with_image_retry(docker_client.containers.run)(
            base_image,
            detach=True,
            ports={"6379/tcp": redis_port},
            labels={
                "source": "docket-unit-tests",
                "container_label": container_label,
            },
            auto_remove=True,
        )

        wait_for_redis(redis_port)

        if ACL_ENABLED:
            setup_acl(redis_port, acl_credentials)

    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def redis_port(redis_server: Container | None) -> int:
    if redis_server is None:
        return 0
    if CLUSTER_ENABLED:
        env_list = redis_server.attrs["Config"]["Env"]
        for env in env_list:
            if env.startswith("CLUSTER_PORT_0="):
                return int(env.split("=")[1])
        raise RuntimeError("CLUSTER_PORT_0 not found in container environment")
    port_bindings = redis_server.attrs["HostConfig"]["PortBindings"]["6379/tcp"]
    return int(port_bindings[0]["HostPort"])


@pytest.fixture
def redis_url(redis_port: int, acl_credentials: ACLCredentials) -> str:
    if BASE_VERSION == "memory":
        return "memory://"

    # On the provider legs the credentials come from credentials_from_a_provider
    userinfo = (
        f"{acl_credentials.username}:{acl_credentials.password}@" if ACL_ENABLED else ""
    )
    url_userinfo = "" if PROVIDER_ENABLED else userinfo

    if CLUSTER_ENABLED:
        return f"redis+cluster://{url_userinfo}localhost:{redis_port}"

    # Each worker owns its Redis, so FLUSHALL is safe
    with sync_redis(f"redis://{userinfo}localhost:{redis_port}/0") as r:
        r.flushall()  # type: ignore
    return f"redis://{url_userinfo}localhost:{redis_port}/0"


@pytest.fixture
def credential_less_url(redis_url: str, redis_port: int) -> str:
    """The test Redis without credentials in the URL (redis_url flushes it)."""
    return f"redis://localhost:{redis_port}/0"


@pytest.fixture(autouse=True)
async def _fresh_memory_server() -> AsyncGenerator[None, None]:
    """Close BurnerRedis instances between tests so Tokio background tasks
    don't hold stale event-loop refs across pytest-asyncio loop teardowns.

    After yield, closes every cached BurnerRedis (draining in-flight futures
    while the event loop is still alive), then clears the cache so the next
    test gets a fresh instance.
    """
    from docket._redis_memory import clear_memory_servers

    await clear_memory_servers()
    yield
    await clear_memory_servers()


@pytest.fixture
async def docket(
    redis_url: str, make_docket_name: Callable[[], str]
) -> AsyncGenerator[Docket, None]:
    name = make_docket_name()
    async with Docket(name=name, url=redis_url) as docket:
        yield docket


@pytest.fixture
async def zero_ttl_docket(
    redis_url: str, make_docket_name: Callable[[], str]
) -> AsyncGenerator[Docket, None]:
    """Docket with execution_ttl=0 for tests that verify immediate expiration."""
    async with Docket(
        name=make_docket_name(),
        url=redis_url,
        execution_ttl=timedelta(0),
    ) as docket:
        yield docket


@pytest.fixture
def make_docket_name(acl_credentials: ACLCredentials) -> Callable[[], str]:
    """Factory fixture that generates ACL-compatible docket names.

    For ACL mode, uses predictable counter-based names that match the
    enumerated ACL channel patterns. For non-ACL mode, uses UUIDs.
    """
    counter = 0

    def _make_name() -> str:
        nonlocal counter
        counter += 1
        if ACL_ENABLED:
            # Predictable names for ACL pattern matching
            return f"{acl_credentials.docket_prefix}-{counter}"
        return f"{acl_credentials.docket_prefix}-{uuid4()}"

    return _make_name


@pytest.fixture
async def worker(docket: Docket) -> AsyncGenerator[Worker, None]:
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as worker:
        yield worker


@pytest.fixture
def the_task() -> AsyncMock:
    import inspect

    task = AsyncMock()
    task.__name__ = "the_task"
    task.__signature__ = inspect.signature(lambda *_args, **_kwargs: None)
    task.return_value = None
    return task


@pytest.fixture
def another_task() -> AsyncMock:
    import inspect

    task = AsyncMock()
    task.__name__ = "another_task"
    task.__signature__ = inspect.signature(lambda *_args, **_kwargs: None)
    task.return_value = None
    return task


@pytest.fixture(autouse=True)
async def key_leak_checker(docket: Docket) -> AsyncGenerator[KeyCountChecker, None]:
    """Automatically verify no keys without TTL leak in any test.

    This autouse fixture runs for every test and ensures that no Redis keys
    without TTL are created during test execution, preventing memory leaks in
    long-running Docket deployments.

    Tests can add exemptions for specific keys:
    - key_leak_checker.add_exemption(f"{docket.name}:special-key")
    """
    if BASE_VERSION == "memory":
        yield KeyCountChecker(docket)
        return

    checker = KeyCountChecker(docket)

    # Prime infrastructure with a temporary worker that exits immediately
    async with Worker(
        docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5),
    ) as temp_worker:
        await temp_worker.run_until_finished()
        # Clean up heartbeat data to avoid polluting tests that check worker counts
        async with docket.redis() as r:
            await r.zrem(docket.workers_set, temp_worker.name)
            for task_name in docket.tasks:
                await r.zrem(docket.task_workers_set(task_name), temp_worker.name)
            await r.delete(docket.worker_tasks_set(temp_worker.name))
            # Release the sweep lease the temp worker took, so the test's own
            # worker can win it and sweep instead of waiting it out.
            await r.delete(docket.redelivery_sweep_key)

    await checker.capture_baseline()

    yield checker

    # Verify no leaks after test completes
    await checker.verify_remaining_keys_have_ttl()
