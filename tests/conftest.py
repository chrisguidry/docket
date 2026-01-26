import logging
import os
import socket
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import AsyncGenerator, Callable, Generator
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from docker import DockerClient
from docker.models.containers import Container

from docket import Docket, Worker
from tests._container import (
    ACL_ENABLED,
    ACLCredentials,
    BASE_VERSION,
    CLUSTER_ENABLED,
    allocate_cluster_ports,
    build_cluster_image,
    cleanup_stale_containers,
    setup_acl,
    setup_cluster_acl,
    sync_redis,
    wait_for_cluster,
    wait_for_redis,
    with_image_retry,
)
from tests._key_leak_checker import KeyCountChecker


@pytest.fixture(scope="session")
def acl_credentials(worker_id: str) -> ACLCredentials:
    """Session-scoped ACL credentials for consistent test isolation."""
    return ACLCredentials(worker_id)


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
        cluster_ports = allocate_cluster_ports()
        port0, port1, port2 = cluster_ports
        bus0, bus1, bus2 = port0 + 10000, port1 + 10000, port2 + 10000

        container = with_image_retry(docker_client.containers.run)(
            cluster_image,
            detach=True,
            ports={
                f"{port0}/tcp": port0,
                f"{port1}/tcp": port1,
                f"{port2}/tcp": port2,
                f"{bus0}/tcp": bus0,
                f"{bus1}/tcp": bus1,
                f"{bus2}/tcp": bus2,
            },
            environment={
                "CLUSTER_PORT_0": str(port0),
                "CLUSTER_PORT_1": str(port1),
                "CLUSTER_PORT_2": str(port2),
            },
            labels={
                "source": "docket-unit-tests",
                "container_label": container_label,
            },
            auto_remove=True,
        )

        wait_for_cluster(port0)

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

    if CLUSTER_ENABLED:
        if ACL_ENABLED:
            return (
                f"redis+cluster://{acl_credentials.username}:{acl_credentials.password}"
                f"@localhost:{redis_port}"
            )
        return f"redis+cluster://localhost:{redis_port}"

    if ACL_ENABLED:
        url = (
            f"redis://{acl_credentials.username}:{acl_credentials.password}"
            f"@localhost:{redis_port}/0"
        )
    else:
        url = f"redis://localhost:{redis_port}/0"

    # Each worker owns its Redis, so FLUSHALL is safe
    with sync_redis(url) as r:
        r.flushall()  # type: ignore
    return url


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

    await checker.capture_baseline()

    yield checker

    # Verify no leaks after test completes
    await checker.verify_remaining_keys_have_ttl()
