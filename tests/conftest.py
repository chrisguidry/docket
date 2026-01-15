import logging
import os
import socket
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from typing import AsyncGenerator, Callable, Generator, Iterable, cast
from unittest.mock import AsyncMock
from uuid import uuid4

import docker.errors
import pytest
import redis.exceptions
from docker import DockerClient
from docker.models.containers import Container
from redis import ConnectionPool, Redis
from redis.cluster import RedisCluster

from docket import Docket, Worker
from tests._key_leak_checker import KeyCountChecker

# Parse REDIS_VERSION with suffix modifiers for easy typing:
# - "7.4" - standalone Redis 7.4
# - "7.4-acl" - standalone Redis with ACL
# - "7.4-cluster" - Redis cluster
# - "7.4-cluster-acl" - Redis cluster with ACL
# - "valkey-8" - standalone Valkey
# - "valkey-8-cluster" - Valkey cluster
# - "memory" - in-memory backend
REDIS_VERSION = os.environ.get("REDIS_VERSION", "8.0")
CLUSTER_ENABLED = "-cluster" in REDIS_VERSION
ACL_ENABLED = "-acl" in REDIS_VERSION
BASE_VERSION = REDIS_VERSION.replace("-cluster", "").replace("-acl", "")


class ACLCredentials:
    """ACL credentials generated deterministically from worker_id."""

    def __init__(self, worker_id: str) -> None:
        if not ACL_ENABLED:
            self.username = ""
            self.password = ""
            self.admin_password = ""
            self.docket_prefix = "test-docket"
        else:  # pragma: no cover
            # Use worker_id for deterministic credentials per worker
            worker_suffix = worker_id or "main"
            self.username = f"docket-user-{worker_suffix}"
            self.password = f"pass-{worker_suffix}"
            self.admin_password = f"admin-{worker_suffix}"
            self.docket_prefix = f"acl-test-{worker_suffix}"


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


@contextmanager
def _sync_redis(url: str) -> Generator[Redis, None, None]:
    pool: ConnectionPool | None = None
    redis = Redis.from_url(url)  # type: ignore
    try:
        with redis:
            pool = redis.connection_pool  # type: ignore
            yield redis
    finally:
        if pool:  # pragma: no branch
            pool.disconnect()


@contextmanager
def _administrative_redis(
    port: int, password: str = ""
) -> Generator[Redis, None, None]:
    if password:  # pragma: no cover
        url = f"redis://:{password}@localhost:{port}/0"
    else:
        url = f"redis://localhost:{port}/0"
    with _sync_redis(url) as r:
        yield r


def _wait_for_redis(port: int) -> None:
    while True:
        try:
            with _administrative_redis(port) as r:
                if r.ping():  # type: ignore  # pragma: no branch
                    return
        except redis.exceptions.ConnectionError:  # pragma: no cover
            time.sleep(0.1)


def _setup_acl(port: int, creds: ACLCredentials) -> None:  # pragma: no cover
    """Configure Redis ACL for testing with restricted permissions."""
    with _administrative_redis(port) as r:
        # PSUBSCRIBE requires literal pattern matches in ACLs.
        # Enumerate patterns for counter-based docket names (1-200).
        channel_patterns: list[str] = []
        for i in range(1, 201):
            channel_patterns.append(f"{creds.docket_prefix}-{i}:cancel:*")
            channel_patterns.append(f"{creds.docket_prefix}-{i}:state:*")
            channel_patterns.append(f"{creds.docket_prefix}-{i}:progress:*")
        r.acl_setuser(  # type: ignore[reportUnknownMemberType]
            creds.username,
            enabled=True,
            passwords=[f"+{creds.password}"],
            keys=[f"{creds.docket_prefix}*:*", "my-application:*"],
            channels=channel_patterns,
            commands=["+@all"],
        )

        r.acl_setuser(  # type: ignore[reportUnknownMemberType]
            "default",
            enabled=True,
            passwords=[f"+{creds.admin_password}"],
        )


def _setup_cluster_acl(
    ports: tuple[int, int, int], creds: ACLCredentials
) -> None:  # pragma: no cover
    """Configure ACL on all cluster nodes."""
    for port in ports:
        with _administrative_redis(port) as r:
            # PSUBSCRIBE requires literal pattern matches in ACLs.
            # Enumerate patterns for counter-based docket names (1-200).
            channel_patterns: list[str] = []
            for i in range(1, 201):
                channel_patterns.append(f"{{{creds.docket_prefix}-{i}}}:cancel:*")
                channel_patterns.append(f"{{{creds.docket_prefix}-{i}}}:state:*")
                channel_patterns.append(f"{{{creds.docket_prefix}-{i}}}:progress:*")
            r.acl_setuser(  # type: ignore[reportUnknownMemberType]
                creds.username,
                enabled=True,
                passwords=[f"+{creds.password}"],
                keys=[f"{{{creds.docket_prefix}*}}:*", "{my-application}:*"],
                channels=channel_patterns,
                commands=["+@all"],
            )

            r.acl_setuser(  # type: ignore[reportUnknownMemberType]
                "default",
                enabled=True,
                passwords=[f"+{creds.admin_password}"],
            )


def _build_cluster_image(
    client: DockerClient, base_image: str
) -> str:  # pragma: no cover
    """Build cluster image from base image, return image tag."""
    tag = f"docket-cluster:{base_image.replace('/', '-').replace(':', '-')}"

    try:
        client.images.get(tag)
        return tag
    except docker.errors.ImageNotFound:
        pass

    cluster_dir = Path(__file__).parent / "cluster"
    client.images.build(
        path=str(cluster_dir),
        tag=tag,
        buildargs={"BASE_IMAGE": base_image},
    )
    return tag


def _allocate_cluster_ports() -> tuple[int, int, int]:  # pragma: no cover
    """Allocate 3 free ports for cluster nodes.

    Ports must be < 55536 because Redis cluster bus ports are data_port + 10000,
    and ports cannot exceed 65535.
    """
    max_port = 55535  # data port + 10000 must be <= 65535
    ports: list[int] = []

    while len(ports) < 3:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
            if port <= max_port:
                ports.append(port)

    return ports[0], ports[1], ports[2]


def _wait_for_cluster(port: int) -> None:  # pragma: no cover
    """Wait for Redis cluster to be healthy and fully accessible."""
    while True:
        try:
            # Try to connect and verify cluster state
            r: RedisCluster = RedisCluster.from_url(  # type: ignore[reportUnknownMemberType]
                f"redis://localhost:{port}"
            )
            info: dict[str, str] = r.cluster_info()  # type: ignore[reportUnknownMemberType]
            if info.get("cluster_state") != "ok":
                r.close()
                time.sleep(0.1)
                continue

            # Verify we can execute a command on the cluster
            r.ping()  # type: ignore[reportUnknownMemberType]
            r.close()
            return
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.ClusterDownError,
            redis.exceptions.RedisClusterException,
            ConnectionResetError,
            OSError,
        ):
            pass
        time.sleep(0.1)


def _cleanup_stale_containers(docker_client: DockerClient) -> None:
    """Remove stale test containers from previous runs."""
    now = datetime.now(timezone.utc)
    stale_threshold = timedelta(minutes=15)

    containers: Iterable[Container] = cast(
        Iterable[Container],
        docker_client.containers.list(  # type: ignore
            all=True,
            filters={"label": "source=docket-unit-tests"},
        ),
    )
    for c in containers:  # pragma: no cover
        try:
            created_str = c.attrs.get("Created", "")
            if created_str:
                created_str = created_str.split(".")[0] + "+00:00"
                created = datetime.fromisoformat(created_str)
                if now - created > stale_threshold:
                    c.remove(force=True)
        except Exception:
            # Ignore errors - container may already be removed or in use
            pass


@pytest.fixture(scope="session")
def redis_server(
    worker_id: str,
    acl_credentials: ACLCredentials,
) -> Generator[Container | None, None, None]:
    """Each xdist worker gets its own Redis container.

    This eliminates cross-worker coordination complexity and allows using
    FLUSHALL between tests since each worker owns its Redis instance.
    """
    if BASE_VERSION == "memory":  # pragma: no cover
        yield None
        return

    docker_client = DockerClient.from_env()

    # Clean up stale containers from previous runs
    _cleanup_stale_containers(docker_client)

    # Unique label per worker
    container_label = f"docket-test-{worker_id or 'main'}-{os.getpid()}"

    # Determine base image
    if BASE_VERSION.startswith("valkey-"):  # pragma: no cover
        base_image = f"valkey/valkey:{BASE_VERSION.replace('valkey-', '')}"
    else:
        base_image = f"redis:{BASE_VERSION}"  # pragma: no cover

    container: Container
    cluster_ports: tuple[int, int, int] | None = None

    if CLUSTER_ENABLED:  # pragma: no cover
        cluster_image = _build_cluster_image(docker_client, base_image)
        cluster_ports = _allocate_cluster_ports()
        port0, port1, port2 = cluster_ports
        bus0, bus1, bus2 = port0 + 10000, port1 + 10000, port2 + 10000

        container = docker_client.containers.run(
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

        _wait_for_cluster(port0)

        if ACL_ENABLED:
            _setup_cluster_acl(cluster_ports, acl_credentials)
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            redis_port = s.getsockname()[1]

        container = docker_client.containers.run(
            base_image,
            detach=True,
            ports={"6379/tcp": redis_port},
            labels={
                "source": "docket-unit-tests",
                "container_label": container_label,
            },
            auto_remove=True,
        )

        _wait_for_redis(redis_port)

        if ACL_ENABLED:  # pragma: no cover
            _setup_acl(redis_port, acl_credentials)

    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def redis_port(redis_server: Container | None) -> int:
    if redis_server is None:  # pragma: no cover
        return 0
    if CLUSTER_ENABLED:  # pragma: no cover
        env_list = redis_server.attrs["Config"]["Env"]
        for env in env_list:
            if env.startswith("CLUSTER_PORT_0="):
                return int(env.split("=")[1])
        raise RuntimeError("CLUSTER_PORT_0 not found in container environment")
    port_bindings = redis_server.attrs["HostConfig"]["PortBindings"]["6379/tcp"]
    return int(port_bindings[0]["HostPort"])


@pytest.fixture
def redis_url(redis_port: int, acl_credentials: ACLCredentials) -> str:
    if BASE_VERSION == "memory":  # pragma: no cover
        return "memory://"

    if CLUSTER_ENABLED:  # pragma: no cover
        if ACL_ENABLED:
            return (
                f"redis+cluster://{acl_credentials.username}:{acl_credentials.password}"
                f"@localhost:{redis_port}"
            )
        return f"redis+cluster://localhost:{redis_port}"

    if ACL_ENABLED:  # pragma: no cover
        url = (
            f"redis://{acl_credentials.username}:{acl_credentials.password}"
            f"@localhost:{redis_port}/0"
        )
    else:
        url = f"redis://localhost:{redis_port}/0"

    # Each worker owns its Redis, so FLUSHALL is safe
    with _sync_redis(url) as r:
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
        if ACL_ENABLED:  # pragma: no cover
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
