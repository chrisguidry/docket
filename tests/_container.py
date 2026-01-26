"""Docker container management for Redis test infrastructure.

This module handles the lifecycle of Redis containers used in testing,
including single-node Redis, Redis Cluster, and Valkey variants.
"""

import os
import socket
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Generator, Iterable, ParamSpec, TypeVar, cast

import docker.errors
import redis.exceptions
from docker import DockerClient
from docker.models.containers import Container
from redis import ConnectionPool, Redis
from redis.cluster import RedisCluster

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
        else:
            # Use worker_id for deterministic credentials per worker
            worker_suffix = worker_id or "main"
            self.username = f"docket-user-{worker_suffix}"
            self.password = f"pass-{worker_suffix}"
            self.admin_password = f"admin-{worker_suffix}"
            self.docket_prefix = f"acl-test-{worker_suffix}"


@contextmanager
def sync_redis(url: str) -> Generator[Redis, None, None]:
    pool: ConnectionPool | None = None
    redis = Redis.from_url(url)  # type: ignore
    try:
        with redis:
            pool = redis.connection_pool  # type: ignore
            yield redis
    finally:
        if pool:
            pool.disconnect()


@contextmanager
def administrative_redis(port: int, password: str = "") -> Generator[Redis, None, None]:
    if password:
        url = f"redis://:{password}@localhost:{port}/0"
    else:
        url = f"redis://localhost:{port}/0"
    with sync_redis(url) as r:
        yield r


def wait_for_redis(port: int) -> None:
    while True:
        try:
            with administrative_redis(port) as r:
                if r.ping():  # type: ignore
                    return
        except redis.exceptions.ConnectionError:
            time.sleep(0.1)


def setup_acl(port: int, creds: ACLCredentials) -> None:
    """Configure Redis ACL for testing with restricted permissions."""
    with administrative_redis(port) as r:
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


def setup_cluster_acl(ports: tuple[int, int, int], creds: ACLCredentials) -> None:
    """Configure ACL on all cluster nodes."""
    for port in ports:
        with administrative_redis(port) as r:
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


P = ParamSpec("P")
T = TypeVar("T")


def with_image_retry(fn: Callable[P, T], max_retries: int = 3) -> Callable[P, T]:
    """Wrap a function to retry on ImageNotFound with exponential backoff."""

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        last_error: Exception | None = None
        for attempt in range(max_retries):
            try:
                return fn(*args, **kwargs)
            except docker.errors.ImageNotFound as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait = 2**attempt
                    print(
                        f"[with_image_retry] ImageNotFound on attempt {attempt + 1}/{max_retries}, retrying in {wait}s: {e}"
                    )
                    time.sleep(wait)
                else:
                    print(
                        f"[with_image_retry] ImageNotFound on final attempt {attempt + 1}/{max_retries}, giving up: {e}"
                    )
        raise last_error or docker.errors.ImageNotFound("Image pull failed")

    return wrapper


def build_cluster_image(client: DockerClient, base_image: str) -> str:
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


def allocate_cluster_ports() -> tuple[int, int, int]:
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


def wait_for_cluster(port: int) -> None:
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


def cleanup_stale_containers(docker_client: DockerClient) -> None:
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
    for c in containers:
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
