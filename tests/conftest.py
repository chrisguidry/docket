import fcntl
import logging
import os
import socket
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import AsyncGenerator, Callable, Generator, Iterable, cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import redis.exceptions
from docker import DockerClient
from docker.models.containers import Container
from redis import ConnectionPool, Redis

from docket import Docket, Worker
from tests._key_leak_checker import KeyCountChecker

REDIS_VERSION = os.environ.get("REDIS_VERSION", "7.4")
ACL_ENABLED = REDIS_VERSION.endswith("-acl")
BASE_REDIS_VERSION = (
    REDIS_VERSION.removesuffix("-acl") if ACL_ENABLED else REDIS_VERSION
)


class ACLCredentials:
    """ACL credentials generated deterministically from testrun_uid.

    Using testrun_uid ensures all pytest-xdist workers use the same credentials,
    since only one worker sets up the ACL in Redis.
    """

    def __init__(self, testrun_uid: str) -> None:
        if not ACL_ENABLED:  # pragma: no cover
            self.username = ""
            self.password = ""
            self.admin_password = ""
            self.docket_prefix = "test-docket"
        else:  # pragma: no cover
            # Use testrun_uid to generate deterministic credentials across xdist workers
            self.username = f"docket-user-{testrun_uid[:8]}"
            self.password = f"pass-{testrun_uid}"
            self.admin_password = f"admin-{testrun_uid}"
            self.docket_prefix = f"acl-test-{testrun_uid[:8]}"


@pytest.fixture(scope="session")
def acl_credentials(testrun_uid: str) -> ACLCredentials:
    """Session-scoped ACL credentials for consistent test isolation."""
    return ACLCredentials(testrun_uid)


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
        url = f"redis://:{password}@localhost:{port}/15"
    else:
        url = f"redis://localhost:{port}/15"
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


def _setup_acl(
    port: int, creds: ACLCredentials, num_workers: int = 8
) -> None:  # pragma: no cover
    """Configure Redis ACL for testing with restricted permissions."""
    with _administrative_redis(port) as r:
        # Create restricted ACL user for Docket tests
        # Channel patterns needed:
        # - &{prefix}-db{N}:* for SUBSCRIBE/PUBLISH (state, progress channels)
        # - &{prefix}-db{N}:cancel:* for PSUBSCRIBE (cancellation listener)
        # PSUBSCRIBE requires literal pattern matches in ACLs
        channel_patterns: list[str] = []
        for i in range(num_workers):
            # For regular SUBSCRIBE/PUBLISH on state/progress channels
            # Use db{i}* to allow suffixes like -db0-1, -db0-2 for tests creating multiple dockets
            channel_patterns.append(f"{creds.docket_prefix}-db{i}*:*")
            # For PSUBSCRIBE on cancellation pattern - add patterns for multiple counter values
            # because make_docket_name returns -db{i}-{counter}
            channel_patterns.append(f"{creds.docket_prefix}-db{i}:cancel:*")
            for j in range(1, 20):  # Support up to 20 dockets per test
                channel_patterns.append(f"{creds.docket_prefix}-db{i}-{j}:cancel:*")

        r.acl_setuser(  # type: ignore[reportUnknownMemberType]
            creds.username,
            enabled=True,
            passwords=[f"+{creds.password}"],
            keys=[
                f"{creds.docket_prefix}*:*",  # docket-scoped keys
                "my-application:*",  # user-managed keys outside docket namespace
            ],
            channels=channel_patterns,
            commands=["+@all"],
        )

        # Set password on default user to prevent unauthenticated connections
        r.acl_setuser(  # type: ignore[reportUnknownMemberType]
            "default",
            enabled=True,
            passwords=[f"+{creds.admin_password}"],
        )


@pytest.fixture(scope="session")
def redis_server(
    testrun_uid: str, worker_id: str, acl_credentials: ACLCredentials
) -> Generator[Container | None, None, None]:
    if BASE_REDIS_VERSION == "memory":  # pragma: no cover
        yield None
        return

    client = DockerClient.from_env()

    container: Container | None = None
    lock_file_name = f"/tmp/docket-unit-tests-{testrun_uid}-startup"

    with open(lock_file_name, "w+") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)

        now = datetime.now(timezone.utc)
        stale_threshold = timedelta(minutes=15)

        containers: Iterable[Container] = cast(
            Iterable[Container],
            client.containers.list(  # type: ignore
                all=True,
                filters={"label": "source=docket-unit-tests"},
            ),
        )
        for c in containers:
            if c.labels.get("testrun_uid") == testrun_uid:  # type: ignore
                container = c
            else:  # pragma: no cover
                # Clean up stale containers from previous test runs
                try:
                    created_str = c.attrs.get("Created", "")
                    if created_str:
                        created_str = created_str.split(".")[0] + "+00:00"
                        created = datetime.fromisoformat(created_str)
                        if now - created > stale_threshold:
                            c.remove(force=True)
                except (ValueError, TypeError):
                    pass

        if not container:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", 0))
                redis_port = s.getsockname()[1]

            image = f"redis:{BASE_REDIS_VERSION}"
            if BASE_REDIS_VERSION.startswith("valkey-"):  # pragma: no cover
                image = f"valkey/valkey:{BASE_REDIS_VERSION.replace('valkey-', '')}"

            container = client.containers.run(
                image,
                detach=True,
                ports={"6379/tcp": redis_port},
                labels={
                    "source": "docket-unit-tests",
                    "testrun_uid": testrun_uid,
                },
                auto_remove=True,
            )

            _wait_for_redis(redis_port)

            if ACL_ENABLED:  # pragma: no cover
                _setup_acl(redis_port, acl_credentials)
        else:
            port_bindings = container.attrs["HostConfig"]["PortBindings"]["6379/tcp"]
            redis_port = int(port_bindings[0]["HostPort"])

        admin_password = acl_credentials.admin_password if ACL_ENABLED else ""
        with _administrative_redis(redis_port, admin_password) as r:
            r.sadd(f"docket-unit-tests:{testrun_uid}", worker_id)

    try:
        yield container
    finally:
        admin_password = acl_credentials.admin_password if ACL_ENABLED else ""
        with _administrative_redis(redis_port, admin_password) as r:
            with r.pipeline() as pipe:  # type: ignore
                pipe.srem(f"docket-unit-tests:{testrun_uid}", worker_id)
                pipe.scard(f"docket-unit-tests:{testrun_uid}")
                _, count = pipe.execute()  # type: ignore

        if count == 0:
            container.stop()
            os.remove(lock_file_name)


@pytest.fixture
def redis_port(redis_server: Container | None) -> int:
    if redis_server is None:  # pragma: no cover
        return 0
    port_bindings = redis_server.attrs["HostConfig"]["PortBindings"]["6379/tcp"]
    return int(port_bindings[0]["HostPort"])


@pytest.fixture(scope="session")
def redis_db(worker_id: str) -> int:
    if not worker_id or "gw" not in worker_id:  # pragma: no cover
        return 0
    return int(worker_id.replace("gw", ""))


@pytest.fixture
def redis_url(redis_port: int, redis_db: int, acl_credentials: ACLCredentials) -> str:
    if BASE_REDIS_VERSION == "memory":  # pragma: no cover
        return "memory://"

    if ACL_ENABLED:  # pragma: no cover
        url = (
            f"redis://{acl_credentials.username}:{acl_credentials.password}"
            f"@localhost:{redis_port}/{redis_db}"
        )
    else:
        url = f"redis://localhost:{redis_port}/{redis_db}"
    with _sync_redis(url) as r:
        r.flushdb()  # type: ignore
    return url


@pytest.fixture
async def docket(
    redis_url: str, redis_db: int, acl_credentials: ACLCredentials
) -> AsyncGenerator[Docket, None]:
    # For ACL tests, the docket name must match the ACL pattern exactly since
    # PSUBSCRIBE requires literal pattern matches. We include redis_db to provide
    # isolation between parallel test workers.
    if ACL_ENABLED:  # pragma: no cover
        name = f"{acl_credentials.docket_prefix}-db{redis_db}"
    else:
        name = f"{acl_credentials.docket_prefix}-{uuid4()}"
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
def make_docket_name(
    acl_credentials: ACLCredentials, redis_db: int
) -> Callable[[], str]:
    """Factory fixture that generates ACL-compatible docket names.

    Use this in tests that create their own Docket instances to ensure
    the names match the ACL key pattern when running with ACL enabled.
    """
    counter = 0

    def _make_name() -> str:
        nonlocal counter
        counter += 1
        if ACL_ENABLED:  # pragma: no cover
            return f"{acl_credentials.docket_prefix}-db{redis_db}-{counter}"
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
    task.__signature__ = inspect.signature(lambda *args, **kwargs: None)
    task.return_value = None
    return task


@pytest.fixture
def another_task() -> AsyncMock:
    import inspect

    task = AsyncMock()
    task.__name__ = "another_task"
    task.__signature__ = inspect.signature(lambda *args, **kwargs: None)
    return task


@pytest.fixture(autouse=True)
async def key_leak_checker(
    redis_url: str, docket: Docket
) -> AsyncGenerator[KeyCountChecker, None]:
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
