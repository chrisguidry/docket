"""Shared Redis Docker container management for chaos tests."""

import socket
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from docker import DockerClient
from docker.models.containers import Container


def get_free_port() -> int:
    """Find an available TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@asynccontextmanager
async def run_redis(version: str) -> AsyncGenerator[tuple[str, Container], None]:
    """Start a Redis Docker container and yield (url, container).

    Args:
        version: Redis Docker image tag (e.g., "7.4.2")

    Yields:
        Tuple of (redis_url, container) where redis_url is like "redis://localhost:PORT/0"
    """
    port = get_free_port()

    client = DockerClient.from_env()
    container: Container = client.containers.run(  # type: ignore[reportUnknownMemberType]
        f"redis:{version}",
        detach=True,
        ports={"6379/tcp": port},
        auto_remove=True,
    )

    # Wait for Redis to be ready
    for line in container.logs(stream=True):  # type: ignore[reportUnknownMemberType]
        if b"Ready to accept connections" in line:
            break

    try:
        yield f"redis://localhost:{port}/0", container
    finally:
        container.stop()  # type: ignore[reportUnknownMemberType]
