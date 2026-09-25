"""A redis-py CredentialProvider in place of credentials in the URL.

The first tests only build pools and clients, which redis-py does lazily, so
they need no Redis.  The last two authenticate against a real, standalone Redis.
"""

# pyright: reportPrivateUsage=false

from typing import Any, Callable

import pytest
from redis.credentials import CredentialProvider, StreamingCredentialProvider
from redis.exceptions import AuthenticationError

from docket import Docket, Worker
from docket._redis import RedisConnection
from tests._container import ACLCredentials
from tests.conftest import skip_cluster, skip_memory


class StreamingProvider(StreamingCredentialProvider):
    """Records the re-auth callbacks redis-py registers for rotating tokens."""

    def __init__(self) -> None:
        self.callbacks: list[Callable[[Any], Any]] = []

    def get_credentials(self) -> tuple[str, str]:
        raise NotImplementedError  # pragma: no cover

    def on_next(self, callback: Callable[[Any], Any]) -> None:
        self.callbacks.append(callback)

    def on_error(self, callback: Callable[[Exception], Any]) -> None:
        pass

    def is_streaming(self) -> bool:
        return True  # pragma: no cover


class CountingProvider(CredentialProvider):
    def __init__(self, username: str, password: str) -> None:
        self.credentials = (username, password)
        self.calls = 0

    async def get_credentials_async(self) -> tuple[str, str]:
        self.calls += 1
        return self.credentials


async def test_standalone_pools_and_clients_get_the_provider():
    """Both pools carry the provider, and both clients register re-auth with it,
    which redis-py only does when the client itself is given the provider."""
    provider = StreamingProvider()
    async with RedisConnection("redis://localhost:6379/0", provider) as connection:
        assert connection._connection_pool is not None
        assert connection._pubsub_pool is not None
        assert connection._connection_pool.connection_kwargs["credential_provider"] is provider
        assert connection._pubsub_pool.connection_kwargs["credential_provider"] is provider
        assert len(provider.callbacks) == 1

        async with connection.pubsub():
            pass
        assert len(provider.callbacks) == 2


async def test_no_provider_leaves_the_url_credentials_alone():
    async with RedisConnection("redis://user:pass@localhost:6379/0") as connection:
        assert connection._connection_pool is not None
        kwargs = connection._connection_pool.connection_kwargs
        assert "credential_provider" not in kwargs
        assert (kwargs["username"], kwargs["password"]) == ("user", "pass")


async def test_sentinel_pool_gets_the_provider():
    provider = StreamingProvider()
    connection = RedisConnection("redis+sentinel://sentinel-a/mymaster", provider)
    pool = await connection._connection_pool_from_url()
    assert pool.connection_kwargs["credential_provider"] is provider
    await pool.aclose()


@pytest.fixture
def credential_less_url(redis_url: str, redis_port: int) -> str:
    """The test Redis without credentials in the URL (redis_url flushes it)."""
    return f"redis://localhost:{redis_port}/0"  # pragma: no cover


def acl_provider(acl_credentials: ACLCredentials) -> CountingProvider:
    """The test suite's ACL user, or the default user when ACLs are off (the
    default user takes any password then)."""
    return CountingProvider(  # pragma: no cover
        acl_credentials.username or "default", acl_credentials.password or "unused"
    )


@skip_memory
@skip_cluster
async def test_docket_authenticates_through_the_provider(  # pragma: no cover
    credential_less_url: str,
    acl_credentials: ACLCredentials,
    make_docket_name: Callable[[], str],
):
    provider = acl_provider(acl_credentials)

    async def add(a: int, b: int) -> int:
        return a + b

    async with Docket(
        name=make_docket_name(), url=credential_less_url, credential_provider=provider
    ) as docket:
        execution = await docket.add(add)(1, 2)
        async with Worker(docket) as worker:
            await worker.run_until_finished()
        assert await execution.get_result() == 3

    assert provider.calls > 0


@skip_memory
@skip_cluster
async def test_wrong_provider_credentials_are_refused(  # pragma: no cover
    credential_less_url: str,
):
    provider = CountingProvider("no-such-user", "wrong")
    async with RedisConnection(credential_less_url, provider) as connection:
        async with connection.client() as r:
            with pytest.raises(AuthenticationError):
                await r.exists("any-key")
