"""Rotating the credentials that a streaming credential provider supplies.

A provider like redis-entraid's refreshes its token before the old one expires,
and the server drops a connection whose token has expired.  So after a
rotation, every connection that docket holds must authenticate as the new
identity.  These need a real, standalone Redis: they read CLIENT LIST to see
which user each connection is authenticated as.
"""

import gc
from datetime import timedelta
from typing import AsyncGenerator, Callable, NamedTuple
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from docket import Docket, Worker
from docket._redis import RedisConnection
from tests._container import ACL_ENABLED, ACLCredentials
from tests._rotating_provider import RotatingProvider
from tests.conftest import skip_cluster, skip_memory, wait_until

pytestmark = [skip_memory, skip_cluster]


class User(NamedTuple):
    name: str
    password: str


@pytest.fixture
async def admin(
    redis_port: int, acl_credentials: ACLCredentials
) -> AsyncGenerator[Redis, None]:
    password = acl_credentials.admin_password if ACL_ENABLED else None
    async with Redis(host="localhost", port=redis_port, password=password) as r:
        yield r


@pytest.fixture
async def users(admin: Redis) -> AsyncGenerator[tuple[User, User], None]:
    """Two users with the same access: the identity before a token refresh,
    and the identity after it."""
    old = User(f"old-{uuid4()}", "old-secret")
    new = User(f"new-{uuid4()}", "new-secret")
    for user in (old, new):
        await admin.acl_setuser(  # type: ignore[reportUnknownMemberType]
            user.name,
            enabled=True,
            passwords=[f"+{user.password}"],
            keys=["*"],
            channels=["*"],
            commands=["+@all"],
        )
    yield old, new
    await admin.acl_deluser(old.name, new.name)  # type: ignore[reportUnknownMemberType]


async def authenticated_as(
    admin: Redis, users: tuple[User, User], *, subscribed: bool
) -> set[str]:
    """The users that the data connections (or the subscribed connections)
    of these two users are authenticated as.  Redis flags a subscriber "P"."""
    names = {user.name for user in users}
    return {
        client["user"]
        for client in await admin.client_list()  # type: ignore[reportUnknownMemberType]
        if client["user"] in names and ("P" in client["flags"]) == subscribed
    }


async def no_connections(admin: Redis, users: tuple[User, User]) -> bool:
    return await authenticated_as(admin, users, subscribed=False) == set()


async def subscribed_as(admin: Redis, users: tuple[User, User], user: User) -> bool:
    return await authenticated_as(admin, users, subscribed=True) == {user.name}


async def test_a_rotation_reauthenticates_the_data_connections_after_pubsub(
    credential_less_url: str, admin: Redis, users: tuple[User, User]
):
    """Opening pub/sub must not take the token refresh away from the data
    connections: they carry the Lua scripts and the stream reads."""
    old, new = users
    provider = RotatingProvider(*old)
    async with RedisConnection(credential_less_url, provider) as connection:
        async with connection.client() as r:
            await r.exists("rotation")
        async with connection.pubsub():
            pass

        await provider.rotate(*new)

        assert await authenticated_as(admin, users, subscribed=False) == {new.name}


async def test_a_rotation_reauthenticates_every_connection_sharing_the_provider(
    credential_less_url: str, admin: Redis, users: tuple[User, User]
):
    """A Docket and its strike list each open a RedisConnection with the same
    provider, and a token refresh must reach both of them."""
    old, new = users
    provider = RotatingProvider(*old)
    async with (
        RedisConnection(credential_less_url, provider) as first,
        RedisConnection(credential_less_url, provider) as second,
    ):
        async with first.client() as r:
            await r.exists("rotation")
        async with second.client() as r:
            await r.exists("rotation")

        await provider.rotate(*new)

        assert await authenticated_as(admin, users, subscribed=False) == {new.name}


async def test_a_rotation_reauthenticates_a_subscribed_connection(
    credential_less_url: str, admin: Redis, users: tuple[User, User]
):
    """A worker's cancellation listener stays subscribed for the worker's whole
    life, so it needs the new identity while it is subscribed."""
    old, new = users
    provider = RotatingProvider(*old)
    async with RedisConnection(credential_less_url, provider) as connection:
        async with connection.pubsub() as pubsub:
            await pubsub.subscribe("rotation")
            await wait_until(
                lambda: subscribed_as(admin, users, old),
                description="the subscriber to authenticate as the old user",
            )

            await provider.rotate(*new)

            await wait_until(
                lambda: subscribed_as(admin, users, new),
                description="the subscriber to authenticate as the new user",
            )


async def test_a_rotation_after_close_opens_no_connections(
    credential_less_url: str, admin: Redis, users: tuple[User, User]
):
    """A closed connection has no connections to re-authenticate, and a token
    refresh must not open them again."""
    old, new = users
    provider = RotatingProvider(*old)
    async with RedisConnection(credential_less_url, provider) as connection:
        async with connection.client() as r:
            await r.exists("rotation")
    await wait_until(
        lambda: no_connections(admin, users),
        description="the server to drop the closed connection",
    )

    await provider.rotate(*new)
    reopened = await authenticated_as(admin, users, subscribed=False)

    # Collect any connections the rotation reopened now, so that their unclosed
    # sockets fail this test instead of whichever test runs next.
    del provider
    gc.collect()

    assert reopened == set()


async def test_a_worker_keeps_working_when_the_old_user_is_revoked(
    credential_less_url: str,
    admin: Redis,
    users: tuple[User, User],
    make_docket_name: Callable[[], str],
):
    """The server drops connections whose token expired.  Revoking the old
    user does the same thing, and the worker must keep running tasks."""
    old, new = users
    provider = RotatingProvider(*old)

    async def add(a: int, b: int) -> int:
        return a + b

    async with Docket(
        name=make_docket_name(), url=credential_less_url, credential_provider=provider
    ) as docket:
        async with Worker(
            docket,
            minimum_check_interval=timedelta(milliseconds=5),
            scheduling_resolution=timedelta(milliseconds=5),
        ) as worker:
            before = await docket.add(add)(1, 2)
            await worker.run_until_finished()
            assert await before.get_result() == 3

            await provider.rotate(*new)
            await admin.acl_deluser(old.name)  # type: ignore[reportUnknownMemberType]

            after = await docket.add(add)(3, 4)
            await worker.run_until_finished()
            assert await after.get_result() == 7
