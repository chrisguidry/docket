"""Rotating the credentials that a streaming credential provider supplies.

A provider like redis-entraid's refreshes its token before the old one expires,
and the server drops a connection whose token has expired.  So after a
rotation, every connection that docket holds must authenticate as the new
identity.  These need a real Redis, standalone or clustered: they read CLIENT
LIST on each node to see which user each connection is authenticated as.
"""

import asyncio
import gc
from contextlib import (
    AbstractAsyncContextManager,
    AsyncExitStack,
    asynccontextmanager,
    suppress,
)
from datetime import timedelta
from typing import Any, AsyncGenerator, Callable, NamedTuple
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from docket import Docket, Worker
from docket._redis import RedisConnection
from docket._subscription import Subscription
from redis.exceptions import AuthenticationError
from tests._container import ACL_ENABLED, ACLCredentials
from tests._rotating_provider import RotatingProvider
from tests.conftest import skip_cluster, skip_memory, wait_until

pytestmark = [skip_memory]


class User(NamedTuple):
    name: str
    password: str


class Admin:
    """The default user on every node of the test Redis: the one standalone
    server, or each primary of the cluster.  A cluster keeps its ACL users and
    its client list per node, and a PUBLISH on any node of a cluster reaches
    the subscribers on every node."""

    def __init__(self, nodes: list[Redis]) -> None:
        self.nodes = nodes

    async def add_user(self, user: User) -> None:
        for node in self.nodes:
            await node.acl_setuser(  # type: ignore[reportUnknownMemberType]
                user.name,
                enabled=True,
                passwords=[f"+{user.password}"],
                keys=["*"],
                channels=["*"],
                commands=["+@all"],
            )

    async def delete_users(self, *users: User) -> None:
        for node in self.nodes:
            await node.acl_deluser(*(user.name for user in users))  # type: ignore[reportUnknownMemberType]

    async def client_list(self) -> list[dict[str, Any]]:
        return [
            client
            for node in self.nodes
            for client in await node.client_list()  # type: ignore[reportUnknownMemberType]
        ]

    async def publish(self, channel: str, message: str | bytes) -> None:
        await self.nodes[0].publish(channel, message)  # type: ignore[reportUnknownMemberType]


@pytest.fixture
async def admin(
    redis_ports: tuple[int, ...], acl_credentials: ACLCredentials
) -> AsyncGenerator[Admin, None]:
    password = acl_credentials.admin_password if ACL_ENABLED else None
    async with AsyncExitStack() as stack:
        yield Admin(
            [
                await stack.enter_async_context(
                    Redis(host="localhost", port=port, password=password)
                )
                for port in redis_ports
            ]
        )


@pytest.fixture
async def users(admin: Admin) -> AsyncGenerator[tuple[User, User], None]:
    """Two users with the same access: the identity before a token refresh,
    and the identity after it."""
    old = User(f"old-{uuid4()}", "old-secret")
    new = User(f"new-{uuid4()}", "new-secret")
    for user in (old, new):
        await admin.add_user(user)
    yield old, new
    await admin.delete_users(old, new)


async def authenticated_as(
    admin: Admin, users: tuple[User, User], *, subscribed: bool
) -> set[str]:
    """The users that the data connections (or the subscribed connections)
    of these two users are authenticated as.  Redis flags a subscriber "P"."""
    names = {user.name for user in users}
    return {
        client["user"]
        for client in await admin.client_list()
        if client["user"] in names and ("P" in client["flags"]) == subscribed
    }


async def no_connections(admin: Admin, users: tuple[User, User]) -> bool:
    return await authenticated_as(admin, users, subscribed=False) == set()


async def subscribed_as(admin: Admin, users: tuple[User, User], user: User) -> bool:
    return await authenticated_as(admin, users, subscribed=True) == {user.name}


async def test_a_rotation_reauthenticates_the_data_connections_after_subscribing(
    credential_less_url: str, admin: Admin, users: tuple[User, User]
):
    """Subscribing must not take the token refresh away from the data
    connections: they carry the Lua scripts and the stream reads.  redis-py
    registers a callback of its own with the provider for each RESP3
    subscriber unless docket turns that off."""
    old, new = users
    provider = RotatingProvider(*old)
    async with (
        RedisConnection(credential_less_url, provider) as connection,
        connection.subscription(channels=["rotation"]),
    ):
        async with connection.client() as r:
            await r.exists("rotation")

        await provider.rotate(*new)

        assert provider.registrations == 1
        assert await authenticated_as(admin, users, subscribed=False) == {new.name}


async def test_a_rotation_reauthenticates_every_connection_sharing_the_provider(
    credential_less_url: str, admin: Admin, users: tuple[User, User]
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


async def test_commands_running_through_a_rotation_keep_their_own_replies(
    credential_less_url: str, users: tuple[User, User]
):
    """A connection in use during a rotation sends its AUTH when it comes back
    to the pool.  redis-py made it available before reading the AUTH reply, so
    another command could read from the same socket and fail with "readuntil()
    called while another coroutine is already waiting" (redis/redis-py#4433)."""
    old, new = users
    provider = RotatingProvider(*old)
    async with RedisConnection(credential_less_url, provider) as connection:
        async with connection.client() as r:

            async def command(n: int) -> None:
                key = f"rotation-{uuid4()}"
                try:
                    for i in range(30):
                        await r.set(key, f"{n}-{i}")
                        assert await r.get(key) == f"{n}-{i}".encode()
                finally:
                    await r.delete(key)

            async def rotate() -> None:
                for user in (new, old, new, old, new):
                    await asyncio.sleep(0.001)
                    await provider.rotate(*user)

            await asyncio.gather(rotate(), *(command(n) for n in range(16)))


async def subscribers(
    admin: Admin, users: tuple[User, User]
) -> list[tuple[str, str, str]]:
    """The node address, client id, and user of each subscribed connection.
    Each node of a cluster numbers its clients on its own."""
    names = {user.name for user in users}
    return sorted(
        (client["laddr"], client["id"], client["user"])
        for client in await admin.client_list()
        if client["user"] in names and "P" in client["flags"]
    )


@asynccontextmanager
async def reading(subscription: Subscription) -> AsyncGenerator[list[bytes], None]:
    """Read messages the way the worker's cancellation listener does, until
    the context exits."""
    received: list[bytes] = []

    async def read() -> None:
        while True:
            message = await subscription.get_message(timeout=0.1)
            if message is not None:
                received.append(message["data"])

    reader = asyncio.create_task(read())
    try:
        yield received
    finally:
        # Cancelling a reader that already failed does nothing, so awaiting it
        # raises the reader's own error.
        reader.cancel()
        with suppress(asyncio.CancelledError):
            await reader


@asynccontextmanager
async def not_reading(
    subscription: Subscription,
) -> AsyncGenerator[list[bytes], None]:
    """Leave the subscription unread, like an Execution.subscribe() generator
    whose caller has not asked for the next event."""
    yield []


async def test_messages_published_around_a_rotation_each_arrive_once_in_order(
    credential_less_url: str, admin: Admin, users: tuple[User, User]
):
    """The rotation writes AUTH onto the subscribed connection and never
    reconnects it, so a message published before, during, or after it arrives
    once, in the order it was published."""
    old, new = users
    provider = RotatingProvider(*old)
    sent = [f"{i}".encode() for i in range(100)]
    async with (
        RedisConnection(credential_less_url, provider) as connection,
        connection.subscription(channels=["rotation"]) as subscription,
        reading(subscription) as received,
    ):
        for i, message in enumerate(sent):
            if i == 50:
                await provider.rotate(*new)
            await admin.publish("rotation", message)

        await wait_until(
            lambda: len(received) >= len(sent),
            description="every message published around the rotation",
        )
        assert received == sent


@pytest.mark.parametrize("reader", [reading, not_reading], ids=["reading", "idle"])
async def test_a_rotation_reauthenticates_the_subscribed_connection_in_place(
    credential_less_url: str,
    admin: Admin,
    users: tuple[User, User],
    reader: Callable[[Subscription], AbstractAsyncContextManager[list[bytes]]],
):
    """The subscribed connection keeps its client id and authenticates as the
    new user, whether or not anything is reading from it."""
    old, new = users
    provider = RotatingProvider(*old)
    async with (
        RedisConnection(credential_less_url, provider) as connection,
        connection.subscription(channels=["rotation"]) as subscription,
        reader(subscription),
    ):
        [(node, client_id, user)] = await subscribers(admin, users)

        await provider.rotate(*new)

        await wait_until(
            lambda: subscribed_as(admin, users, new),
            description="the subscriber to authenticate as the new user",
        )
        assert user == old.name
        assert await subscribers(admin, users) == [(node, client_id, new.name)]


async def test_a_rejected_pool_auth_writes_nothing_to_subscribers(
    credential_less_url: str, admin: Admin, users: tuple[User, User]
):
    """A pool's AUTH reads its reply, so a token the server rejects on an idle
    data connection raises from the rotation before any subscriber is sent it.
    The subscriber keeps its connection and its old user."""
    old, new = users
    provider = RotatingProvider(*old)
    async with (
        RedisConnection(credential_less_url, provider) as connection,
        connection.subscription(channels=["rotation"]) as subscription,
    ):
        async with connection.client() as r:
            await r.exists("rotation")
        before = await subscribers(admin, users)

        with pytest.raises(AuthenticationError):
            await provider.rotate(new.name, "not-the-password")

        await admin.publish("rotation", "after")
        message = await subscription.get_message(timeout=5)
        assert message is not None
        assert message["data"] == b"after"
        assert await subscribers(admin, users) == before


# A cluster client always holds idle connections to its nodes from discovering
# the slots, and the rotation re-authenticates those before any subscriber, so
# on a cluster a rejected token raises from the rotation, as it does in
# test_a_rejected_pool_auth_writes_nothing_to_subscribers.
@skip_cluster
async def test_a_rejected_subscriber_auth_raises_from_the_next_read(
    credential_less_url: str, admin: Admin, users: tuple[User, User]
):
    """With no idle pool connection to try the token on first, a subscriber is
    the first to send it.  redis-py raises the server's rejection from the
    subscriber's next read, as it does for any command whose AUTH fails, and
    closes the connection."""
    old, new = users
    provider = RotatingProvider(*old)
    async with (
        RedisConnection(credential_less_url, provider) as connection,
        connection.subscription(channels=["rotation"]) as subscription,
    ):
        await provider.rotate(new.name, "not-the-password")

        with pytest.raises(AuthenticationError):
            await subscription.get_message(timeout=5)
        assert await subscribers(admin, users) == []


async def test_a_rotation_before_connecting_reaches_the_first_connection(
    credential_less_url: str, admin: Admin, users: tuple[User, User]
):
    """A token refresh can land before docket opens any connection, and the
    first connection must authenticate with the refreshed token."""
    old, new = users
    provider = RotatingProvider(*old)

    await provider.rotate(*new)

    async with RedisConnection(credential_less_url, provider) as connection:
        async with connection.client() as r:
            await r.exists("rotation")
        assert await authenticated_as(admin, users, subscribed=False) == {new.name}


async def test_a_rotation_after_close_opens_no_connections(
    credential_less_url: str, admin: Admin, users: tuple[User, User]
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
    admin: Admin,
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
            await admin.delete_users(old)

            after = await docket.add(add)(3, 4)
            await worker.run_until_finished()
            assert await after.get_result() == 7


def test_the_provider_streams():
    """It says it streams, as redis-entraid's provider does."""
    assert RotatingProvider("user", "secret").is_streaming()
