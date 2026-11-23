"""Helpers for tests that subscribe to docket's pub/sub channels."""

from docket._redis import PubSubClient


async def confirm_subscriptions(pubsub: PubSubClient, count: int) -> None:
    """Block until the server has confirmed ``count`` subscription commands.

    ``PubSub.subscribe()`` only sends the SUBSCRIBE command; the server's
    confirmation arrives later as the first pub/sub message.  A publisher on
    another connection can therefore beat the subscription unless the caller
    waits for the confirmation before signaling readiness.

    Subscription confirmations are delivered on a fresh connection before any
    published message can arrive, so consuming the first ``count`` messages
    consumes exactly the confirmations.
    """
    for _ in range(count):
        await pubsub.get_message(ignore_subscribe_messages=False, timeout=None)
