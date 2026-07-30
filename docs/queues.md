# Reliable Message Queues

Docket queues provide durable, at-least-once message delivery for systems that
own their own execution and result model. Unlike Docket tasks, queue messages
are opaque bytes: Docket does not import a function, execute the payload, or
store a result.

This is useful when Docket is the delivery layer beneath another runtime:

```python
from datetime import timedelta

from docket import Docket

async with Docket(name="orders") as docket:
    queue = docket.queue("commands")
    await queue.put(
        "scheduled",
        b'{"order_id": "123"}',
        key="order:123:charge",
    )
```

A subscription competes with other subscriptions in the same consumer group.
Only one receives a given delivery:

```python
async with Docket(name="orders") as docket:
    queue = docket.queue("commands")
    async with queue.subscribe(
        {"retry": 0, "scheduled": 1},
        visibility_timeout=timedelta(minutes=5),
    ) as subscription:
        while True:
            message = await subscription.receive()
            try:
                await execute_in_my_runtime(message.data)
            except RetryableError:
                await message.release("retry")
            else:
                await message.acknowledge()
```

Lower numeric topic priorities are returned first when multiple claimed
messages are ready. Each topic is FIFO. `release()` atomically moves a message
to another topic, which supports an immediate retry lane without losing the
original delivery.

## Delivery guarantees

Queue delivery is at least once:

- A claimed message remains in Redis until it is acknowledged.
- The subscription renews visibility while the message is outstanding.
- If the subscriber exits or loses its Redis connection, another subscriber
  can reclaim the message after `visibility_timeout`.
- `acknowledge()` removes the message only after the downstream runtime has
  accepted it.

Choose a visibility timeout longer than normal processing stalls and Redis
failovers. Consumers must still be idempotent because a process can finish its
side effect and fail before acknowledging the message.

Message keys are deduplicated across every topic in a queue while the message
is queued or in flight. For repair loops that may rediscover accepted work,
retain a short acknowledgement tombstone:

```python
queue = docket.queue(
    "commands",
    acknowledgement_ttl=timedelta(hours=1),
)
```

Publishing the same key during that period returns `False`; a new publication
returns `True`.

## Backpressure

Set `max_size` on `put()` to bound the number of queued and in-flight messages
in a topic:

```python
await queue.put("scheduled", payload, max_size=1_000)
```

The publisher waits until capacity is available. The same option on
`release()` prevents an immediate-retry lane from exceeding its bound while
keeping the source delivery claimable until the atomic move succeeds.

## Operations

Queues use Redis Streams consumer groups and require Redis 6.2 or newer.
Subscriptions retry transient Redis errors, recreate expired consumer groups,
renew claims, and reclaim abandoned deliveries. They use the Docket's existing
standalone, cluster, Sentinel, authentication, and connection-pool
configuration.

Use the same Docket name, queue name, and consumer group for replicas that
should share work. A queue supports one logical consumer group: acknowledged
messages are deleted rather than broadcast to independent groups.
