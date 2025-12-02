# Getting Started

## Installation

Docket is [available on PyPI](https://pypi.org/project/pydocket/) under the package name
`pydocket`. It targets Python 3.10 or above.

With [`uv`](https://docs.astral.sh/uv/):

```bash
uv pip install pydocket

# or

uv add pydocket
```

With `pip`:

```bash
pip install pydocket
```

You'll also need a [Redis](http://redis.io/) server with Streams support (Redis 5.0+). Docket is tested with Redis 6, 7, and 8, and also works with [Valkey](https://valkey.io/).

## Your First Docket

Each `Docket` should have a name that will be shared across your system, like the name
of a topic or queue. By default this is `"docket"`. You can run multiple separate
dockets on a single Redis server as long as they have different names.

```python
from datetime import datetime, timedelta, timezone
from docket import Docket

async def send_welcome_email(customer_id: int, name: str) -> None:
    print(f"Welcome, {name}! (customer {customer_id})")

async with Docket(name="emails", url="redis://localhost:6379/0") as docket:
    # Schedule immediate work
    await docket.add(send_welcome_email)(12345, "Alice")

    # Schedule future work
    tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
    await docket.add(send_welcome_email, when=tomorrow)(67890, "Bob")
```

The `name` and `url` together represent a single shared docket of work across all your
system. Different services can schedule work on the same docket as long as they use the same connection details.

## Understanding Task Keys

Every task gets a unique identifier called a `key`. By default, Docket generates random UUIDs for these keys, which works well for most use cases since each task execution is truly independent work.

```python
async def send_notification(user_id: int, message: str) -> None:
    print(f"Sending to user {user_id}: {message}")

async with Docket() as docket:
    # Each of these gets a random key and will execute independently
    await docket.add(send_notification)(123, "Welcome!")
    await docket.add(send_notification)(456, "Your order shipped")
    await docket.add(send_notification)(123, "Thank you for your purchase")
```

Sometimes though, you want to control whether multiple tasks represent the same logical work. For example, if a user clicks "process my order" multiple times, you probably only want to process that order once.

Custom keys make scheduling idempotent. There can only ever be one future task scheduled with a given key:

```python
async def process_order(order_id: int) -> None:
    print(f"Processing order {order_id}")

async with Docket() as docket:
    key = f"process-order-{12345}"
    await docket.add(process_order, key=key)(12345)
    await docket.add(process_order, key=key)(12345)  # Ignored - key already exists
```

This is especially valuable for web APIs where client retries or network issues might cause the same request to arrive multiple times:

```python
@app.post("/orders/{order_id}/process")
async def api_process_order(order_id: int):
    # Even if this endpoint gets called multiple times, only one task is scheduled
    key = f"process-order-{order_id}"
    await docket.add(process_order, key=key)(order_id)
    return {"status": "scheduled"}
```

Custom keys also let you manage scheduled work. You can replace future tasks to change their timing or arguments, or cancel them entirely:

```python
key = f"reminder-{customer_id}"

# Schedule a reminder for next week
next_week = datetime.now(timezone.utc) + timedelta(days=7)
await docket.add(send_reminder, when=next_week, key=key)(
    customer_id, "Your trial expires soon"
)

# Customer upgrades - move reminder to next month instead
next_month = datetime.now(timezone.utc) + timedelta(days=30)
await docket.replace(send_reminder, when=next_month, key=key)(
    customer_id, "Thanks for upgrading!"
)

# Customer cancels - remove reminder entirely
await docket.cancel(key)
```

Note that canceling only works for tasks scheduled in the future. Tasks that are ready for immediate execution cannot be canceled once they've been added to the processing queue.

## Running Tasks: Workers

Tasks don't execute automatically - you need workers to process them. A worker connects to the same docket and continuously pulls tasks from the queue.

```python
from docket import Docket, Worker

async def process_order(order_id: int) -> None:
    print(f"Processing order {order_id}")

async def send_notification(message: str) -> None:
    print(f"Notification: {message}")

async with Docket() as docket:
    # Register tasks so workers know about them
    docket.register(process_order)
    docket.register(send_notification)

    async with Worker(docket) as worker:
        await worker.run_forever()  # Process tasks until interrupted
```

For production deployments, you'll typically run workers via the CLI:

```bash
# In tasks.py
async def process_order(order_id: int) -> None:
    print(f"Processing order {order_id}")

async def send_notification(message: str) -> None:
    print(f"Notification: {message}")

tasks = [process_order, send_notification]
```

```bash
docket worker --tasks tasks:tasks --concurrency 5
```

Workers automatically handle concurrency (processing multiple tasks simultaneously), retries on failure, and graceful shutdown. By default, a worker processes up to 10 tasks concurrently.

## Basic Error Handling

By default, if a task fails (raises an exception), Docket will log the error and mark the task as failed in its OpenTelemetry traces. The task won't be retried and the worker will move on to the next task.

For tasks that might fail due to transient issues, you can configure automatic retries:

```python
from docket import Retry

async def flaky_api_call(
    url: str,
    retry: Retry = Retry(attempts=3, delay=timedelta(seconds=5))
) -> None:
    # This will retry up to 3 times with 5 seconds between each attempt
    response = await http_client.get(url)
    if response.status_code != 200:
        raise Exception(f"API returned {response.status_code}")

    print(f"Success on attempt {retry.attempt}")
```

Tasks use a dependency injection pattern similar to FastAPI. The `Retry` dependency tells Docket how to handle failures for that specific task.

## Worker Configuration

Workers handle task delivery guarantees and fault tolerance. By default, workers process up to 10 tasks simultaneously, but you can adjust this with the `concurrency=` parameter or `--concurrency` CLI option. If a worker crashes, its tasks are redelivered to other workers after `redelivery_timeout` expires - you'll want to set this higher than your longest-running task.

Docket provides at-least-once delivery semantics, meaning tasks may be delivered more than once if workers crash, so design your tasks to be idempotent when possible.

## Task Observability

Docket automatically tracks task execution state and provides comprehensive observability features for monitoring long-running tasks.

### Execution State

Every task transitions through well-defined states (SCHEDULED → QUEUED → RUNNING → COMPLETED/FAILED) that you can query at any time:

```python
execution = await docket.add(process_order)(order_id=12345)

# Check the current state
await execution.sync()
print(f"Task state: {execution.state}")
```

### Progress Reporting

Long-running tasks can report their progress to provide visibility:

```python
from docket import Progress
from docket.execution import ExecutionProgress

async def import_records(
    file_path: str,
    progress: ExecutionProgress = Progress()
) -> None:
    records = await load_records(file_path)
    await progress.set_total(len(records))

    for record in records:
        await process_record(record)
        await progress.increment()
```

Progress updates are published in real-time via Redis pub/sub and can be monitored with the `docket watch` CLI command or programmatically.

### Task Results

Tasks can return values that are automatically persisted and retrievable:

```python
async def calculate_total(order_id: int) -> float:
    items = await fetch_order_items(order_id)
    return sum(item.price for item in items)

# Schedule and retrieve the result
execution = await docket.add(calculate_total)(order_id=12345)
total = await execution.get_result()  # Waits for completion if needed
print(f"Order total: ${total:.2f}")
```

For detailed information on state tracking, progress monitoring, result retrieval, and CLI monitoring tools, see [Task State and Progress Monitoring](advanced-patterns.md#task-state-and-progress-monitoring).

## What's Next?

You now know the core concepts: creating dockets, scheduling work with idempotent keys, running workers, and basic error handling. This gives you what you need to build background task systems for most applications.

Ready for more? Check out:

- **[Dependencies Guide](dependencies.md)** - Access current docket, advanced retry patterns, timeouts, and custom dependencies
- **[Testing with Docket](testing.md)** - Ergonomic testing utilities for unit and integration tests
- **[Advanced Task Patterns](advanced-patterns.md)** - Perpetual tasks, striking/restoring, logging, and task chains
- **[Docket in Production](production.md)** - Redis architecture, monitoring, and deployment strategies
- **[API Reference](api-reference.md)** - Complete documentation of all classes and methods

## A Note on Security

Docket uses `cloudpickle` to serialize task functions and their arguments. This allows passing nearly any Python object as task arguments, but also means deserializing arguments can execute arbitrary code. Only schedule tasks from trusted sources in your system.
