# Task Behaviors

Docket tasks can declare behavioral defaults like retries, timeouts, progress reporting, and concurrency limits as default parameter values. Docket's dependency injection resolves them at execution time, so you just write a normal function signature.

## Perpetual Tasks

Perpetual tasks automatically reschedule themselves, making them well-suited for recurring work like health checks, data synchronization, or periodic cleanup operations.

### Basic Perpetual Tasks

```python
from docket import Perpetual

async def health_check_service(
    service_url: str,
    perpetual: Perpetual = Perpetual(every=timedelta(minutes=5))
) -> None:
    try:
        response = await http_client.get(f"{service_url}/health")
        response.raise_for_status()
        print(f"✓ {service_url} is healthy")
    except Exception as e:
        print(f"✗ {service_url} failed health check: {e}")
        await send_alert(f"Service {service_url} is down")

# Schedule the task once, it will run every 5 minutes forever
await docket.add(health_check_service)("https://api.example.com")
```

After each execution, the task automatically schedules itself to run again after the specified interval.

### Automatic Startup

Perpetual tasks can start themselves automatically when a worker sees them, without needing to be explicitly scheduled:

```python
async def background_cleanup(
    perpetual: Perpetual = Perpetual(
        every=timedelta(hours=1),
        automatic=True
    )
) -> None:
    deleted_count = await cleanup_old_records()
    print(f"Cleaned up {deleted_count} old records")

# Just register the task - no need to schedule it
docket.register(background_cleanup)

# When a worker starts, it will automatically begin running this task
# The task key will be the function name: "background_cleanup"
```

### Self-Canceling Tasks

Perpetual tasks can stop themselves when their work is done:

```python
async def monitor_deployment(
    deployment_id: str,
    perpetual: Perpetual = Perpetual(every=timedelta(seconds=30))
) -> None:
    status = await check_deployment_status(deployment_id)

    if status in ["completed", "failed"]:
        await notify_deployment_finished(deployment_id, status)
        perpetual.cancel()  # Stop monitoring this deployment
        return

    print(f"Deployment {deployment_id} status: {status}")
```

### Dynamic Parameters

Perpetual tasks can change their arguments or timing for the next execution:

```python
async def adaptive_rate_limiter(
    api_endpoint: str,
    requests_per_minute: int = 60,
    perpetual: Perpetual = Perpetual(every=timedelta(minutes=1))
) -> None:
    # Check current API load
    current_load = await check_api_load(api_endpoint)

    if current_load > 0.8:  # High load
        new_rate = max(30, requests_per_minute - 10)
        perpetual.every = timedelta(seconds=30)  # Check more frequently
        print(f"High load detected, reducing rate to {new_rate} req/min")
    else:  # Normal load
        new_rate = min(120, requests_per_minute + 5)
        perpetual.every = timedelta(minutes=1)  # Normal check interval
        print(f"Normal load, increasing rate to {new_rate} req/min")

    # Schedule next run with updated parameters
    perpetual.perpetuate(api_endpoint, new_rate)
```

### Error Resilience

Perpetual tasks automatically reschedule themselves regardless of success or failure:

```python
async def resilient_sync(
    source_url: str,
    perpetual: Perpetual = Perpetual(every=timedelta(minutes=15))
) -> None:
    # This will ALWAYS reschedule, whether it succeeds or fails
    await sync_data_from_source(source_url)
    print(f"Successfully synced data from {source_url}")
```

You don't need try/except blocks to ensure rescheduling - Docket handles this automatically. Whether the task completes successfully or raises an exception, the next execution will be scheduled according to the `every` interval.

## Cron Tasks

For tasks that need to run at specific wall-clock times rather than at fixed intervals, use the `Cron` dependency. It extends `Perpetual` with cron expression support, scheduling the next run at the exact matching time after each execution.

### Basic Cron Expressions

```python
from docket import Cron

async def weekly_report(cron: Cron = Cron("0 9 * * 1")) -> None:
    # Runs every Monday at 9:00 AM UTC
    await generate_and_send_report()

async def hourly_sync(cron: Cron = Cron("0 * * * *")) -> None:
    # Runs at the top of every hour
    await sync_external_data()
```

Cron uses standard 5-field syntax: `minute hour day month weekday`.

### Vixie Keywords

For common schedules, use the shorthand keywords:

```python
async def daily_cleanup(cron: Cron = Cron("@daily")) -> None:
    await cleanup_old_records()

async def hourly_check(cron: Cron = Cron("@hourly")) -> None:
    await check_service_health()
```

Supported keywords: `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`.

### Timezone Support

By default, cron expressions are interpreted in UTC. Pass a `tz` argument to use a different timezone — this handles daylight saving time transitions automatically:

```python
from zoneinfo import ZoneInfo

async def morning_standup(
    cron: Cron = Cron("0 9 * * 1-5", tz=ZoneInfo("America/Los_Angeles"))
) -> None:
    # Runs weekdays at 9:00 AM Pacific, adjusting for DST
    await send_standup_reminder()

async def tokyo_report(
    cron: Cron = Cron("30 17 * * *", tz=ZoneInfo("Asia/Tokyo"))
) -> None:
    # Runs at 5:30 PM JST every day
    await generate_daily_report()
```

### Automatic Scheduling

Like `Perpetual`, cron tasks default to `automatic=True`, meaning they start themselves when a worker sees them — no explicit `docket.add()` call needed:

```python
# Just register the task; the worker handles scheduling
docket.register(weekly_report)
docket.register(daily_cleanup)
```

Since cron tasks are automatic by default, they must not require any arguments.

## Retrying Tasks

### Exponential Backoff

For services that might be overloaded, exponential backoff gives them time to recover:

```python
from docket import ExponentialRetry

async def call_external_api(
    url: str,
    retry: ExponentialRetry = ExponentialRetry(
        attempts=5,
        minimum_delay=timedelta(seconds=1),
        maximum_delay=timedelta(minutes=5)
    )
) -> None:
    # Retries with delays: 1s, 2s, 4s, 8s, 16s (but capped at 5 minutes)
    try:
        response = await http_client.get(url)
        response.raise_for_status()
        print(f"API call succeeded on attempt {retry.attempt}")
    except Exception as e:
        print(f"Attempt {retry.attempt} failed: {e}")
        raise
```

### Unlimited Retries

For critical tasks that must eventually succeed, use `attempts=None`:

```python
from docket import Retry

async def critical_data_sync(
    source_url: str,
    retry: Retry = Retry(attempts=None, delay=timedelta(minutes=5))
) -> None:
    # This will retry forever with 5-minute delays until it succeeds
    await sync_critical_data(source_url)
    print(f"Critical sync completed after {retry.attempt} attempts")
```

Both `Retry` and `ExponentialRetry` support unlimited retries this way.

## Task Timeouts

Prevent tasks from running too long with the `Timeout` dependency:

```python
from docket import Timeout

async def data_processing_task(
    large_dataset: dict,
    timeout: Timeout = Timeout(timedelta(minutes=10))
) -> None:
    # This task will be cancelled if it runs longer than 10 minutes
    await process_dataset_phase_one(large_dataset)

    # Extend timeout if we need more time for phase two
    timeout.extend(timedelta(minutes=5))
    await process_dataset_phase_two(large_dataset)
```

The `extend()` method can take a specific duration or default to the original timeout duration:

```python
async def adaptive_timeout_task(
    timeout: Timeout = Timeout(timedelta(minutes=2))
) -> None:
    await quick_check()

    # Extend by the base timeout (another 2 minutes)
    timeout.extend()
    await longer_operation()
```

Timeouts work alongside retries. If a task times out, it can be retried according to its retry policy.

## Reporting Task Progress

The `Progress()` dependency provides access to the current task's progress tracker, allowing tasks to report their progress to external observers:

```python
from docket import Progress
from docket.execution import ExecutionProgress

async def import_records(
    file_path: str,
    progress: ExecutionProgress = Progress()
) -> None:
    records = await load_records(file_path)

    # Set the total number of items to process
    await progress.set_total(len(records))
    await progress.set_message("Starting import")

    for i, record in enumerate(records, 1):
        await import_record(record)

        # Update progress atomically
        await progress.increment()

        # Optionally update status message
        if i % 100 == 0:
            await progress.set_message(f"Imported {i}/{len(records)} records")

    await progress.set_message("Import complete")
```

Progress updates are:

- **Atomic**: `increment()` uses Redis HINCRBY for thread-safe updates
- **Real-time**: Updates published via pub/sub for live monitoring
- **Observable**: Can be monitored with `docket watch` CLI or programmatically
- **Ephemeral**: Progress data is automatically deleted when the task completes

The `ExecutionProgress` object provides these methods:

- `set_total(total: int)`: Set the target/total value for progress tracking
- `increment(amount: int = 1)`: Atomically increment the current progress value
- `set_message(message: str)`: Update the status message
- `sync()`: Refresh local state from Redis

For more details on progress monitoring patterns and real-time observation, see [Task Observability](observability.md).

## Concurrency Control

Docket provides fine-grained concurrency control that limits how many tasks can run at the same time, based on specific argument values. This is useful for protecting shared resources, preventing overwhelming external services, and managing database connections.

### Per-Argument Concurrency

Annotate a parameter with `ConcurrencyLimit` to limit concurrency based on its value. Each distinct value gets its own independent limit:

```python
from typing import Annotated
from docket import ConcurrencyLimit

async def process_customer_data(
    customer_id: Annotated[int, ConcurrencyLimit(1)],
) -> None:
    # Only one task per customer_id can run at a time
    await update_customer_profile(customer_id)
    await recalculate_customer_metrics(customer_id)

# These will run sequentially for the same customer
await docket.add(process_customer_data)(customer_id=1001)
await docket.add(process_customer_data)(customer_id=1001)

# But different customers can run concurrently
await docket.add(process_customer_data)(customer_id=2001)  # Runs in parallel
```

### Per-Task Concurrency

Use a default parameter to limit the total number of concurrent executions of a task, regardless of arguments:

```python
async def expensive_computation(
    input_data: str,
    concurrency: ConcurrencyLimit = ConcurrencyLimit(max_concurrent=3),
) -> None:
    # At most 3 of these tasks can run at once across all arguments
    await run_computation(input_data)
```

### Database Connection Pooling

Limit concurrent database operations to prevent overwhelming your database:

```python
async def backup_database_table(
    db_name: Annotated[str, ConcurrencyLimit(2)],
    table_name: str,
) -> None:
    # Maximum 2 backup operations per database at once
    await create_table_backup(db_name, table_name)
    await verify_backup_integrity(db_name, table_name)

# Schedule many backup tasks - only 2 per database will run concurrently
tables = ["users", "orders", "products", "analytics", "logs"]
for table in tables:
    await docket.add(backup_database_table)("production", table)
    await docket.add(backup_database_table)("staging", table)
```

### API Rate Limiting

Protect external APIs from being overwhelmed:

```python
async def sync_user_with_external_service(
    user_id: int,
    service_name: Annotated[str, ConcurrencyLimit(5)],
) -> None:
    # Limit to 5 concurrent API calls per external service
    api_client = get_api_client(service_name)
    user_data = await fetch_user_data(user_id)
    await api_client.sync_user(user_data)

# These respect per-service limits
await docket.add(sync_user_with_external_service)(123, "salesforce")
await docket.add(sync_user_with_external_service)(456, "salesforce")  # Will queue if needed
await docket.add(sync_user_with_external_service)(789, "hubspot")     # Different service, runs in parallel
```

### Custom Scopes

Use custom scopes to create independent concurrency limits:

```python
async def process_tenant_data(
    tenant_id: Annotated[str, ConcurrencyLimit(2, scope="tenant_operations")],
    operation: str,
) -> None:
    # Each tenant can have up to 2 concurrent operations
    await perform_tenant_operation(tenant_id, operation)
```

### Monitoring Concurrency

Concurrency limits are enforced using Redis sorted sets, so you can monitor them:

```python
async def monitor_concurrency_usage() -> None:
    async with docket.redis() as redis:
        # Check how many tasks are running for a specific limit
        active_count = await redis.scard("docket:concurrency:customer_id:1001")
        print(f"Customer 1001 has {active_count} active tasks")

        # List all active concurrency keys
        keys = await redis.keys("docket:concurrency:*")
        for key in keys:
            count = await redis.scard(key)
            print(f"{key}: {count} active tasks")
```

!!! note "Legacy default-parameter style"
    Prior to 0.18, `ConcurrencyLimit` required passing the argument name as a
    string: `ConcurrencyLimit("customer_id", max_concurrent=1)`. This style
    still works but `Annotated` is preferred — it avoids the string-name
    duplication and is consistent with Debounce, Cooldown, and other
    dependencies.

## Cooldown

Cooldown executes the first submission immediately, then drops duplicates within a window. If another submission arrives before the window expires, it's quietly dropped with an INFO-level log.

### Per-Task Cooldown

```python
from datetime import timedelta
from docket import Cooldown

async def process_webhooks(
    cooldown: Cooldown = Cooldown(timedelta(seconds=30)),
) -> None:
    events = await fetch_pending_webhook_events()
    await process_events(events)

# First call starts immediately and sets a 30-second window
await docket.add(process_webhooks)()

# This one arrives 5 seconds later — quietly dropped
await docket.add(process_webhooks)()
```

### Per-Parameter Cooldown

Annotate a parameter with `Cooldown` to apply independent windows per value:

```python
from typing import Annotated

async def sync_customer(
    customer_id: Annotated[int, Cooldown(timedelta(seconds=30))],
) -> None:
    await refresh_customer_data(customer_id)

# First sync for customer 1001 starts immediately
await docket.add(sync_customer)(customer_id=1001)

# Duplicate for 1001 within 30s — dropped
await docket.add(sync_customer)(customer_id=1001)

# Different customer — runs immediately
await docket.add(sync_customer)(customer_id=2002)
```

## Debounce

Debounce waits for submissions to settle before firing. When rapid-fire events arrive, only one task runs — after a quiet period equal to the settle window. This is the classic "trailing-edge" debounce: keep resetting the timer on each new event, then fire once things calm down.

### Per-Task Debounce

```python
from datetime import timedelta
from docket import Debounce

async def process_webhooks(
    debounce: Debounce = Debounce(timedelta(seconds=5)),
) -> None:
    events = await fetch_pending_webhook_events()
    await process_events(events)

# First submission becomes the "winner" and gets rescheduled
await docket.add(process_webhooks)()

# More events arrive — they reset the settle timer but are dropped
await docket.add(process_webhooks)()
await docket.add(process_webhooks)()

# After 5 seconds of quiet, the winner proceeds
```

### Per-Parameter Debounce

Annotate a parameter with `Debounce` to get independent settle windows per value:

```python
from typing import Annotated

async def sync_customer(
    customer_id: Annotated[int, Debounce(timedelta(seconds=5))],
) -> None:
    await refresh_customer_data(customer_id)

# Each customer_id gets its own independent settle window
await docket.add(sync_customer)(customer_id=1001)
await docket.add(sync_customer)(customer_id=1001)  # resets 1001's timer
await docket.add(sync_customer)(customer_id=2002)  # independent window
```

### Debounce vs. Cooldown

| | Cooldown | Debounce |
|---|---|---|
| **Behavior** | Execute first, drop duplicates | Wait for quiet, then execute |
| **Window anchored to** | First execution | Last submission |
| **Good for** | Deduplicating rapid-fire events | Batching bursts into one action |

### Multiple Cooldowns

You can annotate multiple parameters with `Cooldown` on the same task. Each gets its own independent window scoped to that parameter's value. A task must pass *all* of its cooldown checks to start — if any one blocks, the task is dropped:

```python
from typing import Annotated

async def sync_data(
    customer_id: Annotated[int, Cooldown(timedelta(seconds=30))],
    region: Annotated[str, Cooldown(timedelta(seconds=60))],
) -> None:
    await refresh_data(customer_id, region)

# Runs immediately — both windows are clear
await docket.add(sync_data)(customer_id=1, region="us")

# Blocked — customer_id=1 is still in cooldown
await docket.add(sync_data)(customer_id=1, region="eu")

# Blocked — region="us" is still in cooldown
await docket.add(sync_data)(customer_id=2, region="us")

# Runs — both customer_id=2 and region="eu" are clear
await docket.add(sync_data)(customer_id=2, region="eu")
```

Only one `Debounce` is allowed per task — its reschedule mechanism requires a single settle window.

### Combining with Other Controls

Debounce, cooldown, and concurrency limits can all coexist on the same task:

```python
from typing import Annotated

async def process_order(
    order_id: Annotated[int, ConcurrencyLimit(1)],
    cooldown: Cooldown = Cooldown(timedelta(seconds=60)),
) -> None:
    await finalize_order(order_id)
```

Each admission control is checked independently. A task must satisfy all of them to start.
