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

Docket provides fine-grained concurrency control that allows you to limit the number of concurrent tasks based on specific argument values. This is essential for protecting shared resources, preventing overwhelming external services, and managing database connections.

### Basic Concurrency Limits

Use `ConcurrencyLimit` to restrict concurrent execution based on task arguments:

```python
from docket import ConcurrencyLimit

async def process_customer_data(
    customer_id: int,
    concurrency: ConcurrencyLimit = ConcurrencyLimit("customer_id", max_concurrent=1)
) -> None:
    # Only one task per customer_id can run at a time
    await update_customer_profile(customer_id)
    await recalculate_customer_metrics(customer_id)

# These will run sequentially for the same customer
await docket.add(process_customer_data)(customer_id=1001)
await docket.add(process_customer_data)(customer_id=1001)
await docket.add(process_customer_data)(customer_id=1001)

# But different customers can run concurrently
await docket.add(process_customer_data)(customer_id=2001)  # Runs in parallel
await docket.add(process_customer_data)(customer_id=3001)  # Runs in parallel
```

### Database Connection Pooling

Limit concurrent database operations to prevent overwhelming your database:

```python
async def backup_database_table(
    db_name: str,
    table_name: str,
    concurrency: ConcurrencyLimit = ConcurrencyLimit("db_name", max_concurrent=2)
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
    service_name: str,
    concurrency: ConcurrencyLimit = ConcurrencyLimit("service_name", max_concurrent=5)
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

### File Processing Limits

Control concurrent file operations to manage disk I/O:

```python
async def process_media_file(
    file_path: str,
    operation_type: str,
    concurrency: ConcurrencyLimit = ConcurrencyLimit("operation_type", max_concurrent=3)
) -> None:
    # Limit concurrent operations by type (e.g., 3 video transcodes, 3 image resizes)
    if operation_type == "video_transcode":
        await transcode_video(file_path)
    elif operation_type == "image_resize":
        await resize_image(file_path)
    elif operation_type == "audio_compress":
        await compress_audio(file_path)

# Different operation types can run concurrently, but each type is limited
await docket.add(process_media_file)("/videos/movie1.mp4", "video_transcode")
await docket.add(process_media_file)("/videos/movie2.mp4", "video_transcode")
await docket.add(process_media_file)("/images/photo1.jpg", "image_resize")  # Runs in parallel
```

### Custom Scopes

Use custom scopes to create independent concurrency limits:

```python
async def process_tenant_data(
    tenant_id: str,
    operation: str,
    concurrency: ConcurrencyLimit = ConcurrencyLimit(
        "tenant_id",
        max_concurrent=2,
        scope="tenant_operations"
    )
) -> None:
    # Each tenant can have up to 2 concurrent operations
    await perform_tenant_operation(tenant_id, operation)

async def process_global_data(
    data_type: str,
    concurrency: ConcurrencyLimit = ConcurrencyLimit(
        "data_type",
        max_concurrent=1,
        scope="global_operations"  # Separate from tenant operations
    )
) -> None:
    # Global operations have their own concurrency limits
    await process_global_data_type(data_type)
```

### Multi-Level Concurrency

Combine multiple concurrency controls for complex scenarios:

```python
async def process_user_export(
    user_id: int,
    export_type: str,
    region: str,
    user_limit: ConcurrencyLimit = ConcurrencyLimit("user_id", max_concurrent=1),
    type_limit: ConcurrencyLimit = ConcurrencyLimit("export_type", max_concurrent=3),
    region_limit: ConcurrencyLimit = ConcurrencyLimit("region", max_concurrent=10)
) -> None:
    # This task respects ALL concurrency limits:
    # - Only 1 export per user at a time
    # - Only 3 exports of each type globally
    # - Only 10 exports per region
    await generate_user_export(user_id, export_type, region)
```

**Note**: When using multiple `ConcurrencyLimit` dependencies, all limits must be satisfied before the task can start.

### Monitoring Concurrency

Concurrency limits are enforced using Redis sets, so you can monitor them:

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

### Tips

1. **Choose appropriate argument names**: Use arguments that represent the resource you want to protect (database name, customer ID, API endpoint).

2. **Set reasonable limits**: Base limits on your system's capacity and external service constraints.

3. **Use descriptive scopes**: When you have multiple unrelated concurrency controls, use different scopes to avoid conflicts.

4. **Monitor blocked tasks**: Tasks that can't start due to concurrency limits are automatically rescheduled with small delays.

5. **Consider cascading effects**: Concurrency limits can create queuing effects - monitor your system to ensure tasks don't back up excessively.
