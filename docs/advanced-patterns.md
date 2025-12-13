# Advanced Task Patterns

Docket is made for building complex distributed systems, and the patterns below highlight some of the original use cases for Docket.

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

### Find & Flood Pattern

A common perpetual task pattern is "find & flood" - a single perpetual task that periodically discovers work to do, then creates many smaller tasks to handle the actual work:

```python
from docket import CurrentDocket, Perpetual

async def find_pending_orders(
    docket: Docket = CurrentDocket(),
    perpetual: Perpetual = Perpetual(every=timedelta(minutes=1))
) -> None:
    # Find all orders that need processing
    pending_orders = await database.fetch_pending_orders()

    # Flood the queue with individual processing tasks
    for order in pending_orders:
        await docket.add(process_single_order)(order.id)

    print(f"Queued {len(pending_orders)} orders for processing")

async def process_single_order(order_id: int) -> None:
    # Handle one specific order
    await process_order_payment(order_id)
    await update_inventory(order_id)
    await send_confirmation_email(order_id)
```

This pattern separates discovery (finding work) from execution (doing work), allowing for better load distribution and fault isolation. The perpetual task stays lightweight and fast, while the actual work is distributed across many workers.

## Task Scattering with Agenda

For "find-and-flood" workloads, you often want to distribute a batch of tasks over time rather than scheduling them all immediately. The `Agenda` class collects related tasks and scatters them evenly across a time window.

### Basic Scattering

```python
from datetime import timedelta
from docket import Agenda, Docket

async def process_item(item_id: int) -> None:
    await perform_expensive_operation(item_id)
    await update_database(item_id)

async with Docket() as docket:
    # Build an agenda of tasks
    agenda = Agenda()
    for item_id in range(1, 101):  # 100 items to process
        agenda.add(process_item)(item_id)

    # Scatter them evenly over 50 minutes to avoid overwhelming the system
    executions = await agenda.scatter(docket, over=timedelta(minutes=50))
    print(f"Scheduled {len(executions)} tasks over 50 minutes")
```

Tasks are distributed evenly across the time window. For 100 tasks over 50 minutes, they'll be scheduled approximately 30 seconds apart.

### Jitter for Thundering Herd Prevention

Add random jitter to prevent multiple processes from scheduling identical work at exactly the same times:

```python
# Scatter with ±30 second jitter around each scheduled time
await agenda.scatter(
    docket,
    over=timedelta(minutes=50),
    jitter=timedelta(seconds=30)
)
```

### Future Scatter Windows

Schedule the entire batch to start at a specific time in the future:

```python
from datetime import datetime, timezone

# Start scattering in 2 hours, spread over 30 minutes
start_time = datetime.now(timezone.utc) + timedelta(hours=2)
await agenda.scatter(
    docket,
    start=start_time,
    over=timedelta(minutes=30)
)
```

### Mixed Task Types

Agendas can contain different types of tasks:

```python
async def send_email(user_id: str, template: str) -> None:
    await email_service.send(user_id, template)

async def update_analytics(event_data: dict[str, str]) -> None:
    await analytics_service.track(event_data)

# Create a mixed agenda
agenda = Agenda()
agenda.add(process_item)(item_id=1001)
agenda.add(send_email)("user123", "welcome")
agenda.add(update_analytics)({"event": "signup", "user": "user123"})
agenda.add(process_item)(item_id=1002)

# All tasks will be scattered in the order they were added
await agenda.scatter(docket, over=timedelta(minutes=10))
```

### Single Task Positioning

When scattering a single task, it's positioned at the midpoint of the time window:

```python
agenda = Agenda()
agenda.add(process_item)(item_id=42)

# This task will be scheduled 5 minutes from now (middle of 10-minute window)
await agenda.scatter(docket, over=timedelta(minutes=10))
```

### Agenda Reusability

Agendas can be reused for multiple scatter operations:

```python
# Create a reusable template
daily_cleanup_agenda = Agenda()
daily_cleanup_agenda.add(cleanup_temp_files)()
daily_cleanup_agenda.add(compress_old_logs)()
daily_cleanup_agenda.add(update_metrics)()

# Use it multiple times with different timing
await daily_cleanup_agenda.scatter(docket, over=timedelta(hours=1))

# Later, scatter the same tasks over a different window
tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
await daily_cleanup_agenda.scatter(
    docket,
    start=tomorrow,
    over=timedelta(minutes=30)
)
```

### Failure Behavior

Keep in mind that, if an error occurs during scheduling, some tasks may have already been scheduled successfully:

```python
agenda = Agenda()
agenda.add(valid_task)("arg1")
agenda.add(valid_task)("arg2")
agenda.add("nonexistent_task")("arg3")  # This will cause an error
agenda.add(valid_task)("arg4")

try:
    await agenda.scatter(docket, over=timedelta(minutes=10))
except KeyError:
    # The first two tasks were scheduled successfully
    # The error prevented the fourth task from being scheduled
    pass
```

## Striking and Restoring Tasks

Striking allows you to temporarily disable tasks without redeploying code. This is invaluable for incident response, gradual rollouts, or handling problematic customers.

### Striking Entire Task Types

Disable all instances of a specific task:

```python
# Disable all order processing during maintenance
await docket.strike(process_order)

# Orders added during this time won't be processed
await docket.add(process_order)(order_id=12345)  # Won't run
await docket.add(process_order)(order_id=67890)  # Won't run

# Re-enable when ready
await docket.restore(process_order)
```

### Striking by Parameter Values

Disable tasks based on their arguments using comparison operators:

```python
# Block all tasks for a problematic customer
await docket.strike(None, "customer_id", "==", "12345")

# Block low-priority work during high load
await docket.strike(process_order, "priority", "<=", "low")

# Block all orders above a certain value during fraud investigation
await docket.strike(process_payment, "amount", ">", 10000)

# Later, restore them
await docket.restore(None, "customer_id", "==", "12345")
await docket.restore(process_order, "priority", "<=", "low")
```

Supported operators include `==`, `!=`, `<`, `<=`, `>`, `>=`.

### Striking Specific Task-Parameter Combinations

Target very specific scenarios:

```python
# Block only high-value orders for a specific customer
await docket.strike(process_order, "customer_id", "==", "12345")
await docket.strike(process_order, "amount", ">", 1000)

# This order won't run (blocked customer)
await docket.add(process_order)(customer_id="12345", amount=500)

# This order won't run (blocked customer AND high amount)
await docket.add(process_order)(customer_id="12345", amount=2000)

# This order WILL run (different customer)
await docket.add(process_order)(customer_id="67890", amount=2000)
```

Striking is useful for incident response when you need to quickly disable failing tasks, customer management to block problematic accounts, gradual rollouts where you disable features for certain parameters, load management during high traffic, and debugging to isolate specific scenarios.

## Advanced Logging and Debugging

### Argument Logging

Control which task arguments appear in logs using the `Logged` annotation:

```python
from typing import Annotated
from docket import Logged

async def process_payment(
    customer_id: Annotated[str, Logged],           # Will be logged
    credit_card: str,                             # Won't be logged
    amount: Annotated[float, Logged()] = 0.0,    # Will be logged
    trace_id: Annotated[str, Logged] = "unknown" # Will be logged
) -> None:
    # Process the payment...
    pass

# Log output will show:
# process_payment('12345', credit_card=..., amount=150.0, trace_id='abc-123')
```

### Collection Length Logging

For large collections, log just their size instead of contents:

```python
async def bulk_update_users(
    user_ids: Annotated[list[str], Logged(length_only=True)],
    metadata: Annotated[dict[str, str], Logged(length_only=True)],
    options: Annotated[set[str], Logged(length_only=True)]
) -> None:
    # Process users...
    pass

# Log output will show:
# bulk_update_users([len 150], metadata={len 5}, options={len 3})
```

This prevents logs from being overwhelmed with large data structures while still providing useful information.

### Task Context Logging

Use `TaskLogger` for structured logging with task context:

```python
from logging import Logger, LoggerAdapter
from docket import TaskLogger

async def complex_data_pipeline(
    dataset_id: str,
    logger: LoggerAdapter[Logger] = TaskLogger()
) -> None:
    logger.info("Starting data pipeline", extra={"dataset_id": dataset_id})

    try:
        await extract_data(dataset_id)
        logger.info("Data extraction completed")

        await transform_data(dataset_id)
        logger.info("Data transformation completed")

        await load_data(dataset_id)
        logger.info("Data loading completed")

    except Exception as e:
        logger.error("Pipeline failed", extra={"error": str(e)})
        raise
```

The logger automatically includes task context like the task name, key, and worker information.

### Built-in Utility Tasks

Docket provides helpful debugging tasks:

```python
from docket import tasks

# Simple trace logging
await docket.add(tasks.trace)("System startup completed")
await docket.add(tasks.trace)("Processing batch 123")

# Intentional failures for testing error handling
await docket.add(tasks.fail)("Testing error notification system")
```

These are particularly useful for:

- Marking milestones in complex workflows
- Testing monitoring and alerting systems
- Debugging task execution order
- Creating synthetic load for testing

## Task Chain Patterns

### Sequential Processing

Create chains of related tasks that pass data forward:

```python
async def download_data(
    url: str,
    docket: Docket = CurrentDocket()
) -> None:
    file_path = await download_file(url)
    await docket.add(validate_data)(file_path)

async def validate_data(
    file_path: str,
    docket: Docket = CurrentDocket()
) -> None:
    if await is_valid_data(file_path):
        await docket.add(process_data)(file_path)
    else:
        await docket.add(handle_invalid_data)(file_path)

async def process_data(file_path: str) -> None:
    # Final processing step
    await transform_and_store(file_path)
```

### Fan-out Processing

Break large tasks into parallel subtasks:

```python
async def process_large_dataset(
    dataset_id: str,
    docket: Docket = CurrentDocket()
) -> None:
    chunk_ids = await split_dataset_into_chunks(dataset_id)

    # Schedule parallel processing of all chunks
    for chunk_id in chunk_ids:
        await docket.add(process_chunk)(dataset_id, chunk_id)

    # Schedule a task to run after all chunks should be done
    estimated_completion = datetime.now(timezone.utc) + timedelta(hours=2)
    await docket.add(
        finalize_dataset,
        when=estimated_completion,
        key=f"finalize-{dataset_id}"
    )(dataset_id, len(chunk_ids))

async def process_chunk(dataset_id: str, chunk_id: str) -> None:
    await process_data_chunk(dataset_id, chunk_id)
    await mark_chunk_complete(dataset_id, chunk_id)
```

### Conditional Workflows

Tasks can make decisions about what work to schedule next:

```python
async def analyze_user_behavior(
    user_id: str,
    docket: Docket = CurrentDocket()
) -> None:
    behavior_data = await collect_user_behavior(user_id)

    if behavior_data.indicates_churn_risk():
        await docket.add(create_retention_campaign)(user_id)
    elif behavior_data.indicates_upsell_opportunity():
        await docket.add(create_upsell_campaign)(user_id)
    elif behavior_data.indicates_satisfaction():
        # Schedule a follow-up check in 30 days
        future_check = datetime.now(timezone.utc) + timedelta(days=30)
        await docket.add(
            analyze_user_behavior,
            when=future_check,
            key=f"behavior-check-{user_id}"
        )(user_id)
```

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

### Best Practices

1. **Choose appropriate argument names**: Use arguments that represent the resource you want to protect (database name, customer ID, API endpoint).

2. **Set reasonable limits**: Base limits on your system's capacity and external service constraints.

3. **Use descriptive scopes**: When you have multiple unrelated concurrency controls, use different scopes to avoid conflicts.

4. **Monitor blocked tasks**: Tasks that can't start due to concurrency limits are automatically rescheduled with small delays.

5. **Consider cascading effects**: Concurrency limits can create queuing effects - monitor your system to ensure tasks don't back up excessively.

Concurrency control helps you build robust systems that respect resource limits while maintaining high throughput for independent operations.

## Task State and Progress Monitoring

Docket provides comprehensive execution state tracking, progress monitoring, and result persistence. These features enable you to observe task execution in real-time, report progress to users, and retrieve results from completed tasks.

### High-Level Design

Understanding how Docket tracks and stores task execution information helps when building observable systems.

#### Execution State Machine

Every task execution transitions through a well-defined lifecycle:

```
SCHEDULED → QUEUED → RUNNING → COMPLETED
                              ↘ FAILED
```

- **SCHEDULED**: Task is scheduled and waiting in the queue for its execution time
- **QUEUED**: Task has been moved to the stream and is ready to be claimed by a worker
- **RUNNING**: Task is currently being executed by a worker
- **COMPLETED**: Task execution finished successfully
- **FAILED**: Task execution failed

State transitions are atomic and published via Redis pub/sub for real-time monitoring.

#### Redis Data Model

Docket stores execution state and progress in Redis with automatic cleanup:

**Execution State** (`{docket}:runs:{key}`):

- Stored as Redis hash containing state, timestamps, worker name, error messages, and result keys
- TTL controlled by `execution_ttl` (default: 15 minutes)
- Setting `execution_ttl=0` skips state persistence entirely for maximum throughput

**Progress Data** (`{docket}:progress:{key}`):

- Stored as Redis hash with `current`, `total`, `message`, and `updated_at` fields
- Ephemeral data deleted when task completes
- Updated atomically with `increment()` for thread-safe progress tracking

**Result Storage**:

- Results stored using `py-key-value-aio` library (RedisStore or MemoryStore)
- Serialized with cloudpickle and base64-encoded for reliability
- TTL matches `execution_ttl` for consistent cleanup
- Skipped when `execution_ttl=0` or task returns `None`

#### Pub/Sub Event System

Docket publishes real-time events for state transitions and progress updates:

**State Events** (channel: `{docket}:state:{key}`):
```python
{
    "type": "state",
    "key": "task-key",
    "state": "running",
    "when": "2025-01-15T10:30:00Z",
    "worker": "worker-1",
    "started_at": "2025-01-15T10:30:05Z",
    "completed_at": None,
    "error": None
}
```

**Progress Events** (channel: `{docket}:progress:{key}`):
```python
{
    "type": "progress",
    "key": "task-key",
    "current": 45,
    "total": 100,
    "message": "Processing records...",
    "updated_at": "2025-01-15T10:30:10Z"
}
```

These events enable real-time dashboards, progress bars, and monitoring systems to track task execution without polling.

### Tracking Execution State

Access the current state of any task execution:

```python
from docket import Docket
from docket.execution import ExecutionState

async with Docket() as docket:
    # Schedule a task
    execution = await docket.add(process_order)(order_id=12345)

    # Check initial state
    print(f"State: {execution.state}")  # ExecutionState.QUEUED

    # Later, sync with Redis to get current state
    await execution.sync()
    print(f"State: {execution.state}")  # May be RUNNING or COMPLETED

    # Check specific states
    if execution.state == ExecutionState.COMPLETED:
        print(f"Task completed at {execution.completed_at}")
    elif execution.state == ExecutionState.FAILED:
        print(f"Task failed: {execution.error}")
    elif execution.state == ExecutionState.RUNNING:
        print(f"Task running on {execution.worker} since {execution.started_at}")
```

### Reporting Task Progress

Tasks can report their progress to provide visibility into long-running operations:

```python
from docket import Progress
from docket.execution import ExecutionProgress

async def import_customer_records(
    file_path: str,
    progress: ExecutionProgress = Progress()
) -> None:
    # Read the data source
    records = await load_records(file_path)

    # Set the total number of items
    await progress.set_total(len(records))
    await progress.set_message("Starting import")

    # Process records one by one
    for i, record in enumerate(records, 1):
        await import_record(record)

        # Update progress
        await progress.increment()
        await progress.set_message(f"Imported record {i}/{len(records)}")

    await progress.set_message("Import complete")

# Schedule the task
await docket.add(import_customer_records)("/data/customers.csv")
```

Progress updates are atomic and published via pub/sub, so multiple observers can monitor the same task simultaneously.

### Monitoring Progress in Real-Time

Subscribe to progress updates programmatically:

```python
async def monitor_task_progress(execution: Execution) -> None:
    """Monitor a task's progress and state in real-time."""
    async for event in execution.subscribe():
        if event["type"] == "state":
            state = event["state"]
            print(f"State changed to: {state}")

            if state in (ExecutionState.COMPLETED, ExecutionState.FAILED):
                break

        elif event["type"] == "progress":
            current = event["current"]
            total = event["total"]
            message = event["message"]
            percentage = (current / total * 100) if total > 0 else 0
            print(f"Progress: {current}/{total} ({percentage:.1f}%) - {message}")

# Schedule a task and monitor it
execution = await docket.add(import_customer_records)("/data/large_dataset.csv")

# Monitor in a separate task
asyncio.create_task(monitor_task_progress(execution))
```

### Advanced Progress Patterns

#### Incremental Progress

For tasks with known steps, use `set_total()` and `increment()`:

```python
async def process_batch(
    batch_id: int,
    progress: ExecutionProgress = Progress()
) -> None:
    items = await fetch_batch_items(batch_id)
    await progress.set_total(len(items))

    for item in items:
        await process_item(item)
        await progress.increment()  # Increments by 1
```

#### Batch Progress Updates

For fine-grained work, batch progress updates to reduce Redis calls:

```python
async def process_large_dataset(
    dataset_id: str,
    progress: ExecutionProgress = Progress()
) -> None:
    records = await load_dataset(dataset_id)
    await progress.set_total(len(records))

    # Update every 100 records instead of every record
    for i, record in enumerate(records):
        await process_record(record)

        if (i + 1) % 100 == 0:
            await progress.increment(100)
            await progress.set_message(f"Processed {i + 1} records")

    # Update any remaining progress
    remaining = len(records) % 100
    if remaining > 0:
        await progress.increment(remaining)
```

#### Nested Progress Tracking

Break down complex tasks into subtasks with their own progress:

```python
async def data_migration(
    source_db: str,
    progress: ExecutionProgress = Progress()
) -> None:
    # Define major phases
    phases = [
        ("extract", extract_data),
        ("transform", transform_data),
        ("load", load_data),
        ("verify", verify_data)
    ]

    await progress.set_total(len(phases) * 100)

    for phase_num, (phase_name, phase_func) in enumerate(phases):
        await progress.set_message(f"Phase: {phase_name}")

        # Each phase reports its own progress (0-100)
        # We scale it to our overall progress
        phase_progress = 0
        async for update in phase_func(source_db):
            # Each phase returns progress from 0-100
            delta = update - phase_progress
            await progress.increment(delta)
            phase_progress = update
```

### Retrieving Task Results

Tasks can return values that are automatically persisted and retrievable:

```python
async def calculate_metrics(dataset_id: str) -> dict[str, float]:
    """Calculate and return metrics from a dataset."""
    data = await load_dataset(dataset_id)
    return {
        "mean": sum(data) / len(data),
        "max": max(data),
        "min": min(data),
        "count": len(data)
    }

# Schedule the task
execution = await docket.add(calculate_metrics)("dataset-2025-01")

# Later, retrieve the result
metrics = await execution.get_result()
print(f"Mean: {metrics['mean']}, Count: {metrics['count']}")
```

#### Waiting for Results

`get_result()` automatically waits for task completion if it's still running:

```python
# Schedule a task and immediately wait for its result
execution = await docket.add(fetch_external_data)("https://api.example.com/data")

# This will wait until the task completes
try:
    data = await execution.get_result()
    print(f"Retrieved {len(data)} records")
except Exception as e:
    print(f"Task failed: {e}")
```

#### Timeout and Deadline

Control how long to wait for results:

```python
from datetime import datetime, timedelta, timezone

# Wait at most 30 seconds for a result
try:
    result = await execution.get_result(timeout=timedelta(seconds=30))
except TimeoutError:
    print("Task didn't complete in 30 seconds")

# Or specify an absolute deadline
deadline = datetime.now(timezone.utc) + timedelta(minutes=5)
try:
    result = await execution.get_result(deadline=deadline)
except TimeoutError:
    print("Task didn't complete by deadline")
```

Following Python conventions, you can specify either `timeout` (relative duration) or `deadline` (absolute time), but not both.

#### Exception Handling

When tasks fail, `get_result()` re-raises the original exception:

```python
async def risky_operation(data: dict) -> str:
    if not data.get("valid"):
        raise ValueError("Invalid data provided")
    return process_data(data)

execution = await docket.add(risky_operation)({"valid": False})

try:
    result = await execution.get_result()
except ValueError as e:
    # The original ValueError is re-raised
    print(f"Validation failed: {e}")
except Exception as e:
    # Other exceptions are also preserved
    print(f"Unexpected error: {e}")
```

#### Result Patterns for Workflows

Chain tasks together using results:

```python
async def download_file(url: str) -> str:
    """Download a file and return the local path."""
    file_path = await download(url)
    return file_path

async def process_file(file_path: str) -> dict:
    """Process a file and return statistics."""
    data = await parse_file(file_path)
    return calculate_statistics(data)

# Chain tasks together
download_execution = await docket.add(download_file)("https://example.com/data.csv")
file_path = await download_execution.get_result()

# Use the result to schedule the next task
process_execution = await docket.add(process_file)(file_path)
stats = await process_execution.get_result()
print(f"Statistics: {stats}")
```

For complex workflows with many dependencies, consider using the `CurrentDocket()` dependency to schedule follow-up work from within tasks themselves.

### CLI Monitoring with Watch

Monitor task execution in real-time from the command line:

```bash
# Watch a specific task
docket watch --url redis://localhost:6379/0 --docket emails task-key-123

# The watch command shows:
# - Current state (SCHEDULED, QUEUED, RUNNING, COMPLETED, FAILED)
# - Progress bar with percentage
# - Status messages
# - Execution timing
# - Worker information
```

Example output:
```
State: RUNNING (worker-1)
Started: 2025-01-15 10:30:05

Progress: [████████████░░░░░░░░] 60/100 (60.0%)
Message: Processing records...
Updated: 2025-01-15 10:30:15
```

The watch command uses pub/sub to receive real-time updates without polling, making it efficient for monitoring long-running tasks.

### Fire-and-Forget Mode

For high-throughput scenarios where observability isn't critical, disable state and result persistence:

```python
from datetime import timedelta

async with Docket(
    name="high-throughput",
    url="redis://localhost:6379/0",
    execution_ttl=timedelta(0)  # Disable state persistence
) as docket:
    # Tasks scheduled with this docket won't track state or store results
    # This maximizes throughput for fire-and-forget operations
    for i in range(10000):
        await docket.add(send_notification)(user_id=i)
```

With `execution_ttl=0`:

- No state records are created in Redis
- No results are persisted
- Progress tracking is not available
- `get_result()` will not work

This is ideal for high-volume event processing where individual task tracking isn't necessary.

These advanced patterns enable building sophisticated distributed systems that can adapt to changing conditions, handle operational requirements, and provide the debugging and testing capabilities needed for production deployments.
