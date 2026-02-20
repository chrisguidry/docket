# Task Design Patterns

Docket is made for building complex distributed systems, and the patterns below highlight some of the original use cases for Docket.

## Find & Flood Pattern

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
