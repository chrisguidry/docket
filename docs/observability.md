# Task Observability

Docket tracks execution state, progress, and results for every task. These features let you observe task execution in real-time, report progress to users, and retrieve results from completed tasks.

## Tracking Execution State

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

## Monitoring Progress in Real-Time

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

## Progress Patterns

### Incremental Progress

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

### Batch Progress Updates

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

### Nested Progress Tracking

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

## Retrieving Task Results

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

### Waiting for Results

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

### Timeout and Deadline

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

### Exception Handling

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

### Result Patterns for Workflows

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

## Logging and Debugging

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

## CLI Monitoring with Watch

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
