# Getting Started with docket

This guide will help you get up and running with docket for background task processing.

## Prerequisites

- Python 3.12 or higher
- Redis server (local or remote)

## Installation

Install docket using pip:

```bash
pip install pydocket
```

## Basic Usage

### 1. Configure Your Docket

First, create a Docket instance that will manage your tasks:

```python
from docket import Docket

async with Docket(
    name="my-docket",  # Optional name for your docket instance
    url="redis://localhost:6379/0",  # Redis connection URL
    heartbeat_interval=timedelta(seconds=2),  # How often workers send heartbeats
    missed_heartbeats=5,  # How many missed heartbeats before a worker is considered dead
) as docket:
    # Your docket usage here
    pass
```

### 2. Define Tasks

Tasks are async Python functions decorated with `@docket.add`:

```python
from datetime import timedelta, datetime, timezone

@docket.add
async def process_user_data(user_id: str):
    # Your processing logic here
    print(f"Processing data for user {user_id}")

@docket.add
async def send_daily_report(email: str):
    # Report generation and sending logic
    print(f"Sending daily report to {email}")
```

### 3. Schedule Tasks

You can schedule tasks for immediate or future execution:

```python
# Schedule for immediate execution
await process_user_data.schedule(user_id="123")

# Schedule for future execution with a delay
await send_daily_report.schedule(
    when=datetime.now(timezone.utc) + timedelta(days=1),
    email="user@example.com"
)

# Schedule with a unique key for idempotency
await send_daily_report.schedule(
    key="daily-report-2024-03-20",
    when=datetime.now(timezone.utc) + timedelta(days=1),
    email="user@example.com"
)
```

### 4. Run a Worker

To process tasks, you need to run at least one worker. You can do this from the command line:

```bash
python -m docket worker
```

Or programmatically:

```python
from docket import Worker

async with docket, Worker(docket) as worker:
    await worker.run()
```

## Advanced Features

### Task Dependencies

docket provides powerful dependency injection for tasks:

```python
from docket import Depends, CurrentDocket, TaskLogger

async def get_user_profile(user_id: str) -> dict:
    # Fetch user profile
    return {"id": user_id, "name": "Example User"}

@docket.add
async def process_user(
    user_id: str,
    profile: dict = Depends(get_user_profile),
    logger: TaskLogger = Depends(TaskLogger),
    docket: Docket = Depends(CurrentDocket),
):
    logger.info("Processing user", user_id=user_id, profile=profile)
    # Your processing logic here
```

### Task Retries and Timeouts

Configure retry policies and timeouts for your tasks:

```python
from docket import ExponentialRetry, Timeout

@docket.add
@Timeout(seconds=30)  # Task will be cancelled if it runs longer than 30 seconds
@ExponentialRetry(max_attempts=3)  # Retry up to 3 times with exponential backoff
async def sensitive_operation():
    try:
        # Your operation here
        pass
    except TemporaryError:
        # Task will be retried with exponential backoff
        raise
    except PermanentError:
        # Task will not be retried
        pass
```

### Self-Perpetuating Tasks

Create tasks that reschedule themselves:

```python
from docket import Perpetual

@docket.add
@Perpetual(interval=timedelta(minutes=5))
async def monitor_system_health():
    # Check system health
    status = await check_health()
    if status.needs_attention:
        await notify_admin(status)

    # Task will automatically reschedule itself for 5 minutes later
```

### Task Strikes

Temporarily disable tasks based on conditions:

```python
# Strike all tasks for a specific user
await docket.strike(
    parameter="user_id",
    operator="==",
    value="problematic_user"
)

# Restore tasks for that user
await docket.restore(
    parameter="user_id",
    operator="==",
    value="problematic_user"
)
```

## Testing

docket is designed to be easily testable:

```python
import pytest
from docket import Docket, Worker

@pytest.mark.asyncio
async def test_my_task():
    async with Docket() as docket, Worker(docket) as worker:
        # Schedule a task
        execution = await my_task.schedule(arg1="value")

        # Process pending tasks
        await worker.run_once()

        # Get a snapshot of the docket state
        snapshot = await docket.snapshot()
        assert execution.key not in [e.key for e in snapshot.running]
```

## Next Steps

- Check out the [API Reference](api-reference.md) for detailed documentation
- Explore example projects in the [GitHub repository](https://github.com/chrisguidry/docket/tree/main/examples)
- Join the community discussions on GitHub
