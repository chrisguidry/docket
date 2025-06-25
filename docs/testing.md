# Testing with Docket

Docket includes the utilities you need to test all your background task systems in realistic ways. The ergonomic design supports testing complex workflows with minimal setup.

## Testing Tasks as Simple Functions

Often you can test your tasks without running a worker at all! Docket tasks are just Python functions, so you can call them directly and pass test values for dependency parameters:

```python
from docket import CurrentDocket, Retry
from unittest.mock import AsyncMock

async def process_order(
    order_id: int,
    docket: Docket = CurrentDocket(),
    retry: Retry = Retry(attempts=3)
):
    # Your task logic here
    order = await fetch_order(order_id)
    await charge_payment(order)
    await docket.add(send_confirmation)(order_id)

async def test_process_order_logic():
    """Test the task logic without running a worker."""
    mock_docket = AsyncMock()

    # Call the task directly with test parameters
    await process_order(
        order_id=123,
        docket=mock_docket,
        retry=Retry(attempts=1)
    )

    # Verify the task scheduled follow-up work
    mock_docket.add.assert_called_once()
```

This approach is great for testing business logic quickly without the overhead of setting up dockets and workers.

## Testing with Pytest Fixtures

The most powerful way to test with Docket is using pytest fixtures to set up your docket and worker. This approach, used throughout Docket's own test suite, provides clean isolation and reusable test infrastructure.

### Basic Fixture Setup

Create fixtures for your test docket and worker:

```python
import pytest
from datetime import datetime, timedelta
from typing import AsyncGenerator, Callable
from uuid import uuid4
from unittest.mock import AsyncMock
from docket import Docket, Worker

@pytest.fixture
async def test_docket() -> AsyncGenerator[Docket, None]:
    """Create a test docket with a unique name for each test."""
    async with Docket(
        name=f"test-{uuid4()}",
        url="redis://localhost:6379/0"
    ) as docket:
        yield docket

@pytest.fixture
async def test_worker(test_docket: Docket) -> AsyncGenerator[Worker, None]:
    """Create a test worker with fast polling for quick tests."""
    async with Worker(
        test_docket,
        minimum_check_interval=timedelta(milliseconds=5),
        scheduling_resolution=timedelta(milliseconds=5)
    ) as worker:
        yield worker
```

### Using Fixtures in Tests

With these fixtures, your tests become much cleaner:

```python
async def send_notification(user_id: int, message: str):
    """Example task for testing."""
    print(f"Sending '{message}' to user {user_id}")

async def test_task_execution(test_docket: Docket, test_worker: Worker):
    """Test that tasks execute with correct arguments."""
    test_docket.register(send_notification)
    await test_docket.add(send_notification)(123, "Welcome!")

    await test_worker.run_until_finished()

    # Verify by checking side effects or using test doubles

async def test_idempotent_scheduling(test_docket: Docket, test_worker: Worker):
    """Test that tasks with same key don't duplicate."""
    test_docket.register(send_notification)
    key = "unique-notification"

    # Schedule same task multiple times with same key
    await test_docket.add(send_notification, key=key)(123, "message1")
    await test_docket.add(send_notification, key=key)(123, "message2")  # Should replace
    await test_docket.add(send_notification, key=key)(123, "message3")  # Should replace

    # Verify only one task is scheduled
    snapshot = await test_docket.snapshot()
    assert len([t for t in snapshot.future if t.key == key]) == 1
```

## Running Until Finished

For tests and batch processing, use [`run_until_finished()`](api-reference.md#docket.Worker.run_until_finished) to process all pending tasks then stop:

```python
async def test_order_processing():
    async with Docket() as docket:
        docket.register(process_order)
        docket.register(send_confirmation)
        docket.register(update_inventory)

        # Schedule some work
        await docket.add(process_order)(order_id=123)
        await docket.add(send_confirmation)(order_id=123)
        await docket.add(update_inventory)(product_id=456)

        # Process all pending tasks
        async with Worker(docket) as worker:
            await worker.run_until_finished()

        # Now verify results
        assert order_is_processed(123)
        assert confirmation_was_sent(123)
        assert inventory_was_updated(456)
```

This works well for testing workflows where you need to ensure all tasks complete before making assertions.

### Testing Task Registration

Test that tasks are properly registered and can be called by name:

```python
async def test_task_registration_by_name(test_docket: Docket, test_worker: Worker):
    """Test executing tasks by string name."""
    async def example_task(data: str):
        print(f"Processing: {data}")

    test_docket.register(example_task)

    # Execute by name instead of function reference
    await test_docket.add("example_task")("test data")

    await test_worker.run_until_finished()

    # Verify by checking side effects or logs
```

## Controlling Perpetual Tasks

Use [`run_at_most()`](api-reference.md#docket.Worker.run_at_most) to limit how many times specific tasks run, which is essential for testing perpetual tasks:

```python
async def test_perpetual_monitoring():
    async with Docket() as docket:
        docket.register(health_check_service)
        docket.register(process_data)
        docket.register(send_reports)

        # This would normally run forever
        await docket.add(health_check_service)("https://api.example.com")

        # Also schedule some regular tasks
        await docket.add(process_data)(dataset="test")
        await docket.add(send_reports)()

        async with Worker(docket) as worker:
            # Let health check run 3 times, everything else runs to completion
            await worker.run_at_most({"health_check_service": 3})

        # Verify the health check ran the expected number of times
        assert health_check_call_count == 3
```

The [`run_at_most()`](api-reference.md#docket.Worker.run_at_most) method takes a dictionary mapping task names to maximum execution counts. Tasks not in the dictionary run to completion as normal.

## Testing Self-Perpetuating Chains

For tasks that create chains of future work, you can control the chain length:

```python
async def test_batch_processing_chain():
    async with Docket() as docket:
        docket.register(process_batch)

        # This creates a chain: batch 1 -> batch 2 -> batch 3
        await docket.add(process_batch, key="batch-job")(batch_id=1, total_batches=3)

        async with Worker(docket) as worker:
            # Let this specific key run 3 times (for 3 batches)
            await worker.run_at_most({"batch-job": 3})

        # Verify all batches were processed
        assert all_batches_processed([1, 2, 3])
```

You can use task keys in [`run_at_most()`](api-reference.md#docket.Worker.run_at_most) to control specific task instances rather than all tasks of a given type.


## Testing Dependencies

When testing tasks with custom dependencies, create simple test implementations that provide predictable behavior:

```python
from unittest.mock import AsyncMock
from docket import Depends

async def test_database():
    """Simple test database that tracks calls."""
    db = AsyncMock()
    db.fetch_user.return_value = {"id": 123, "name": "Test User"}
    return db

async def test_api_client():
    """Test API client with predictable responses."""
    client = AsyncMock()
    client.send_notification.return_value = {"message_id": "test-123"}
    return client

async def sync_user_data(
    user_id: int,
    db=Depends(test_database),
    api=Depends(test_api_client)
):
    user = await db.fetch_user(user_id)
    result = await api.send_notification(
        user["name"], "Account updated"
    )
    return result["message_id"]

async def test_dependency_injection(test_docket: Docket, test_worker: Worker):
    """Test that dependencies are injected correctly."""
    test_docket.register(sync_user_data)
    await test_docket.add(sync_user_data)(123)

    await test_worker.run_until_finished()

    # Verify the task executed with our test dependencies
    # (Check side effects, logs, or returned values as appropriate)
```

This approach gives you full control over dependency behavior and makes tests deterministic.

## Testing Task Scheduling

Test that tasks are scheduled correctly without running them:

```python
async def test_scheduling_logic():
    async with Docket() as docket:
        docket.register(send_reminder)

        # Schedule some tasks
        future_time = datetime.now(timezone.utc) + timedelta(hours=1)
        await docket.add(send_reminder, when=future_time, key="reminder-123")(
            customer_id=123,
            message="Your subscription expires soon"
        )

        # Check that task was scheduled (but not executed)
        snapshot = await docket.snapshot()

        assert len(snapshot.future) == 1
        assert len(snapshot.running) == 0
        assert snapshot.future[0].key == "reminder-123"
        assert snapshot.future[0].function.__name__ == "send_reminder"
```

## Integration Testing with Real Redis

For integration tests, use a real Redis instance but with a test-specific docket name:

```python
import pytest
from redis.asyncio import Redis

@pytest.fixture
async def test_docket():
    # Use a unique docket name for each test
    test_name = f"test-{uuid4()}"

    async with Docket(name=test_name, url="redis://localhost:6379/1") as docket:
        yield docket

        # Clean up after test
        await docket.clear()

async def test_full_workflow(test_docket):
    test_docket.register(process_order)
    test_docket.register(send_confirmation)

    await test_docket.add(process_order)(order_id=123)

    async with Worker(test_docket) as worker:
        await worker.run_until_finished()

    # Verify against real external systems
    assert order_exists_in_database(123)
    assert email_was_sent_to_customer(123)
```

## Best Practices for Testing

### Use Descriptive Task Keys

Use meaningful task keys in tests to make debugging easier:

```python
# Good: Clear what this task represents
await docket.add(process_order, key=f"test-order-{order_id}")(order_id)

# Less clear: Generic key doesn't help with debugging
await docket.add(process_order, key=f"task-{uuid4()}")(order_id)
```

### Test Error Scenarios

Always test what happens when tasks fail:

```python
async def test_order_processing_failure():
    # Simulate a failing external service
    with mock.patch('external_service.process_payment', side_effect=PaymentError):
        await docket.add(process_order)(order_id=123)

        async with Worker(docket) as worker:
            await worker.run_until_finished()

        # Verify error handling
        assert order_status(123) == "payment_failed"
        assert error_notification_sent()
```

### Test Idempotency

Verify that tasks with the same key don't create duplicate work:

```python
async def test_idempotent_scheduling():
    key = "process-order-123"

    # Schedule the same task multiple times
    await docket.add(process_order, key=key)(order_id=123)
    await docket.add(process_order, key=key)(order_id=123)
    await docket.add(process_order, key=key)(order_id=123)

    snapshot = await docket.snapshot()

    # Should only have one task scheduled
    assert len(snapshot.future) == 1
    assert snapshot.future[0].key == key
```

### Test Timing-Sensitive Logic

For tasks that depend on timing, use controlled time in tests:

```python
async def test_scheduled_task_timing():
    now = datetime.now(timezone.utc)
    future_time = now + timedelta(seconds=10)

    await docket.add(send_reminder, when=future_time)(customer_id=123)

    # Task should not run immediately
    async with Worker(docket) as worker:
        await worker.run_until_finished()

    assert not reminder_was_sent(123)

    # Fast-forward time and test again
    with mock.patch('docket.datetime') as mock_datetime:
        mock_datetime.now.return_value = future_time + timedelta(seconds=1)

        async with Worker(docket) as worker:
            await worker.run_until_finished()

        assert reminder_was_sent(123)
```

Docket's testing utilities make it straightforward to write comprehensive tests for even complex distributed task workflows. The key is using [`run_until_finished()`](api-reference.md#docket.Worker.run_until_finished) for deterministic execution and [`run_at_most()`](api-reference.md#docket.Worker.run_at_most) for controlling perpetual or self-scheduling tasks.
