# Testing with Docket

Docket includes the utilities you need to test all your background task systems in realistic ways. The ergonomic design supports testing complex workflows with minimal setup.

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

# Mock task fixture for testing
@pytest.fixture
def mock_task() -> AsyncMock:
    task = AsyncMock()
    task.__name__ = "mock_task"
    return task

@pytest.fixture
def now() -> Callable[[], datetime]:
    """Consistent time source for tests."""
    from functools import partial
    from datetime import timezone
    return partial(datetime.now, timezone.utc)
```

### Using Fixtures in Tests

With these fixtures, your tests become much cleaner:

```python
async def test_task_execution(
    test_docket: Docket, test_worker: Worker, mock_task: AsyncMock
):
    """Test that tasks execute with correct arguments."""
    await test_docket.add(mock_task)("arg1", "arg2", keyword="value")

    await test_worker.run_until_finished()

    mock_task.assert_awaited_once_with("arg1", "arg2", keyword="value")

async def test_idempotent_scheduling(
    test_docket: Docket, test_worker: Worker, mock_task: AsyncMock
):
    """Test that tasks with same key don't duplicate."""
    key = "unique-task-key"

    # Schedule same task multiple times with same key
    await test_docket.add(mock_task, key=key)("data1")
    await test_docket.add(mock_task, key=key)("data2")  # Should replace
    await test_docket.add(mock_task, key=key)("data3")  # Should replace

    await test_worker.run_until_finished()

    # Should only execute once with the last data
    mock_task.assert_awaited_once_with("data3")
```

### Time-based Testing Fixtures

For testing scheduled tasks, add a time fixture:

```python
from datetime import datetime, timezone
from functools import partial

@pytest.fixture
def now():
    """Consistent time source for tests."""
    return partial(datetime.now, timezone.utc)

async def test_scheduled_execution(test_docket, test_worker, mock_task, now):
    """Test that tasks execute at the right time."""
    # Schedule for 100ms in the future
    when = now() + timedelta(milliseconds=100)
    await test_docket.add(mock_task, when=when)("scheduled data")

    await test_worker.run_until_finished()

    # Verify it ran and that enough time passed
    mock_task.assert_awaited_once_with("scheduled data")
    assert now() >= when
```

## Running Until Finished

For tests and batch processing, use `run_until_finished()` to process all pending tasks then stop:

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
async def test_task_registration_by_name(test_docket, test_worker, mock_task):
    """Test executing tasks by string name."""
    test_docket.register(mock_task)

    # Execute by name instead of function reference
    await test_docket.add("mock_task")("test data")

    await test_worker.run_until_finished()

    mock_task.assert_awaited_once_with("test data")
```

## Controlling Perpetual Tasks

Use `run_at_most()` to limit how many times specific tasks run, which is essential for testing perpetual tasks:

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

The `run_at_most()` method takes a dictionary mapping task names to maximum execution counts. Tasks not in the dictionary run to completion as normal.

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

You can use task keys in `run_at_most()` to control specific task instances rather than all tasks of a given type.

## Testing with Built-in Utility Tasks

Docket provides helpful debugging tasks that are particularly useful in tests:

```python
from docket import tasks

async def test_workflow_milestones():
    async with Docket() as docket:
        # Use trace tasks to mark workflow milestones
        await docket.add(tasks.trace)("Starting workflow test")
        await docket.add(process_data)(dataset="test")
        await docket.add(tasks.trace)("Data processing scheduled")
        await docket.add(send_notification)("Processing complete")
        await docket.add(tasks.trace)("Workflow test complete")

        async with Worker(docket) as worker:
            await worker.run_until_finished()

        # Verify all milestones were logged
        assert "Starting workflow test" in captured_logs
        assert "Workflow test complete" in captured_logs

async def test_error_handling():
    async with Docket() as docket:
        # Use fail task to test error handling
        await docket.add(tasks.fail)("Simulated failure for testing")
        await docket.add(handle_error_notification)()

        async with Worker(docket) as worker:
            await worker.run_until_finished()

        # Verify error handling worked correctly
        assert error_was_handled()
```

These utility tasks are useful for:
- Marking milestones in complex workflows
- Testing monitoring and alerting systems
- Debugging task execution order
- Creating synthetic failures for error handling tests

## Testing Retry Logic

Test retry behavior by using tasks that fail a specific number of times:

```python
class FailingService:
    def __init__(self, fail_count: int):
        self.fail_count = fail_count
        self.attempts = 0

    async def unreliable_operation(self):
        self.attempts += 1
        if self.attempts <= self.fail_count:
            raise Exception(f"Attempt {self.attempts} failed")
        return "success"

async def test_retry_behavior():
    service = FailingService(fail_count=2)

    async def retrying_task(
        retry: Retry = Retry(attempts=3, delay=timedelta(milliseconds=100))
    ):
        return await service.unreliable_operation()

    async with Docket() as docket:
        docket.register(retrying_task)
        await docket.add(retrying_task)()

        async with Worker(docket) as worker:
            await worker.run_until_finished()

        # Verify it succeeded on the third attempt
        assert service.attempts == 3
```

## Testing Dependencies

Test tasks with dependencies by providing mock implementations:

```python
async def mock_database():
    return MockDatabase()

async def mock_api_client():
    return MockAPIClient()

async def test_task_with_dependencies():
    async def data_sync_task(
        db=Depends(mock_database),
        api=Depends(mock_api_client)
    ):
        data = await api.fetch_data()
        await db.store_data(data)
        return len(data)

    async with Docket() as docket:
        docket.register(data_sync_task)
        await docket.add(data_sync_task)()

        async with Worker(docket) as worker:
            await worker.run_until_finished()

        # Verify mocks were used correctly
        assert mock_api_client.fetch_data_called
        assert mock_database.store_data_called
```

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

Docket's testing utilities make it straightforward to write comprehensive tests for even complex distributed task workflows. The key is using `run_until_finished()` for deterministic execution and `run_at_most()` for controlling perpetual or self-scheduling tasks.
