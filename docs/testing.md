# Testing with Docket

Docket includes the utilities you need to test all your background task systems in realistic ways. The ergonomic design supports testing complex workflows with minimal setup.

## Using In-Memory Backend (No Redis Required)

For the fastest tests and simplest setup, Docket supports an in-memory backend using [fakeredis](https://github.com/cunla/fakeredis-py). This is perfect for:

- **CI/CD environments** - No need to spin up Redis containers
- **Local development** - Test without installing/running Redis
- **Unit tests** - Fast, isolated tests without external dependencies
- **Educational environments** - Workshops and tutorials without infrastructure

### Installation

Fakeredis is included as a standard dependency, so no extra installation is needed.

### Usage

Use the `memory://` URL scheme to enable the in-memory backend:

```python
from docket import Docket

async with Docket(name="test-docket", url="memory://test") as docket:
    # Use docket normally - all operations are in-memory
    docket.register(my_task)
    await docket.add(my_task)("arg")
```

### Multiple In-Memory Dockets

You can run multiple independent in-memory dockets simultaneously by using different URLs:

```python
async with (
    Docket(name="service-a", url="memory://service-a") as docket_a,
    Docket(name="service-b", url="memory://service-b") as docket_b,
):
    # Each docket has its own isolated in-memory data
    await docket_a.add(task_a)()
    await docket_b.add(task_b)()
```

This is useful for testing multi-service scenarios in a single process.

### Pytest Fixture Example

```python
import pytest
from docket import Docket, Worker
from uuid import uuid4

@pytest.fixture
async def test_docket() -> AsyncGenerator[Docket, None]:
    """Create a test docket with in-memory backend."""
    async with Docket(
        name=f"test-{uuid4()}",
        url=f"memory://test-{uuid4()}"
    ) as docket:
        yield docket
```

### Limitations

The in-memory backend has some limitations compared to real Redis:

- **Single process only** - Cannot distribute work across multiple processes/machines
- **Data is ephemeral** - Lost when the process exits
- **Performance may differ** - Timing-sensitive tests may behave differently
- **Async polling behavior** - Uses non-blocking reads with manual sleeps for proper asyncio integration

For integration tests or multi-worker scenarios across processes, use a real Redis instance.

## Testing Tasks as Simple Functions

Often you can test your tasks without running a worker at all! Docket tasks are just Python functions, so you can call them directly and pass test values for dependency parameters:

```python
from docket import Docket, CurrentDocket, Retry
from unittest.mock import AsyncMock

async def process_order(
    order_id: int,
    docket: Docket = CurrentDocket(),
    retry: Retry = Retry(attempts=3)
) -> None:
    # Your task logic here
    order = await fetch_order(order_id)
    await charge_payment(order)
    await docket.add(send_confirmation)(order_id)

async def test_process_order_logic() -> None:
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
async def send_notification(user_id: int, message: str) -> None:
    """Example task for testing."""
    print(f"Sending '{message}' to user {user_id}")

async def test_task_execution(test_docket: Docket, test_worker: Worker) -> None:
    """Test that tasks execute with correct arguments."""
    test_docket.register(send_notification)
    await test_docket.add(send_notification)(123, "Welcome!")

    await test_worker.run_until_finished()

    # Verify by checking side effects or using test doubles

async def test_idempotent_scheduling(test_docket: Docket, test_worker: Worker) -> None:
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
async def test_order_processing(test_docket: Docket, test_worker: Worker) -> None:
    """Test order processing workflow."""
    test_docket.register(process_order)
    test_docket.register(send_confirmation)
    test_docket.register(update_inventory)

    # Schedule some work
    await test_docket.add(process_order)(order_id=123)
    await test_docket.add(send_confirmation)(order_id=123)
    await test_docket.add(update_inventory)(product_id=456)

    # Process all pending tasks
    await test_worker.run_until_finished()

    # Now verify results
    assert order_is_processed(123)
    assert confirmation_was_sent(123)
    assert inventory_was_updated(456)
```

This works well for testing workflows where you need to ensure all tasks complete before making assertions.

### Testing Task Registration

Test that tasks are properly registered and can be called by name:

```python
async def test_task_registration_by_name(test_docket: Docket, test_worker: Worker) -> None:
    """Test executing tasks by string name."""
    async def example_task(data: str) -> None:
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
async def test_perpetual_monitoring(test_docket: Docket, test_worker: Worker) -> None:
    """Test perpetual task monitoring."""
    test_docket.register(health_check_service)
    test_docket.register(process_data)
    test_docket.register(send_reports)

    # This would normally run forever
    await test_docket.add(health_check_service)("https://api.example.com")

    # Also schedule some regular tasks
    await test_docket.add(process_data)(dataset="test")
    await test_docket.add(send_reports)()

    # Let health check run 3 times, everything else runs to completion
    await test_worker.run_at_most({"health_check_service": 3})

    # Verify the health check ran the expected number of times
    assert health_check_call_count == 3
```

The [`run_at_most()`](api-reference.md#docket.Worker.run_at_most) method takes a dictionary mapping task names to maximum execution counts. Tasks not in the dictionary run to completion as normal.

## Testing Self-Perpetuating Chains

For tasks that create chains of future work, you can control the chain length:

```python
async def test_batch_processing_chain(test_docket: Docket, test_worker: Worker) -> None:
    """Test batch processing chains."""
    test_docket.register(process_batch)

    # This creates a chain: batch 1 -> batch 2 -> batch 3
    await test_docket.add(process_batch, key="batch-job")(batch_id=1, total_batches=3)

    # Let this specific key run 3 times (for 3 batches)
    await test_worker.run_at_most({"batch-job": 3})

    # Verify all batches were processed
    assert all_batches_processed([1, 2, 3])
```

You can use task keys in [`run_at_most()`](api-reference.md#docket.Worker.run_at_most) to control specific task instances rather than all tasks of a given type.

## Testing Task Scheduling

Test that tasks are scheduled correctly without running them:

```python
from datetime import datetime, timedelta, timezone

async def test_scheduling_logic(test_docket: Docket) -> None:
    """Test task scheduling without execution."""
    test_docket.register(send_reminder)

    # Schedule some tasks
    future_time = datetime.now(timezone.utc) + timedelta(hours=1)
    await test_docket.add(send_reminder, when=future_time, key="reminder-123")(
        customer_id=123,
        message="Your subscription expires soon"
    )

    # Check that task was scheduled (but not executed)
    snapshot = await test_docket.snapshot()

    assert len(snapshot.future) == 1
    assert len(snapshot.running) == 0
    assert snapshot.future[0].key == "reminder-123"
    assert snapshot.future[0].function.__name__ == "send_reminder"
```

## Integration Testing with Real Redis

For integration tests, use a real Redis instance but with a test-specific docket name:

```python
import pytest
from typing import AsyncGenerator
from uuid import uuid4
from docket import Docket, Worker
from redis.asyncio import Redis

@pytest.fixture
async def test_docket() -> AsyncGenerator[Docket, None]:
    # Use a unique docket name for each test
    test_name = f"test-{uuid4()}"

    async with Docket(name=test_name, url="redis://localhost:6379/1") as docket:
        yield docket

        # Clean up after test
        await docket.clear()

async def test_full_workflow(test_docket: Docket) -> None:
    test_docket.register(process_order)
    test_docket.register(send_confirmation)

    await test_docket.add(process_order)(order_id=123)

    async with Worker(test_docket) as worker:
        await worker.run_until_finished()

    # Verify against real external systems
    assert order_exists_in_database(123)
    assert email_was_sent_to_customer(123)
```

## Testing Guidelines

### Use Descriptive Task Keys

Use meaningful task keys in tests to make debugging easier:

```python
from uuid import uuid4

# Good: Clear what this task represents
await test_docket.add(process_order, key=f"test-order-{order_id}")(order_id)

# Less clear: Generic key doesn't help with debugging
await test_docket.add(process_order, key=f"task-{uuid4()}")(order_id)
```

### Test Error Scenarios

Always test what happens when tasks fail:

```python
from unittest import mock
async def test_order_processing_failure(test_docket: Docket, test_worker: Worker) -> None:
    """Test error handling in order processing."""
    test_docket.register(process_order)

    # Simulate a failing external service
    with mock.patch('external_service.process_payment', side_effect=PaymentError):
        await test_docket.add(process_order)(order_id=123)

        await test_worker.run_until_finished()

        # Verify error handling
        assert order_status(123) == "payment_failed"
        assert error_notification_sent()
```

### Test Idempotency

Verify that tasks with the same key don't create duplicate work:

```python
async def test_idempotent_scheduling(test_docket: Docket) -> None:
    """Test idempotent task scheduling."""
    test_docket.register(process_order)
    key = "process-order-123"

    # Schedule the same task multiple times
    await test_docket.add(process_order, key=key)(order_id=123)
    await test_docket.add(process_order, key=key)(order_id=123)
    await test_docket.add(process_order, key=key)(order_id=123)

    snapshot = await test_docket.snapshot()

    # Should only have one task scheduled
    assert len(snapshot.future) == 1
    assert snapshot.future[0].key == key
```

### Test Timing-Sensitive Logic

For tasks that depend on timing, use controlled time in tests:

```python
from datetime import datetime, timedelta, timezone
from unittest import mock

async def test_scheduled_task_timing(test_docket: Docket, test_worker: Worker) -> None:
    """Test timing-sensitive task scheduling."""
    test_docket.register(send_reminder)
    now = datetime.now(timezone.utc)
    future_time = now + timedelta(seconds=10)

    await test_docket.add(send_reminder, when=future_time)(customer_id=123)

    # Task should not run immediately
    await test_worker.run_until_finished()

    assert not reminder_was_sent(123)

    # Fast-forward time and test again
    with mock.patch('docket.datetime') as mock_datetime:
        mock_datetime.now.return_value = future_time + timedelta(seconds=1)

        await test_worker.run_until_finished()

        assert reminder_was_sent(123)
```

Docket's testing utilities make it straightforward to write comprehensive tests for even complex distributed task workflows. The key is using [`run_until_finished()`](api-reference.md#docket.Worker.run_until_finished) for deterministic execution and [`run_at_most()`](api-reference.md#docket.Worker.run_at_most) for controlling perpetual or self-scheduling tasks.
