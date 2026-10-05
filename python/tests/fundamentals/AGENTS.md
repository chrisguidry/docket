# Fundamentals Tests

These tests serve as **living documentation** for docket's core features. They should
be the first place a newcomer looks to understand how docket works.

## Purpose

The fundamentals tests demonstrate the **developer experience** of using docket. They
show how to use each feature clearly and concisely, without getting bogged down in
implementation details or edge cases.

Think of these tests as **examples in a tutorial**, not as exhaustive test coverage.

## Guidelines

### Keep tests simple and focused

Each test should demonstrate **one concept**. A newcomer should be able to read a test
and immediately understand what feature it's showing.

**Good:**

```python
async def test_immediate_task_execution(docket: Docket, worker: Worker):
    """Tasks are executed immediately when added without a scheduled time."""
    results = []

    async def my_task(value: str):
        results.append(value)

    await docket.add(my_task)("hello")
    await worker.run_until_finished()

    assert results == ["hello"]
```

**Bad:**

```python
async def test_immediate_task_execution(docket: Docket, worker: Worker):
    """Test immediate execution with various argument types and error conditions."""
    # Testing too many things at once
    for value in ["string", 123, None, {"key": "value"}]:
        async def my_task(v):
            if v is None:
                raise ValueError("None not allowed")
            return v

        try:
            await docket.add(my_task)(value)
            await worker.run_until_finished()
        except ValueError:
            pass
        # ... continues for 50 more lines
```

### Use real task functions, not mocks

When possible, define actual async functions instead of using `AsyncMock`. This shows
developers what their real code will look like.

**Good:**

```python
async def test_retry_on_failure(docket: Docket, worker: Worker):
    """Tasks can automatically retry when they fail."""
    attempts = 0

    async def flaky_task(retry: Retry = Retry(attempts=3)):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("Temporary failure")

    await docket.add(flaky_task)()
    await worker.run_until_finished()

    assert attempts == 3
```

**Bad:**

```python
async def test_retry_on_failure(docket: Docket, worker: Worker, the_task: AsyncMock):
    """Test retry behavior."""
    the_task.side_effect = [Exception(), Exception(), None]
    # Hard to understand what's being demonstrated
```

### Write descriptive docstrings

The docstring should explain **what behavior is being demonstrated**, not just repeat
the test name. A newcomer should understand the feature from the docstring alone.

**Good:**

```python
async def test_perpetual_tasks_can_cancel_themselves(docket: Docket, worker: Worker):
    """A perpetual task can stop its own rescheduling by calling perpetual.cancel()."""
```

**Bad:**

```python
async def test_perpetual_cancel(docket: Docket, worker: Worker):
    """Test perpetual cancellation."""
```

### Avoid implementation details

Don't test internal Redis keys, specific timing behavior, or other implementation
details. Focus on the public API and observable behavior.

**Good:**

```python
async def test_task_keys_prevent_duplicates(docket: Docket, worker: Worker):
    """Adding a task with the same key twice only runs the first one."""
    runs = []

    async def my_task(value: str):
        runs.append(value)

    await docket.add(my_task, key="unique-key")("first")
    await docket.add(my_task, key="unique-key")("second")  # Ignored
    await worker.run_until_finished()

    assert runs == ["first"]
```

**Bad:**

```python
async def test_task_keys_prevent_duplicates(docket: Docket, worker: Worker):
    """Test that duplicate keys are stored correctly in Redis."""
    await docket.add(my_task, key="unique-key")("first")

    # Don't dig into Redis internals
    async with docket.redis() as r:
        assert await r.exists("test-docket:unique-key")
        assert await r.hget("test-docket:unique-key", "args") == b'["first"]'
```

### Don't test edge cases here

Edge cases, error conditions, and thorough coverage belong in the other test files.
The fundamentals tests should show the **happy path**.

**Good (for fundamentals):**

```python
async def test_scheduled_execution(docket: Docket, worker: Worker):
    """Tasks can be scheduled to run at a specific time."""
    when = datetime.now(timezone.utc) + timedelta(seconds=1)
    await docket.add(my_task, when=when)()
    await worker.run_until_finished()
```

**Bad (move to another test file):**

```python
async def test_scheduled_execution_with_past_time(docket: Docket, worker: Worker):
    """What happens if you schedule a task for the past?"""

async def test_scheduled_execution_with_none_time(docket: Docket, worker: Worker):
    """What happens if when=None?"""

async def test_scheduled_execution_with_naive_datetime(docket: Docket, worker: Worker):
    """What happens with timezone-naive datetimes?"""
```

## File Organization

Each file covers one feature area:

- `test_scheduling.py` - Adding and scheduling tasks
- `test_idempotency.py` - Task key behavior
- `test_cancellation.py` - Cancelling tasks
- `test_retries.py` - Retry strategies
- `test_perpetual.py` - Self-rescheduling tasks
- `test_timeouts.py` - Task timeouts
- `test_context_injection.py` - Accessing current docket/worker/execution
- `test_progress_state.py` - Progress tracking
- etc.

## When in Doubt

Ask yourself: "Would this test help someone learning docket for the first time?"

If the answer is no, the test probably belongs in a different test file.
