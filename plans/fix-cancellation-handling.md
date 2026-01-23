# Fix Asyncio Cancellation Handling

## Problem Summary

Docket has several incorrect patterns for handling asyncio cancellation:

1. **`docket.cancel()` doesn't work for tasks with timeouts** - External cancellation gets converted to `TimeoutError` in `_run_function_with_timeout`
2. **Cleanup code swallows CancelledError** - Shield + catch pattern prevents cancellation from propagating
3. **No differentiation between cancellation sources** - Can't tell "I cancelled this" from "someone cancelled me"

## User Requirements

1. `docket.cancel()` must actually cancel tasks
2. Clean shutdown of workers/dockets in code (critical for tests)
3. Ctrl+C should be respected and propagate properly

## The Correct Pattern

Use sentinel messages with `task.cancel(msg=...)` to identify our own cancellations. The `msg` parameter was added in Python 3.9 and is accessible via `CancelledError.args[0]`.

```python
# Sentinel messages for internal cancellation - human-readable and identifiable
CANCEL_MSG_TIMEOUT = "Task exceeded its timeout"
CANCEL_MSG_CLEANUP = "Worker shutting down"

# When WE cancel a task, always pass our sentinel
task.cancel(CANCEL_MSG_TIMEOUT)

# When catching CancelledError, check the message
try:
    await task
except asyncio.CancelledError as e:
    if e.args and e.args[0] == CANCEL_MSG_TIMEOUT:
        # We cancelled this ourselves - safe to suppress/convert
        raise asyncio.TimeoutError
    else:
        # External cancellation (no args, or different message) - propagate
        raise
```

**Key insight**: By always passing a sentinel message when WE cancel tasks, we can distinguish "I cancelled this" from "someone cancelled me" by checking `e.args`. This works consistently on Python 3.10+.

### Why This Approach (Cross-Version Compatibility)

| Version | `cancel(msg=)` | `cancelling()` | `uncancel()` | Notes |
|---------|---------------|----------------|--------------|-------|
| 3.10 | ✅ | ❌ | ❌ | msg available via `e.args[0]` inside task |
| 3.11 | ✅ | ✅ | ✅ | msg propagates to awaiter; structured concurrency methods added |
| 3.12 | ✅ | ✅ | ✅ | Some TaskGroup edge case bugs (fixed in patches) |
| 3.13 | ✅ | ✅ | ✅ | Improved simultaneous internal/external cancellation handling |
| 3.14 | ✅ | ✅ | ✅ | Performance improvements, free-threaded support |

**Why `msg` over `cancelling()`**: The `msg` parameter works identically across 3.10-3.14. Using `cancelling()` would require version checks or graceful degradation on 3.10.

**Why we don't need `uncancel()`**: You need `uncancel()` when you suppress a CancelledError but want to **continue running** with structured concurrency (`asyncio.timeout()`, `TaskGroup`). Without it, the cancellation count stays elevated and those primitives get confused. In our cases, when we suppress (via sentinel message), we're at terminal points - converting to TimeoutError or finishing cleanup. We're not continuing with more structured concurrency afterward. External cancellations always propagate, preserving the count for callers.

**Don't swallow CancelledError**: The official guidance warns that `asyncio.TaskGroup` and `asyncio.timeout()` use cancellation internally and misbehave if CancelledError is swallowed. Our fix removes the patterns that were swallowing it.

### Helper Function

```python
def _is_our_cancellation(exc: asyncio.CancelledError, expected_msg: str) -> bool:
    """Check if we initiated this cancellation (vs. someone cancelling us)."""
    return bool(exc.args and exc.args[0] == expected_msg)
```

## Changes Required

### 1. Add `taskgroup` Dependency

Add to `pyproject.toml` for Python 3.10 compatibility:

```toml
dependencies = [
    ...
    "taskgroup>=0.2.2; python_version < '3.11'",
]
```

And create a compatibility import in `worker.py`:

```python
import sys
if sys.version_info >= (3, 11):
    from asyncio import TaskGroup
else:
    from taskgroup import TaskGroup
```

### 2. Refactor Worker Internal Tasks to Use TaskGroup

**Current approach** (manual task management):
```python
# In _worker_loop - multiple tasks with different shutdown patterns
cancellation_listener_task = asyncio.create_task(...)
scheduler_task = asyncio.create_task(...)
lease_renewal_task = asyncio.create_task(...)

# Manual cleanup with events and cancel/suppress
finally:
    self._worker_stopping.set()
    await scheduler_task  # checks event
    await lease_renewal_task  # checks event
    cancellation_listener_task.cancel()  # needs explicit cancel
    with suppress(asyncio.CancelledError):
        await cancellation_listener_task
```

**New approach** (TaskGroup):
```python
async def _worker_loop(self, redis: Redis, forever: bool = False):
    async with TaskGroup() as infra:
        infra.create_task(self._cancellation_listener())
        infra.create_task(self._scheduler_loop(redis))
        infra.create_task(self._renew_leases(redis, active_tasks))

        try:
            # Main work loop
            while (forever or has_work or active_tasks) and not self._worker_stopping.is_set():
                await process_completed_tasks()
                # ... rest of work loop ...
        except asyncio.CancelledError:
            # Drain user tasks gracefully before TaskGroup cancels infra
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)

    # TaskGroup automatically cancels and awaits all infra tasks
    self._worker_done.set()
```

**Benefits**:
- No manual cancel/await/suppress patterns for internal tasks
- TaskGroup handles cancellation propagation correctly (uses `cancelling()`/`uncancel()` internally)
- Internal task failures become ExceptionGroup instead of silent failures
- Removes need for `_worker_stopping` checks in scheduler/lease_renewal loops - they just get cancelled

**Internal loops simplified**:
```python
# Before: check event in loop
async def _scheduler_loop(self, redis: Redis) -> None:
    while not self._worker_stopping.is_set():
        # ... do work ...

# After: just loop, TaskGroup cancels us
async def _scheduler_loop(self, redis: Redis) -> None:
    while True:
        # ... do work ...
```

### 3. Add Constants and Helper Function for Timeout Handling

The sentinel message pattern is still needed for `_run_function_with_timeout` (not inside TaskGroup).

Add to `worker.py`:

```python
# Sentinel messages for internal cancellation - human-readable and identifiable
# These allow distinguishing "we cancelled this" from "someone cancelled us"
CANCEL_MSG_TIMEOUT = "Task exceeded its timeout"
CANCEL_MSG_CLEANUP = "Worker shutting down"

def _is_our_cancellation(exc: asyncio.CancelledError, expected_msg: str) -> bool:
    """Check if we initiated this cancellation (vs. someone cancelling us)."""
    return bool(exc.args and exc.args[0] == expected_msg)
```

### 4. Fix `_run_function_with_timeout` (worker.py:966-1006)

**Current bug**: All CancelledError converted to TimeoutError

**Fix**: Use sentinel message when we cancel for timeout:

```python
async def _run_function_with_timeout(self, execution, dependencies, timeout):
    task = asyncio.create_task(...)

    try:
        while not task.done():
            if timeout.expired():
                task.cancel(CANCEL_MSG_TIMEOUT)  # <-- sentinel message
                break
            try:
                return await asyncio.wait_for(asyncio.shield(task), timeout=remaining)
            except asyncio.TimeoutError:
                continue
    finally:
        if not task.done():
            task.cancel(CANCEL_MSG_TIMEOUT)  # <-- sentinel message

    try:
        return await task
    except asyncio.CancelledError as e:
        if _is_our_cancellation(e, CANCEL_MSG_TIMEOUT):
            raise asyncio.TimeoutError
        # External cancellation - propagate it
        raise
```

### 5. Remove Unnecessary Shields from Cleanup Code

Remove `asyncio.shield()` from cleanup operations. These are quick, atomic operations that don't need protection. If cleanup is interrupted, the process is exiting anyway.

**Files to update**:
- `src/docket/_redis.py` (lines 107, 114, 121, 319, 323)
- `src/docket/docket.py` (lines 212, 225)
- `src/docket/strikelist.py` (line 264)
- `src/docket/_result_store.py` (lines 227, 234)

**New pattern** (let CancelledError propagate):

```python
# Before (wrong):
try:
    await asyncio.shield(pool.aclose())
except (Exception, asyncio.CancelledError):
    logger.warning("Failed to close...", exc_info=True)

# After (correct):
try:
    await pool.aclose()
except Exception:
    logger.warning("Failed to close...", exc_info=True)
# CancelledError propagates naturally
```

### 6. Fix Remaining Task Cancellation Patterns

With TaskGroup handling worker internal tasks, fewer places need manual cancel/await. Remaining spots:

**File**: `src/docket/worker.py`
- Line 225-227 (heartbeat task in `__aexit__`) - still needs sentinel pattern OR move heartbeat into TaskGroup

**File**: `src/docket/strikelist.py`
- Line 253-257 (monitor task cleanup) - use sentinel pattern

**File**: `src/docket/dependencies/_concurrency.py`
- Line 164-166 (renewal task cleanup) - use sentinel pattern

**Pattern for remaining spots**:

```python
# Before (wrong):
self._task.cancel()
with suppress(asyncio.CancelledError):
    await self._task

# After (correct):
self._task.cancel(CANCEL_MSG_CLEANUP)
try:
    await self._task
except asyncio.CancelledError as e:
    if not _is_our_cancellation(e, CANCEL_MSG_CLEANUP):
        raise  # External cancellation, propagate it
```

### 7. Fix `_execute` CancelledError Handler (worker.py:918-925)

This handler correctly marks tasks as cancelled when they receive CancelledError. No changes needed here - the fix is in `_run_function_with_timeout` which will now propagate external cancellation correctly instead of converting to TimeoutError.

## Files to Modify

| File | Changes |
|------|---------|
| `pyproject.toml` | Add `taskgroup` dependency for Python <3.11 |
| `src/docket/worker.py` | Add TaskGroup import, refactor `_worker_loop` to use TaskGroup, add sentinel constants + `_is_our_cancellation()`, fix `_run_function_with_timeout`, simplify internal loop functions |
| `src/docket/_redis.py` | Remove shields, let CancelledError propagate |
| `src/docket/docket.py` | Remove shields from `__aexit__` |
| `src/docket/strikelist.py` | Remove shield, fix monitor task cleanup with sentinel pattern |
| `src/docket/_result_store.py` | Remove shields from cleanup |
| `src/docket/dependencies/_concurrency.py` | Fix renewal task cleanup with sentinel pattern |

## Test Plan

### New Tests to Write (in `tests/test_cancellation.py`)

1. **Test external cancellation works with timeout tasks** (this is the gap - no existing test combines `docket.cancel()` with a `Timeout` dependency):
   ```python
   async def test_cancel_running_task_with_timeout(docket, worker):
       """A running task with Timeout can be cancelled via docket.cancel().

       This specifically tests the bug where _run_function_with_timeout was
       converting ALL CancelledError to TimeoutError, breaking external cancellation.
       """
       started = asyncio.Event()

       async def slow_task_with_timeout(timeout: Timeout = Timeout(timedelta(seconds=60))):
           started.set()
           await asyncio.sleep(60)

       docket.register(slow_task_with_timeout)
       execution = await docket.add(slow_task_with_timeout)()

       worker_task = asyncio.create_task(worker.run_until_finished())
       await started.wait()

       await docket.cancel(execution.key)
       await worker_task

       await execution.sync()
       assert execution.state == ExecutionState.CANCELLED  # NOT FAILED
   ```

2. **Test timeout still works after fix**:
   - Existing tests in `test_timeouts.py` should continue to pass

3. **Test clean shutdown propagates cancellation**:
   ```python
   async def test_worker_shutdown_propagates_cancellation(docket):
       """Worker cancellation during task execution propagates correctly."""
       task_started = asyncio.Event()
       task_cancelled = asyncio.Event()

       async def long_task():
           task_started.set()
           try:
               await asyncio.sleep(60)
           except asyncio.CancelledError:
               task_cancelled.set()
               raise

       docket.register(long_task)
       await docket.add(long_task)()

       async with Worker(docket) as worker:
           worker_task = asyncio.create_task(worker.run_forever())
           await asyncio.wait_for(task_started.wait(), timeout=5.0)
           worker_task.cancel()
           with suppress(asyncio.CancelledError):
               await worker_task

       # Task should have received cancellation
       assert task_cancelled.is_set()
   ```

### Existing Tests to Verify Pass

```bash
pytest tests/test_cancellation.py tests/fundamentals/test_cancellation.py tests/fundamentals/test_timeouts.py tests/test_worker_lifecycle.py -v
```

## Implementation Order

1. Add `taskgroup` dependency to `pyproject.toml`
2. Write failing test for the `docket.cancel()` + timeout bug
3. Add TaskGroup import and sentinel constants + `_is_our_cancellation()` helper
4. Refactor `_worker_loop` to use TaskGroup for internal tasks
5. Simplify internal loop functions (remove `_worker_stopping` checks)
6. Fix `_run_function_with_timeout` to use sentinel message pattern
7. Run new test - verify it passes
8. Update remaining cleanup patterns (remove shields, use sentinel pattern where needed)
9. Run full test suite to ensure no regressions

## Verification

```bash
# Run specific tests
pytest tests/test_cancellation.py tests/fundamentals/test_cancellation.py tests/fundamentals/test_timeouts.py tests/test_worker_lifecycle.py -v

# Run full test suite
pytest

# Run with coverage to ensure 100%
pytest --cov=src/docket --cov-report=term-missing
```

## References

- [Python asyncio Task documentation](https://docs.python.org/3/library/asyncio-task.html) - Official docs on `cancel(msg=)`, `cancelling()`, `uncancel()`
- [Asyncio Task Cancellation Best Practices](https://superfastpython.com/asyncio-task-cancellation-best-practices/) - Comprehensive guide
- [Safe synchronous cancellation in asyncio (CPython issue)](https://github.com/python/cpython/issues/103486) - Discussion of cancellation patterns
- [Tracking the source of cancellation in tasks](https://discuss.python.org/t/tracking-the-source-of-cancellation-in-tasks/18352) - Python forum discussion
- [PEP 789 - Preventing task-cancellation bugs](https://peps.python.org/pep-0789/) - Future direction for structured concurrency
