# Task Memory

**Created:** 2025-07-10 22:23:35
**Branch:** feature/task-concurrency-control

## Requirements

# Task concurrency control

**Labels:** enhancement, devx

**Issue URL:** https://github.com/chrisguidry/docket/issues/86

## Description

A very common requirement of background task systems is to govern concurrent access based on aspects of the data, like "only run one task per unique `customer_id` at the same time" or "only run `n` tasks per database name at a time".  This should be separate from the `key` of a task and should be expressed in more granular dependencies/parameter annotations.


## Development Notes

### Work Log

- [2025-07-10 22:23:35] Task setup completed, TASK_MEMORY.md created
- [2025-07-11] Task concurrency control implementation completed

### Implementation Summary

**Core Changes:**
1. **New ConcurrencyLimit dependency** (`src/docket/dependencies.py`):
   - Added `ConcurrencyLimit` class that allows limiting concurrent tasks based on argument values
   - API: `ConcurrencyLimit("argument_name", max_concurrent=N, scope="optional")`
   - Integrates with existing dependency injection system

2. **Worker concurrency management** (`src/docket/worker.py`):
   - Added `_can_start_task()` method that checks Redis-based concurrency limits using Lua scripts
   - Added `_release_concurrency_slot()` method that cleans up concurrency tracking when tasks complete
   - Modified task dispatch loop to respect concurrency limits and requeue blocked tasks

3. **Redis-based coordination**:
   - Uses Redis sets to track active tasks per argument value
   - Atomic operations via Lua scripts ensure thread-safety across multiple workers
   - Key format: `{scope}:concurrency:{argument_name}:{argument_value}`

**Key Decisions:**
- **Separate from task keys**: Concurrency control is independent of task deduplication keys
- **Redis-based coordination**: Enables multi-worker concurrency limits
- **Automatic rescheduling**: Blocked tasks are automatically rescheduled with small delays
- **Atomic operations**: Lua scripts ensure race-condition-free concurrency checking

**API Examples:**
```python
# Limit to 1 concurrent task per customer_id
@task
async def process_customer(
    customer_id: int,
    concurrency: ConcurrencyLimit = ConcurrencyLimit("customer_id", max_concurrent=1)
):
    ...

# Limit to 3 concurrent backups per database
@task  
async def backup_database(
    db_name: str,
    concurrency: ConcurrencyLimit = ConcurrencyLimit("db_name", max_concurrent=3)
):
    ...
```

**Files Modified:**
- `src/docket/dependencies.py` - Added ConcurrencyLimit class
- `src/docket/__init__.py` - Exported ConcurrencyLimit 
- `src/docket/worker.py` - Added concurrency management logic
- `tests/conftest.py` - Fixed test fixture issues
- `tests/test_concurrency_control.py` - Comprehensive test suite
- `tests/test_concurrency_basic.py` - Basic functionality test
- `examples/concurrency_control.py` - Working example demonstrating the feature

**Testing:**
- All existing tests continue to pass
- New comprehensive test suite covers various concurrency scenarios
- Working example demonstrates real-world usage patterns
- Verified concurrency limits work correctly across multiple workers

**Challenges Encountered:**
1. **Redis eval() API**: Had to learn proper parameter passing format for Lua scripts
2. **Task rescheduling**: Initial approach of not acknowledging blocked tasks caused infinite loops
3. **Type checking**: Redis client types needed explicit type ignores for eval() calls
4. **Test fixtures**: Async fixture configuration required adjustment

**Solutions Implemented:**
1. **Proper Lua script integration**: Used Redis eval() with correct parameter format
2. **Smart rescheduling**: Blocked tasks are rescheduled with new keys and small delays
3. **Type safety**: Added appropriate type annotations and ignores
4. **Robust testing**: Created comprehensive test suite covering edge cases

---

*This file serves as your working memory for this task. Keep it updated as you progress through the implementation.*
