# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Docket** (`pydocket` on PyPI) is a distributed background task system for Python functions with Redis-backed persistence. It enables scheduling both immediate and future work with comprehensive dependency injection, retry mechanisms, and fault tolerance.

**Key Requirements**: Python 3.10+, Redis 6.2+ or Valkey 8.0+

## Development Commands

### Testing

```bash
# Run full test suite with coverage and parallel execution
pytest

# Run specific test
pytest tests/test_docket.py::test_specific_function
```

The project REQUIRES 100% test coverage

### Docker Test Runner (for reproducing CI timing issues)

When debugging flaky CI tests that are hard to reproduce locally, use the Docker test
runner with CPU limits. CI failures often stem from timing issues that only manifest
under CPU contention - throttling to 0.2 CPU can reproduce these locally.

Use docker compose to run tests with CPU throttling, simulating slow CI environments:

```bash
# Run specific tests with CPU limit (default 0.5 = half a core)
docker compose run --rm pytest tests/path/to/test.py -v

# Throttling to reproduce timing bugs (0.2 is the sweet spot)
CPU_LIMIT=0.2 docker compose run --rm pytest tests/concurrency_limits/ -v --timeout=120

# Different Python version
PYTHON_VERSION=3.12 docker compose build
PYTHON_VERSION=3.12 docker compose run --rm pytest tests/path -v
```

Environment variables:
- `PYTHON_VERSION`: Python version (default: 3.10)
- `CPU_LIMIT`: CPU cores fraction (default: 0.5)
- `MEM_LIMIT`: Memory limit (default: 512M)
- `REDIS_VERSION`: `memory` or version like `6.2`, `8.0`, `valkey-8.0` (default: memory)

```bash
# With real Redis
REDIS_VERSION=6.2 docker compose run --rm pytest tests/path -v
```

### Code Quality

```bash
# Run all checks (linting, formatting, type checking, file size limits)
uv run prek run --all-files
```

This runs ruff, pyright, loq, and other checks in one command.

**File size limits**: This project uses [loq](https://github.com/jakekaplan/loq) to enforce file size limits. The default limit is 500 lines. Existing large files have baselines in `loq.toml`. If you need to exceed a limit, run `loq baseline` to update the configuration.

### Development Setup

```bash
# Install development dependencies
uv sync --group dev

# Install prek hooks
uv run prek install
```

### Git Workflow

- This project uses Github for issue tracking
- This project can use git worktrees under .worktrees/

## Core Architecture

### Key Classes

- **`Docket`** (`src/docket/docket.py`): Central task registry and scheduler
  - `add()`: Schedule tasks for execution
  - `replace()`: Replace existing scheduled tasks
  - `cancel()`: Cancel pending tasks
  - `strike()`/`restore()`: Conditionally block/unblock tasks
  - `snapshot()`: Get current state for observability

- **`Worker`** (`src/docket/worker.py`): Task execution engine
  - `run_forever()`/`run_until_finished()`: Main execution loops
  - Handles concurrency, retries, and dependency injection
  - Maintains heartbeat for liveness tracking

- **`Execution`** (`src/docket/execution.py`): Task execution context with metadata

### Dependencies System (`src/docket/dependencies.py`)

Rich dependency injection supporting:

- Context access: `CurrentDocket`, `CurrentWorker`, `CurrentExecution`
- Retry strategies: `Retry`, `ExponentialRetry`
- Special behaviors: `Perpetual` (self-rescheduling), `Timeout`
- Custom injection: `Depends()`
- Contextual logging: `TaskLogger`

### Redis Data Model

- **Streams**: `{docket}:stream` (ready tasks), `{docket}:strikes` (commands)
- **Sorted Sets**: `{docket}:queue` (scheduled tasks), `{docket}:workers` (heartbeats)
- **Hashes**: `{docket}:{key}` (parked task data)
- **Sets**: `{docket}:worker-tasks:{worker}` (worker capabilities)

### Task Lifecycle

1. Registration with `Docket.register()` or `@docket.task`
2. Scheduling: immediate → Redis stream, future → Redis sorted set
3. Worker processing: scheduler moves due tasks, workers consume via consumer groups
4. Execution: dependency injection, retry logic, acknowledgment

## Project Structure

### Source Code

- `src/docket/` - Main package
  - `__init__.py` - Public API exports
  - `docket.py` - Core Docket class
  - `worker.py` - Worker implementation
  - `execution.py` - Task execution context
  - `dependencies.py` - Dependency injection system
  - `tasks.py` - Built-in utility tasks
  - `cli.py` - Command-line interface

### Testing and Examples

- `tests/` - Comprehensive test suite
- `examples/` - Usage examples
- `chaos/` - Chaos testing framework

## CLI Usage

```bash
# Run a worker
docket worker --url redis://localhost:6379/0 --tasks your.module --concurrency 4

# See all commands
docket --help
```
