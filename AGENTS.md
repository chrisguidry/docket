# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Docket** (`pydocket` on PyPI) is a distributed background task system for Python functions with Redis-backed persistence. It enables scheduling both immediate and future work with comprehensive dependency injection, retry mechanisms, and fault tolerance.

**Key Requirements**: Python 3.10+, Redis 6.2+ or Valkey 8.0+

The repository is a uv workspace.  The Python package lives in `python/`,
and the tools that work across the repository (prek, loq, codespell, the
docs, chaos, examples) live at the root.  Run `uv sync` once at the root;
it installs everything.  Do not run `uv sync` inside `python/`, because it
syncs only that member and removes the root tools from the shared `.venv`.
`uv run` from any directory is safe.

## Development Commands

### Testing

Run the tests from `python/`:

```bash
cd python

# Run full test suite with coverage and parallel execution
uv run pytest

# Run specific test
uv run pytest tests/fundamentals/test_scheduling.py::test_immediate_task_execution
```

The project REQUIRES 100% test coverage

### Docker Test Runner (for reproducing CI timing issues)

When debugging flaky CI tests that are hard to reproduce locally, use the Docker test
runner with CPU limits. CI failures often stem from timing issues that only manifest
under CPU contention - throttling to 0.2 CPU can reproduce these locally.

Use docker compose from the repository root to run tests with CPU throttling, simulating slow CI environments:

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
- `REDIS_VERSION`: `memory` or version like `6.2`, `8.10`, `valkey-9.1` (default: memory)

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
# Install development dependencies, from the repository root
uv sync

# Install prek hooks
uv run prek install
```

### Git Workflow

- This project uses Github for issue tracking
- This project can use git worktrees under .worktrees/

### Keeping docs honest as you edit

While editing `python/src/docket/`, `docs/*.md`, or `README.md`, follow the
**`audit-docs`** skill at `.claude/skills/audit-docs/SKILL.md`. It is a
*habit*, not a deliverable: when you change a function body, signature,
Lua script, or any public contract, re-read the affected docstring and
any narrative-docs section that mentions the API, and fix any drift in
the same edit. The aim is to never let stale docstrings or wrong doc
prose ride along to a PR. Only switch into the skill's standalone
"report" mode if the user explicitly asks for an audit pass.

## Core Architecture

### Key Classes

- **`Docket`** (`python/src/docket/docket.py`): Central task registry and scheduler
  - `add()`: Schedule tasks for execution
  - `replace()`: Replace existing scheduled tasks
  - `call()` + `add_many()`/`replace_many()`: Batch scheduling in one pipelined round-trip
  - `cancel()`: Cancel pending tasks
  - `strike()`/`restore()`: Conditionally block/unblock tasks
  - `snapshot()`: Get current state for observability

- **`Worker`** (`python/src/docket/worker.py`): Task execution engine
  - `run_forever()`/`run_until_finished()`: Main execution loops
  - Handles concurrency, retries, and dependency injection
  - Maintains heartbeat for liveness tracking

- **`Execution`** (`python/src/docket/execution.py`): Task execution context with metadata

### Dependencies System (`python/src/docket/dependencies/`)

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
- **Strings**: `{docket}:leases:redelivery-sweep` (fleet-wide sweep lease, expiring)

### Task Lifecycle

1. Registration with `Docket.register()`, or implicitly when `add()`, `replace()`, or `call()` receives a function
2. Scheduling: immediate → Redis stream, future → Redis sorted set
3. Worker processing: scheduler moves due tasks, workers consume via consumer groups
4. Execution: dependency injection, retry logic, acknowledgment

## Project Structure

### Source Code

- `python/src/docket/` - Main package
  - `__init__.py` - Public API exports
  - `docket.py` - Core Docket class
  - `worker.py` - Worker implementation
  - `execution.py` - Task execution context
  - `dependencies/` - Dependency injection system
  - `tasks.py` - Built-in utility tasks
  - `cli/` - Command-line interface

### Testing and Examples

- `python/tests/` - Comprehensive test suite
- `examples/` - Usage examples
- `chaos/` - Chaos testing framework

## CLI Usage

```bash
# Run a worker
docket worker --url redis://localhost:6379/0 --tasks your.module --concurrency 4

# See all commands
docket --help
```
