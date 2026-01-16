Docket is a distributed background task system for Python functions with a focus
on the scheduling of future work as seamlessly and efficiently as immediate work.

[![PyPI - Version](https://img.shields.io/pypi/v/pydocket)](https://pypi.org/project/pydocket/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pydocket)](https://pypi.org/project/pydocket/)
[![GitHub main checks](https://img.shields.io/github/check-runs/chrisguidry/docket/main)](https://github.com/chrisguidry/docket/actions/workflows/ci.yml)
[![Codecov](https://img.shields.io/codecov/c/github/chrisguidry/docket)](https://app.codecov.io/gh/chrisguidry/docket)
[![PyPI - License](https://img.shields.io/pypi/l/pydocket)](https://github.com/chrisguidry/docket/blob/main/LICENSE)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://chrisguidry.github.io/docket/)

## At a glance

```python
from datetime import datetime, timedelta, timezone

from docket import Docket


async def greet(name: str, greeting="Hello") -> None:
    print(f"{greeting}, {name} at {datetime.now()}!")


async with Docket() as docket:
    await docket.add(greet)("Jane")

    now = datetime.now(timezone.utc)
    soon = now + timedelta(seconds=3)
    await docket.add(greet, when=soon)("John", greeting="Howdy")
```

```python
from docket import Docket, Worker

async with Docket() as docket:
    async with Worker(docket) as worker:
        worker.register(greet)
        await worker.run_until_finished()
```

```
Hello, Jane at 2025-03-05 13:58:21.552644!
Howdy, John at 2025-03-05 13:58:24.550773!
```

Check out our docs for more [details](http://chrisguidry.github.io/docket/),
[examples](https://chrisguidry.github.io/docket/getting-started/), and the [API
reference](https://chrisguidry.github.io/docket/api-reference/).

## Why `docket`?

⚡️ Snappy one-way background task processing without any bloat

📅 Schedule immediate or future work seamlessly with the same interface

⏭️ Skip problematic tasks or parameters without redeploying

🌊 Purpose-built for Redis streams

🧩 Fully type-complete and type-aware for your background task functions

💉 Dependency injection like FastAPI, Typer, and FastMCP for reusable resources

## Installing `docket`

Docket is [available on PyPI](https://pypi.org/project/pydocket/) under the package name
`pydocket`. It targets Python 3.10 or above.

With [`uv`](https://docs.astral.sh/uv/):

```bash
uv pip install pydocket

or

uv add pydocket
```

With `pip`:

```bash
pip install pydocket
```

Docket requires a [Redis](http://redis.io/) server with Streams support (which was
introduced in Redis 5.0.0). Docket is tested with:

- Redis 6.2, 7.4, and 8.0 (standalone and cluster modes)
- [Valkey](https://valkey.io/) 8.0
- In-memory backend via [fakeredis](https://github.com/cunla/fakeredis-py) for testing

For testing without Redis, use the in-memory backend:

```python
from docket import Docket

async with Docket(name="my-docket", url="memory://my-docket") as docket:
    # Use docket normally - all operations are in-memory
    ...
```

See [Testing with Docket](https://chrisguidry.github.io/docket/testing/#using-in-memory-backend-no-redis-required) for more details.

# Hacking on `docket`

We use [`uv`](https://docs.astral.sh/uv/) for project management, so getting set up
should be as simple as cloning the repo and running:

```bash
uv sync
```

The to run the test suite:

```bash
pytest
```

We aim to maintain 100% test coverage, which is required for all PRs to `docket`. We
believe that `docket` should stay small, simple, understandable, and reliable, and that
begins with testing all the dusty branches and corners. This will give us the
confidence to upgrade dependencies quickly and to adapt to new versions of Redis over
time.
