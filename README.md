Docket is a distributed background task system for Python functions with a focus
on the scheduling of future work as seamlessly and efficiency as immediate work.

[![PyPI - Version](https://img.shields.io/pypi/v/pydocket)](https://pypi.org/project/pydocket/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pydocket)](https://pypi.org/project/pydocket/)
[![GitHub main checks](https://img.shields.io/github/check-runs/chrisguidry/docket/main)](https://github.com/chrisguidry/docket/actions/workflows/ci.yml)
[![Codecov](https://img.shields.io/codecov/c/github/chrisguidry/docket)](https://app.codecov.io/gh/chrisguidry/docket)
[![PyPI - License](https://img.shields.io/pypi/l/pydocket)](https://github.com/chrisguidry/docket/blob/main/LICENSE)

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
        await worker.run_until_finished()
```

```
Hello, Jane at 2025-03-05 13:58:21.552644!
Howdy, John at 2025-03-05 13:58:24.550773!
```

## Why `docket`?

⚡️ Snappy one-way background task processing without any bloat

📅 Schedule immediate or future work seamlessly with the same interface

⏭️ Skip problematic tasks or parameters without redeploying

🌊 Purpose-built for Redis streams

🧩 Fully type-complete and type-aware for your background task functions


## Installing `docket`

Docket is [available on PyPI](https://pypi.org/project/pydocket/) under the package name
`pydocket`.  It targets Python 3.12 or above.

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
introduced in Redis 5.0.0).  Docket is tested with Redis 7.


## Creating a `Docket`

Each `Docket` should have a name that will be shared across your system, like the name
of a topic or queue.  By default this is `"docket"`.  You can support many separate
dockets on a single Redis server as long as they have different names.

Docket accepts a URL to connect to the Redis server (defaulting to the local
server), and you can pass any additional connection configuration you need on that
connection URL.

```python
async with Docket(name="orders", url="redis://my-redis:6379/0") as docket:
    ...
```

The `name` and `url` together represent a single shared docket of work across all your
system.


## Scheduling work

A `Docket` is the entrypoint to scheduling immediate and future work.  You define work
in the form of `async` functions that return `None`.  These task functions can accept
any parameter types, so long as they can be serialized with
[`cloudpickle`](https://github.com/cloudpipe/cloudpickle).

```python
def now() -> datetime:
    return datetime.now(timezone.utc)

async def send_welcome_email(customer_id: int, name: str) -> None:
    ...

async def send_followup_email(customer_id: int, name: str) -> None:
    ...

async with Docket() as docket:
    await docket.add(send_welcome_email)(12345, "Jane Smith")

    tomorrow = now() + timedelta(days=1)
    await docket.add(send_followup_email, when=tomorrow)(12345, "Jane Smith")
```

`docket.add` schedules both immediate work (the default) or future work (with the
`when: datetime` parameter).

All task executions are identified with a `key` that captures the unique essence of that
piece of work.  By default they are randomly assigned UUIDs, but assigning your own keys
unlocks many powerful capabilities.

```python
async with Docket() as docket:
    await docket.add(send_welcome_email)(12345, "Jane Smith")

    tomorrow = now() + timedelta(days=1)
    key = "welcome-email-for-12345"
    await docket.add(send_followup_email, when=tomorrow, key=key)(12345, "Jane Smith")
```

If you've given your future work a `key`, then only one unique instance of that
execution will exist in the future:

```python
key = "welcome-email-for-12345"
await docket.add(send_followup_email, when=tomorrow, key=key)(12345, "Jane Smith")
```

Calling `.add` a second time with the same key won't do anything, so luckily your
customer won't get two emails!

However, at any time later you can replace that task execution to alter _when_ it will
happen:

```python
key = "welcome-email-for-12345"
next_week = now() + timedelta(days=7)
await docket.replace(send_followup_email, when=next_week, key=key)(12345, "Jane Smith")
```

_what arguments_ will be passed:

```python
key = "welcome-email-for-12345"
await docket.replace(send_followup_email, when=tomorrow, key=key)(12345, "Jane Q. Smith")
```

Or just cancel it outright:

```python
await docket.cancel("welcome-email-for-12345")
```

Tasks may also be called by name, in cases where you can't or don't want to import the
module that has your tasks.  This may be common in a distributed environment where the
code of your task system just isn't available, or it requires heavyweight libraries that
you wouldn't want to import into your web server.  In this case, you will lose the
type-checking for `.add` and `.replace` calls, but otherwise everything will work as
it does with the actual function:

```python
await docket.add("send_followup_email", when=tomorrow)(12345, "Jane Smith")
```

These primitives of `.add`, `.replace`, and `.cancel` are sufficient to build a
large-scale and robust system of background tasks for your application.

## Writing tasks

Tasks are any `async` function that takes `cloudpickle`-able parameters, and returns
`None`.  Returning `None` is a strong signal that these are _fire-and-forget_ tasks
whose results aren't used or waited-on by your application.  These are the only kinds of
tasks that Docket supports.

Docket uses a parameter-based dependency and configuration pattern, which has become
common in frameworks like [FastAPI](https://fastapi.tiangolo.com/),
[Typer](https://typer.tiangolo.com/), or [FastMCP](https://github.com/jlowin/fastmcp).
As such, there is no decorator for tasks.

A very common requirement for tasks is that they have access to schedule further work
on their own docket, especially for chains of self-perpetuating tasks to implement
distributed polling and other periodic systems.  One of the first dependencies you may
look for is the `CurrentDocket`:

```python
from docket import Docket, CurrentDocket

POLLING_INTERVAL = timedelta(seconds=10)

async def poll_for_changes(file: Path, docket: Docket = CurrentDocket()) -> None:
    if file.exists():
        ...do something interesting...
        return
    else:
        await docket.add(poll_for_changes, when=now() + POLLING_INTERVAL)(file)
```

Here the argument to `docket` is an instance of `Docket` with the same name and URL as
the worker it's running on.  You can ask for the `CurrentWorker` and `CurrentExecution`
as well.  Many times it could be useful to have your own task `key` available in order
to idempotently schedule future work:

```python
from docket import Docket, CurrentDocket, TaskKey

async def poll_for_changes(
    file: Path,
    key: str = TaskKey(),
    docket: Docket = CurrentDocket()
) -> None:
    if file.exists():
        ...do something interesting...
        return
    else:
        await docket.add(poll_for_changes, when=now() + POLLING_INTERVAL, key=key)(file)
```

This helps to ensure that there is one continuous "chain" of these future tasks, as they
all use the same key.

Configuring the retry behavior for a task is also done with a dependency:

```python
from datetime import timedelta
from docket import Retry

async def faily(retry: Retry = Retry(attempts=5, delay=timedelta(seconds=3))):
    if retry.attempt == 4:
        print("whew!")
        return

    raise ValueError("whoops!")
```

In this case, the task `faily` will run 4 times with a delay of 3 seconds between each
attempt.  If it were to get to 5 attempts, no more would be attempted.  This is a
linear retry, and an `ExponentialRetry` is also available:

```python
from datetime import timedelta
from docket import Retry, ExponentialRetry


async def faily(
    retry: Retry = Retry(
        attempts=5,
        minimum_delay=timedelta(seconds=2),
        maximum_delay=timedelta(seconds=32),
    ),
):
    if retry.attempt == 4:
        print("whew!")
        return

    raise ValueError("whoops!")
```

This would retry in 2, 4, 8, then 16 seconds before that fourth attempt succeeded.


## Running workers

You can run as many workers as you like to process the tasks on your docket.  You can
either run a worker programmatically in Python, or via the CLI.  Clients using docket
have the advantage that they are usually passing the task functions, but workers don't
necessarily know which tasks they are supposed to run.  Docket solves this by allowing
you to explicitly register tasks.

In `my_tasks.py`:

```python
async def my_first_task():
    ...

async def my_second_task():
    ...

my_task_collection = [
    my_first_task,
    my_second_task,
]
```

From Python:

```python
from my_tasks import my_task_collection

async with Docket() as docket:
    for task in my_task_collection:
        docket.register(task)

    async with Worker(docket) as worker:
        await worker.run_forever()
```

From the CLI:

```bash
docket worker --tasks my_tasks:my_task_collection
```

By default, workers will process up to 10 tasks concurrently, but you can adjust this
to your needs with the `concurrency=` keyword argument or the `--concurrency` CLI
option.

When a worker crashes ungracefully, any tasks it was currently executing will be held
for a period of time before being redelivered to other workers.  You can control this
time period with `redelivery_timeout=` or `--redelivery-timeout`.  You'd want to set
this to a value higher than the longest task you expect to run.  For queues of very fast
tasks, a few seconds may be ideal; for long data-processing steps involving large
amount of data, you may need minutes.


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

We aim to main 100% test coverage, which is required for all PRs to `docket`.  We
believe that `docket` should stay small, simple, understandable, and reliable, and that
begins with testing all the dusty branches and corners.  This will give us the
confidence to upgrade dependencies quickly and to adapt to new versions of Redis over
time.
