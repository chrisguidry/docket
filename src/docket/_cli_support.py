"""Support functions for the docket CLI."""

import enum
import importlib
import logging
import os
import socket
import sys
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Collection

import typer
from rich.console import Console
from rich.table import Table

from .docket import DocketSnapshot, WorkerInfo


async def iterate_with_timeout(
    iterator: AsyncIterator[dict[str, Any]], timeout: float
) -> AsyncGenerator[dict[str, Any] | None, None]:
    """Iterate over an async iterator with timeout, ensuring proper cleanup.

    Wraps an async iterator to add timeout support and guaranteed cleanup.
    On timeout, yields None to allow the caller to handle polling fallback.

    Args:
        iterator: An async generator (must have __anext__ and aclose methods)
        timeout: Timeout in seconds for each iteration

    Yields:
        Items from the iterator, or None if timeout expires
    """
    import asyncio

    try:
        while True:
            try:
                yield await asyncio.wait_for(iterator.__anext__(), timeout=timeout)
            except asyncio.TimeoutError:
                # Yield None to signal timeout, allowing caller to handle polling
                yield None
            except StopAsyncIteration:
                break
    finally:
        await iterator.aclose()


class LogLevel(str, enum.Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogFormat(str, enum.Enum):
    RICH = "rich"
    PLAIN = "plain"
    JSON = "json"


def local_time(when: datetime) -> str:
    return when.astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


def default_worker_name() -> str:
    return f"{socket.gethostname()}#{os.getpid()}"


def duration(duration_str: str | timedelta) -> timedelta:
    """
    Parse a duration string into a timedelta.

    Supported formats:
    - 123 = 123 seconds
    - 123s = 123 seconds
    - 123m = 123 minutes
    - 123h = 123 hours
    - 00:00 = mm:ss
    - 00:00:00 = hh:mm:ss
    """
    if isinstance(duration_str, timedelta):
        return duration_str

    if ":" in duration_str:
        parts = duration_str.split(":")
        if len(parts) == 2:  # mm:ss
            minutes, seconds = map(int, parts)
            return timedelta(minutes=minutes, seconds=seconds)
        elif len(parts) == 3:  # hh:mm:ss
            hours, minutes, seconds = map(int, parts)
            return timedelta(hours=hours, minutes=minutes, seconds=seconds)
        else:
            raise ValueError(f"Invalid duration string: {duration_str}")
    elif duration_str.endswith("s"):
        return timedelta(seconds=int(duration_str[:-1]))
    elif duration_str.endswith("m"):
        return timedelta(minutes=int(duration_str[:-1]))
    elif duration_str.endswith("h"):
        return timedelta(hours=int(duration_str[:-1]))
    else:
        return timedelta(seconds=int(duration_str))


def set_logging_format(format: LogFormat) -> None:
    root_logger = logging.getLogger()
    if format == LogFormat.JSON:
        from pythonjsonlogger.json import JsonFormatter

        formatter = JsonFormatter(
            "{name}{asctime}{levelname}{message}{exc_info}", style="{"
        )
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
    elif format == LogFormat.PLAIN:
        handler = logging.StreamHandler(stream=sys.stdout)
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
    else:
        from rich.logging import RichHandler

        handler = RichHandler()
        formatter = logging.Formatter("%(message)s", datefmt="[%X]")
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)


def set_logging_level(level: LogLevel) -> None:
    logging.getLogger().setLevel(level.value)


def validate_url(url: str) -> str:
    """
    Validate that the provided URL is compatible with the CLI.

    The memory:// backend is not compatible with the CLI as it doesn't persist
    across processes.
    """
    if url.startswith("memory://"):
        raise typer.BadParameter(
            "The memory:// URL scheme is not supported by the CLI.\n"
            "The memory backend does not persist across processes.\n"
            "Please use a persistent backend like Redis or Valkey."
        )
    return url


def handle_strike_wildcard(value: str) -> str | None:
    if value in ("", "*"):
        return None
    return value


def interpret_python_value(value: str | None) -> Any:
    if value is None:
        return None

    type, _, value = value.rpartition(":")
    if not type:
        # without a type hint, we assume the value is a string
        return value

    module_name, _, member_name = type.rpartition(".")
    module = importlib.import_module(module_name or "builtins")
    member = getattr(module, member_name)

    # special cases for common useful types
    if member is timedelta:
        return timedelta(seconds=int(value))
    elif member is bool:
        return value.lower() == "true"
    else:
        return member(value)


def relative_time(now: datetime, when: datetime) -> str:
    delta = now - when
    if delta < -timedelta(minutes=30):
        return f"at {local_time(when)}"
    elif delta < timedelta(0):
        return f"in {-delta}"
    elif delta < timedelta(minutes=30):
        return f"{delta} ago"
    else:
        return f"at {local_time(when)}"


@dataclass
class TaskStats:
    """Statistics for a single task function."""

    running: int = 0
    queued: int = 0
    total: int = 0
    oldest_queued: datetime | None = None
    latest_queued: datetime | None = None
    oldest_started: datetime | None = None
    latest_started: datetime | None = None


def get_task_stats(snapshot: DocketSnapshot) -> dict[str, TaskStats]:
    """Get task count statistics by function name with timestamp data."""
    stats: dict[str, TaskStats] = {}

    # Count running tasks by function
    for execution in snapshot.running:
        func_name = execution.function_name
        if func_name not in stats:
            stats[func_name] = TaskStats()
        task_stats = stats[func_name]
        task_stats.running += 1
        task_stats.total += 1

        # Track oldest/latest started times for running tasks
        started = execution.started
        oldest = task_stats.oldest_started
        if oldest is None or started < oldest:
            task_stats.oldest_started = started
        latest = task_stats.latest_started
        if latest is None or started > latest:
            task_stats.latest_started = started

    # Count future tasks by function
    for execution in snapshot.future:
        func_name = execution.function_name
        if func_name not in stats:
            stats[func_name] = TaskStats()
        task_stats = stats[func_name]
        task_stats.queued += 1
        task_stats.total += 1

        # Track oldest/latest queued times for future tasks
        when = execution.when
        oldest = task_stats.oldest_queued
        if oldest is None or when < oldest:
            task_stats.oldest_queued = when
        latest = task_stats.latest_queued
        if latest is None or when > latest:
            task_stats.latest_queued = when

    return stats


def print_workers(
    docket_name: str,
    workers: Collection[WorkerInfo],
    highlight_task: str | None = None,
) -> None:
    sorted_workers = sorted(workers, key=lambda w: w.last_seen, reverse=True)

    table = Table(title=f"Workers in Docket: {docket_name}")

    table.add_column("Name", style="cyan")
    table.add_column("Last Seen", style="green")
    table.add_column("Tasks", style="yellow")

    now = datetime.now(timezone.utc)

    for worker in sorted_workers:
        time_ago = now - worker.last_seen

        tasks = [
            f"[bold]{task}[/bold]" if task == highlight_task else task
            for task in sorted(worker.tasks)
        ]

        table.add_row(
            worker.name,
            f"{time_ago} ago",
            "\n".join(tasks) if tasks else "(none)",
        )

    console = Console()
    console.print(table)


__all__ = [
    "iterate_with_timeout",
    "LogLevel",
    "LogFormat",
    "local_time",
    "default_worker_name",
    "duration",
    "set_logging_format",
    "set_logging_level",
    "validate_url",
    "handle_strike_wildcard",
    "interpret_python_value",
    "relative_time",
    "get_task_stats",
    "TaskStats",
    "print_workers",
]
