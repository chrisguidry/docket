import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

from watchfiles import PythonFilter, run_process

from docket.cli._support import (
    LogFormat,
    LogLevel,
    set_logging_format,
    set_logging_level,
)


def _run_worker(
    worker: Callable[[], Coroutine[Any, Any, None]],
    logging_level: LogLevel,
    logging_format: LogFormat,
) -> None:
    set_logging_level(logging_level)
    set_logging_format(logging_format)
    asyncio.run(worker())


def run_with_reload(
    worker: Callable[[], Coroutine[Any, Any, None]],
    logging_level: LogLevel,
    logging_format: LogFormat,
) -> None:
    run_process(
        ".",
        target=_run_worker,
        args=(worker, logging_level, logging_format),
        watch_filter=PythonFilter(),
    )
