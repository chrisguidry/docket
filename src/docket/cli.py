import asyncio
import enum
import importlib
import logging
import socket
import sys
from datetime import timedelta
from typing import Annotated, Any

import typer

from . import __version__, tasks
from .docket import Docket
from .execution import Operator
from .worker import Worker

app: typer.Typer = typer.Typer(
    help="Docket - A distributed background task system for Python functions",
    add_completion=True,
    no_args_is_help=True,
)


class LogLevel(enum.StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogFormat(enum.StrEnum):
    RICH = "rich"
    PLAIN = "plain"
    JSON = "json"


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
    logging.getLogger().setLevel(level)


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


@app.command(
    help="Start a worker to process tasks",
)
def worker(
    tasks: Annotated[
        list[str],
        typer.Option(
            "--tasks",
            help=(
                "The dotted path of a task collection to register with the docket. "
                "This can be specified multiple times.  A task collection is any "
                "iterable of async functions."
            ),
        ),
    ] = ["docket.tasks:standard_tasks"],
    docket_: Annotated[
        str,
        typer.Option(
            "--docket",
            help="The name of the docket",
            envvar="DOCKET_NAME",
        ),
    ] = "docket",
    url: Annotated[
        str,
        typer.Option(
            help="The URL of the Redis server",
            envvar="DOCKET_URL",
        ),
    ] = "redis://localhost:6379/0",
    name: Annotated[
        str | None,
        typer.Option(
            help="The name of the worker",
            envvar="DOCKET_WORKER_NAME",
        ),
    ] = socket.gethostname(),
    logging_level: Annotated[
        LogLevel,
        typer.Option(
            help="The logging level",
            envvar="DOCKET_LOGGING_LEVEL",
            callback=set_logging_level,
        ),
    ] = LogLevel.INFO,
    logging_format: Annotated[
        LogFormat,
        typer.Option(
            help="The logging format",
            envvar="DOCKET_LOGGING_FORMAT",
            callback=set_logging_format,
        ),
    ] = LogFormat.RICH if sys.stdout.isatty() else LogFormat.PLAIN,
    concurrency: Annotated[
        int,
        typer.Option(
            help="The maximum number of tasks to process concurrently",
            envvar="DOCKET_WORKER_CONCURRENCY",
        ),
    ] = 10,
    redelivery_timeout: Annotated[
        timedelta,
        typer.Option(
            parser=duration,
            help="How long to wait before redelivering a task to another worker",
            envvar="DOCKET_WORKER_REDELIVERY_TIMEOUT",
        ),
    ] = timedelta(minutes=5),
    reconnection_delay: Annotated[
        timedelta,
        typer.Option(
            parser=duration,
            help=(
                "How long to wait before reconnecting to the Redis server after "
                "a connection error"
            ),
            envvar="DOCKET_WORKER_RECONNECTION_DELAY",
        ),
    ] = timedelta(seconds=5),
    until_finished: Annotated[
        bool,
        typer.Option(
            "--until-finished",
            help="Exit after the current docket is finished",
        ),
    ] = False,
) -> None:
    asyncio.run(
        Worker.run(
            docket_name=docket_,
            url=url,
            name=name,
            concurrency=concurrency,
            redelivery_timeout=redelivery_timeout,
            reconnection_delay=reconnection_delay,
            until_finished=until_finished,
            tasks=tasks,
        )
    )


@app.command(help="Strikes a task or parameters from the Docket")
def strike(
    function: Annotated[
        str,
        typer.Argument(
            help="The function to strike",
            callback=handle_strike_wildcard,
        ),
    ] = "*",
    parameter: Annotated[
        str,
        typer.Argument(
            help="The parameter to strike",
            callback=handle_strike_wildcard,
        ),
    ] = "*",
    operator: Annotated[
        Operator,
        typer.Argument(
            help="The operator to compare the value against",
        ),
    ] = Operator.EQUAL,
    value: Annotated[
        str | None,
        typer.Argument(
            help="The value to strike from the docket",
        ),
    ] = None,
    docket_: Annotated[
        str,
        typer.Option(
            "--docket",
            help="The name of the docket",
            envvar="DOCKET_NAME",
        ),
    ] = "docket",
    url: Annotated[
        str,
        typer.Option(
            help="The URL of the Redis server",
            envvar="DOCKET_URL",
        ),
    ] = "redis://localhost:6379/0",
) -> None:
    if not function and not parameter:
        raise typer.BadParameter(
            message="Must provide either a function and/or a parameter",
        )

    value_ = interpret_python_value(value)
    if parameter:
        function_name = f"{function or '(all tasks)'}"
        print(f"Striking {function_name} {parameter} {operator} {value_!r}")
    else:
        print(f"Striking {function}")

    async def run() -> None:
        async with Docket(name=docket_, url=url) as docket:
            await docket.strike(function, parameter, operator, value_)

    asyncio.run(run())


@app.command(help="Restores a task or parameters to the Docket")
def restore(
    function: Annotated[
        str,
        typer.Argument(
            help="The function to restore",
            callback=handle_strike_wildcard,
        ),
    ] = "*",
    parameter: Annotated[
        str,
        typer.Argument(
            help="The parameter to restore",
            callback=handle_strike_wildcard,
        ),
    ] = "*",
    operator: Annotated[
        Operator,
        typer.Argument(
            help="The operator to compare the value against",
        ),
    ] = Operator.EQUAL,
    value: Annotated[
        str | None,
        typer.Argument(
            help="The value to restore to the docket",
        ),
    ] = None,
    docket_: Annotated[
        str,
        typer.Option(
            "--docket",
            help="The name of the docket",
            envvar="DOCKET_NAME",
        ),
    ] = "docket",
    url: Annotated[
        str,
        typer.Option(
            help="The URL of the Redis server",
            envvar="DOCKET_URL",
        ),
    ] = "redis://localhost:6379/0",
) -> None:
    if not function and not parameter:
        raise typer.BadParameter(
            message="Must provide either a function and/or a parameter",
        )

    value_ = interpret_python_value(value)
    if parameter:
        function_name = f"{function or '(all tasks)'}"
        print(f"Striking {function_name} {parameter} {operator} {value_!r}")
    else:
        print(f"Restoring {function}")

    async def run() -> None:
        async with Docket(name=docket_, url=url) as docket:
            await docket.restore(function, parameter, operator, value_)

    asyncio.run(run())


@app.command(help="Adds a trace task to the Docket")
def trace(
    docket_: Annotated[
        str,
        typer.Option(
            "--docket",
            help="The name of the docket",
            envvar="DOCKET_NAME",
        ),
    ] = "docket",
    url: Annotated[
        str,
        typer.Option(
            help="The URL of the Redis server",
            envvar="DOCKET_URL",
        ),
    ] = "redis://localhost:6379/0",
    message: Annotated[
        str,
        typer.Argument(
            help="The message to print",
        ),
    ] = "Howdy!",
    error: Annotated[
        bool,
        typer.Option(
            "--error",
            help="Intentionally raise an error",
        ),
    ] = False,
) -> None:
    async def run() -> None:
        async with Docket(name=docket_, url=url) as docket:
            if error:
                execution = await docket.add(tasks.fail)(message)
            else:
                execution = await docket.add(tasks.trace)(message)

            print(
                f"Added {execution.function.__name__} task {execution.key!r} to "
                f"the docket {docket.name!r}"
            )

    asyncio.run(run())


@app.command(
    help="Print the version of Docket",
)
def version() -> None:
    print(__version__)
