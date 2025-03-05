import asyncio
import logging
from datetime import datetime, timezone

from .dependencies import CurrentDocket, CurrentExecution, CurrentWorker, Retry
from .docket import Docket, TaskCollection
from .execution import Execution
from .worker import Worker

logger: logging.Logger = logging.getLogger(__name__)


async def trace(
    message: str,
    docket: Docket = CurrentDocket(),
    worker: Worker = CurrentWorker(),
    execution: Execution = CurrentExecution(),
) -> None:
    logger.info(
        "%s: %r added to docket %r %s ago now running on worker %r",
        message,
        execution.key,
        docket.name,
        (datetime.now(timezone.utc) - execution.when),
        worker.name,
        extra={
            "docket.name": docket.name,
            "worker.name": worker.name,
            "execution.key": execution.key,
        },
    )


async def fail(
    message: str,
    docket: Docket = CurrentDocket(),
    worker: Worker = CurrentWorker(),
    execution: Execution = CurrentExecution(),
    retry: Retry = Retry(attempts=2),
) -> None:
    raise Exception(
        f"{message}: {execution.key} added to docket "
        f"{docket.name} {datetime.now(timezone.utc) - execution.when} "
        f"ago now running on worker {worker.name}"
    )


async def sleep(seconds: float) -> None:
    logger.info("Sleeping for %s seconds", seconds)
    await asyncio.sleep(seconds)


standard_tasks: TaskCollection = [
    trace,
    fail,
    sleep,
]
