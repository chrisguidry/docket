import logging
from datetime import datetime, timezone

from .dependencies import CurrentDocket, CurrentExecution, CurrentWorker
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
        "%s: %r added to docket %r %ss ago now running on worker %r",
        message,
        execution.key,
        docket.name,
        (datetime.now(timezone.utc) - execution.when).total_seconds(),
        worker.name,
        extra={
            "docket.name": docket.name,
            "worker.name": worker.name,
            "execution.key": execution.key,
        },
    )


standard_tasks: TaskCollection = [
    trace,
]
