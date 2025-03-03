"""
docket - A distributed background task system for Python functions.

docket focuses on scheduling future work as seamlessly and efficiently as immediate work.
"""

from importlib.metadata import version

__version__ = version("pydocket")

from .annotations import Logged
from .dependencies import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    ExponentialRetry,
    Retry,
    TaskKey,
    TaskLogger,
)
from .docket import Docket
from .execution import Execution
from .worker import Worker

__all__ = [
    "Docket",
    "Worker",
    "Execution",
    "CurrentDocket",
    "CurrentWorker",
    "CurrentExecution",
    "TaskKey",
    "TaskLogger",
    "Retry",
    "ExponentialRetry",
    "Logged",
    "__version__",
]
