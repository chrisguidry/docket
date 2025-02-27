"""
docket - A distributed background task system for Python functions.

docket focuses on scheduling future work as seamlessly and efficiently as immediate work.
"""

from importlib.metadata import version

__version__ = version("docket")

from .dependencies import CurrentDocket, CurrentWorker, Retry
from .docket import Docket
from .execution import Execution
from .worker import Worker

__all__ = [
    "Docket",
    "Worker",
    "Execution",
    "CurrentDocket",
    "CurrentWorker",
    "Retry",
    "__version__",
]
