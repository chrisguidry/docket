"""Dependency injection system for docket tasks.

This module provides the dependency injection primitives used to inject
resources, context, and behavior into task functions.
"""

from ._base import Dependency
from ._concurrency import ConcurrencyLimit
from ._context import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    TaskArgument,
    TaskKey,
    TaskLogger,
    _CurrentDocket,
    _CurrentExecution,
    _CurrentWorker,
    _TaskArgument,
    _TaskKey,
    _TaskLogger,
)
from ._depends import (
    Depends,
    DependencyFunction,
    _Depends,
    get_dependency_parameters,
)
from ._perpetual import Perpetual
from ._progress import Progress
from ._resolution import (
    FailedDependency,
    get_single_dependency_of_type,
    get_single_dependency_parameter_of_type,
    resolved_dependencies,
    validate_dependencies,
)
from ._retry import ExponentialRetry, ForcedRetry, Retry
from ._shared import Shared, SharedContext, SharedFactory, _Shared
from ._timeout import Timeout

__all__ = [
    # Base
    "Dependency",
    # Context dependencies
    "CurrentDocket",
    "CurrentExecution",
    "CurrentWorker",
    "TaskArgument",
    "TaskKey",
    "TaskLogger",
    # Internal context classes (used in some tests)
    "_CurrentDocket",
    "_CurrentExecution",
    "_CurrentWorker",
    "_TaskArgument",
    "_TaskKey",
    "_TaskLogger",
    # Depends
    "Depends",
    "DependencyFunction",
    "_Depends",
    "get_dependency_parameters",
    # Shared
    "Shared",
    "SharedContext",
    "SharedFactory",
    "_Shared",
    # Retry
    "ForcedRetry",
    "Retry",
    "ExponentialRetry",
    # Other dependencies
    "ConcurrencyLimit",
    "Perpetual",
    "Progress",
    "Timeout",
    # Resolution helpers
    "FailedDependency",
    "get_single_dependency_of_type",
    "get_single_dependency_parameter_of_type",
    "resolved_dependencies",
    "validate_dependencies",
]
