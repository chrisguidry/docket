"""Dependency injection system for docket tasks.

This module provides the dependency injection primitives used to inject
resources, context, and behavior into task functions.
"""

from __future__ import annotations

from ._base import Dependency
from ._concurrency import ConcurrencyLimit
from ._contextual import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    TaskArgument,
    TaskKey,
    TaskLogger,
)
from ._depends import (
    Depends,
    DependencyFunction,
    Shared,
    SharedContext,
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
from ._timeout import Timeout

__all__ = [
    # Base
    "Dependency",
    # Contextual dependencies
    "CurrentDocket",
    "CurrentExecution",
    "CurrentWorker",
    "TaskArgument",
    "TaskKey",
    "TaskLogger",
    # Depends and Shared
    "Depends",
    "DependencyFunction",
    "Shared",
    "SharedContext",
    "get_dependency_parameters",
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
