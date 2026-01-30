"""Dependency injection system for docket tasks.

This module provides the dependency injection primitives used to inject
resources, context, and behavior into task functions.
"""

from __future__ import annotations

from ._base import (
    AdmissionBlocked,
    CompletionHandler,
    Dependency,
    FailureHandler,
    Runtime,
    TaskOutcome,
    format_duration,
)
from ._concurrency import ConcurrencyBlocked, ConcurrencyLimit
from ._cron import Cron
from ._contextual import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    TaskArgument,
    TaskKey,
    TaskLogger,
)
from ._functional import (
    Depends,
    DependencyFunction,
    Shared,
    SharedContext,
    _Depends,
    _parameter_cache,
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
    "Runtime",
    "FailureHandler",
    "CompletionHandler",
    "TaskOutcome",
    "format_duration",
    # Contextual dependencies
    "CurrentDocket",
    "CurrentExecution",
    "CurrentWorker",
    "TaskArgument",
    "TaskKey",
    "TaskLogger",
    # Functional dependencies
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
    "AdmissionBlocked",
    "ConcurrencyBlocked",
    "ConcurrencyLimit",
    "Cron",
    "Perpetual",
    "Progress",
    "Timeout",
    # Resolution helpers
    "FailedDependency",
    "get_single_dependency_of_type",
    "get_single_dependency_parameter_of_type",
    "resolved_dependencies",
    "validate_dependencies",
    # fastmcp uses these for its DI integration; do not remove
    "_Depends",
    "_parameter_cache",
]
