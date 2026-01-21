"""Concurrency limiting dependency."""

from __future__ import annotations

from ._base import Dependency


class ConcurrencyLimit(Dependency):
    """Configures concurrency limits for a task based on specific argument values.

    This allows fine-grained control over task execution by limiting concurrent
    tasks based on the value of specific arguments.

    Example:

    ```python
    async def process_customer(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit("customer_id", max_concurrent=1)
    ) -> None:
        # Only one task per customer_id will run at a time
        ...

    async def backup_db(
        db_name: str,
        concurrency: ConcurrencyLimit = ConcurrencyLimit("db_name", max_concurrent=3)
    ) -> None:
        # Only 3 backup tasks per database name will run at a time
        ...
    ```
    """

    single: bool = True

    def __init__(
        self, argument_name: str, max_concurrent: int = 1, scope: str | None = None
    ) -> None:
        """
        Args:
            argument_name: The name of the task argument to use for concurrency grouping
            max_concurrent: Maximum number of concurrent tasks per unique argument value
            scope: Optional scope prefix for Redis keys (defaults to docket name)
        """
        self.argument_name = argument_name
        self.max_concurrent = max_concurrent
        self.scope = scope
        self._concurrency_key: str | None = None
        self._initialized: bool = False

    async def __aenter__(self) -> ConcurrencyLimit:
        execution = self.execution.get()
        docket = self.docket.get()

        # Get the argument value to group by
        try:
            argument_value = execution.get_argument(self.argument_name)
        except KeyError:
            # If argument not found, create a bypass limit that doesn't apply concurrency control
            limit = ConcurrencyLimit(
                self.argument_name, self.max_concurrent, self.scope
            )
            limit._concurrency_key = None  # Special marker for bypassed concurrency
            limit._initialized = True  # Mark as initialized but bypassed
            return limit

        # Create a concurrency key for this specific argument value
        scope = self.scope or docket.name
        self._concurrency_key = (
            f"{scope}:concurrency:{self.argument_name}:{argument_value}"
        )

        limit = ConcurrencyLimit(self.argument_name, self.max_concurrent, self.scope)
        limit._concurrency_key = self._concurrency_key
        limit._initialized = True  # Mark as initialized
        return limit

    @property
    def concurrency_key(self) -> str | None:
        """Redis key used for tracking concurrency for this specific argument value.
        Returns None when concurrency control is bypassed due to missing arguments.
        Raises RuntimeError if accessed before initialization."""
        if not self._initialized:
            raise RuntimeError(
                "ConcurrencyLimit not initialized - use within task context"
            )
        return self._concurrency_key

    @property
    def is_bypassed(self) -> bool:
        """Returns True if concurrency control is bypassed due to missing arguments."""
        return self._initialized and self._concurrency_key is None
