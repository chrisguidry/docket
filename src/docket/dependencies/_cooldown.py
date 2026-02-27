"""Cooldown admission control dependency.

Executes the first task immediately, then drops duplicates within the window.
Sets a Redis key on entry with TTL.
"""

from __future__ import annotations

from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Any

from ._base import AdmissionBlocked, Dependency, current_docket, current_execution

if TYPE_CHECKING:  # pragma: no cover
    from ..execution import Execution


class CooldownBlocked(AdmissionBlocked):
    """Raised when a task is blocked by cooldown."""

    reschedule = False

    def __init__(self, execution: Execution, cooldown_key: str, window: timedelta):
        self.cooldown_key = cooldown_key
        self.window = window
        reason = f"cooldown ({window}) on {cooldown_key}"
        super().__init__(execution, reason=reason)


class Cooldown(Dependency["Cooldown"]):
    """Execute first, drop duplicates within window.

    Sets a Redis key on entry with a TTL equal to the window. If the key
    already exists, the task is blocked and silently dropped.

    Works both as a default parameter and as ``Annotated`` metadata::

        # Per-task: don't start if one started in the last 30s
        async def process_webhooks(
            cooldown: Cooldown = Cooldown(timedelta(seconds=30)),
        ) -> None: ...

        # Per-parameter: don't start for this customer if one started in the last 30s
        async def process_customer(
            customer_id: Annotated[int, Cooldown(timedelta(seconds=30))],
        ) -> None: ...
    """

    single: bool = True

    def __init__(self, window: timedelta, *, scope: str | None = None) -> None:
        self.window = window
        self.scope = scope
        self._argument_name: str | None = None
        self._argument_value: Any = None

    def bind_to_parameter(self, name: str, value: Any) -> Cooldown:
        bound = Cooldown(self.window, scope=self.scope)
        bound._argument_name = name
        bound._argument_value = value
        return bound

    async def __aenter__(self) -> Cooldown:
        execution = current_execution.get()
        docket = current_docket.get()

        scope = self.scope or docket.name
        if self._argument_name is not None:
            cooldown_key = (
                f"{scope}:cooldown:{self._argument_name}:{self._argument_value}"
            )
        else:
            cooldown_key = f"{scope}:cooldown:{execution.function_name}"

        window_ms = int(self.window.total_seconds() * 1000)

        async with docket.redis() as redis:
            acquired = await redis.set(cooldown_key, 1, nx=True, px=window_ms)

        if not acquired:
            raise CooldownBlocked(execution, cooldown_key, self.window)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass
