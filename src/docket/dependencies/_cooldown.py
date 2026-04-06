"""Cooldown admission control dependency.

Executes the first task immediately, then drops duplicates within the window.
Sets a Redis key on entry with TTL.
"""

from __future__ import annotations

from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Any

from uncalled_for import Depends

from ._base import AdmissionBlocked, Dependency
from ._contextual import CurrentDocket, CurrentExecution

if TYPE_CHECKING:  # pragma: no cover
    from ..docket import Docket
    from ..execution import Execution


class Cooldown(Dependency["Cooldown"]):
    """Execute first, drop duplicates within window.

    Sets a Redis key on entry with a TTL equal to the window. If the key
    already exists, the task is blocked and quietly dropped.

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

    execution: Execution = Depends(CurrentExecution)
    docket: Docket = Depends(CurrentDocket)

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
        scope = self.scope or self.docket.name
        if self._argument_name is not None:
            cooldown_key = (
                f"{scope}:cooldown:{self._argument_name}:{self._argument_value}"
            )
        else:
            cooldown_key = f"{scope}:cooldown:{self.execution.function_name}"

        window_ms = int(self.window.total_seconds() * 1000)

        async with self.docket.redis() as redis:
            acquired = await redis.set(cooldown_key, 1, nx=True, px=window_ms)

        if not acquired:
            raise AdmissionBlocked(
                self.execution,
                reason=f"cooldown ({self.window}) on {cooldown_key}",
                reschedule=False,
            )

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass
