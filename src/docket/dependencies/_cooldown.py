"""Cooldown (trailing-edge) admission control dependency."""

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
    """Trailing-edge cooldown: blocks execution if one recently succeeded.

    Checks for a Redis key on entry.  If present, the task is blocked.
    The key is only set on *successful* exit, so failed tasks don't
    trigger the cooldown — they can be retried immediately.

    Works both as a default parameter and as ``Annotated`` metadata::

        # Per-task: don't start if one succeeded in the last 60s
        async def send_digest(
            cooldown: Cooldown = Cooldown(timedelta(seconds=60)),
        ) -> None: ...

        # Per-parameter: don't start for this customer if one succeeded in the last 60s
        async def send_notification(
            customer_id: Annotated[int, Cooldown(timedelta(seconds=60))],
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

    def _cooldown_key(self, function_name: str) -> str:
        scope = self.scope or current_docket.get().name
        if self._argument_name is not None:
            return f"{scope}:cooldown:{self._argument_name}:{self._argument_value}"
        return f"{scope}:cooldown:{function_name}"

    async def __aenter__(self) -> Cooldown:
        execution = current_execution.get()
        docket = current_docket.get()

        self._key = self._cooldown_key(execution.function_name)

        async with docket.redis() as redis:
            exists = await redis.exists(self._key)

        if exists:
            raise CooldownBlocked(execution, self._key, self.window)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            return

        docket = current_docket.get()
        window_ms = int(self.window.total_seconds() * 1000)

        async with docket.redis() as redis:
            await redis.set(self._key, 1, px=window_ms)
