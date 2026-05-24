"""Regression test for the `_acked` flag and `_terminal` ordering.

When `_terminal` succeeds server-side but the client errors on the way back
(e.g. a network blip on the response read), the Python `_mark_as_terminal`
helper saw an exception and never set `self._acked = True`.  The worker's
safety net in `process_completed_tasks` then fired `mark_as_failed(error=None)`
on the already-committed execution, overwriting state=COMPLETED with
state=FAILED and a missing error payload.

The fix is to set `_acked = True` *before* the awaited `_terminal` call: the
moment we hand the call off to Redis we accept responsibility for the ack,
since the server commit is authoritative.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import ConnectionError

from docket import Docket
from docket.execution import Execution


async def _make_execution(docket: Docket) -> Execution:
    async def the_task() -> None: ...

    docket.register(the_task)
    return await docket.add(the_task)()


async def test_acked_is_true_when_terminal_raises_during_mark_as_completed(
    docket: Docket,
) -> None:
    execution = await _make_execution(docket)
    boom = ConnectionError("simulated network blip on _terminal response")

    with patch("docket.execution._terminal", new=AsyncMock(side_effect=boom)):
        with pytest.raises(ConnectionError):
            await execution.mark_as_completed()

    assert execution._acked, (  # pyright: ignore[reportPrivateUsage]
        "Once the Lua call is in flight, the server commit is authoritative; "
        "`_acked` must be True even if the client raises on the response, "
        "so the worker safety net does not overwrite a committed terminal state."
    )


async def test_acked_is_true_when_terminal_raises_during_mark_as_failed(
    docket: Docket,
) -> None:
    execution = await _make_execution(docket)
    boom = ConnectionError("simulated network blip on _terminal response")

    with patch("docket.execution._terminal", new=AsyncMock(side_effect=boom)):
        with pytest.raises(ConnectionError):
            await execution.mark_as_failed("boom")

    assert execution._acked  # pyright: ignore[reportPrivateUsage]


async def test_acked_is_true_when_terminal_raises_during_mark_as_cancelled(
    docket: Docket,
) -> None:
    execution = await _make_execution(docket)
    boom = ConnectionError("simulated network blip on _terminal response")

    with patch("docket.execution._terminal", new=AsyncMock(side_effect=boom)):
        with pytest.raises(ConnectionError):
            await execution.mark_as_cancelled()

    assert execution._acked  # pyright: ignore[reportPrivateUsage]
