"""Edge-case tests for CallArgument references and Depends bindings.

The happy-path tests live in tests/fundamentals/test_call_arguments.py.
"""

import logging
from typing import cast

import pytest

from docket import Docket, Worker
from docket.dependencies import CallArgument, Dependency, Depends


async def test_circular_call_arguments_fail_the_task(
    docket: Docket, worker: Worker, caplog: pytest.LogCaptureFixture
):
    """CallArgument references that form a cycle fail the task with CycleError"""

    async def needs_b(b: str = CallArgument("b")) -> str:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    async def needs_a(a: str = CallArgument("a")) -> str:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    async def dependent_task(
        a: str = Depends(needs_b),
        b: str = Depends(needs_a),
    ) -> None:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    await docket.add(dependent_task)()

    with caplog.at_level(logging.ERROR):
        await worker.run_until_finished()

    assert "Failed to resolve dependencies" in caplog.text
    assert "CycleError" in caplog.text
    assert "Circular argument reference" in caplog.text


async def test_positional_argument_overrides_a_dependency_backed_parameter(
    docket: Docket, worker: Worker
):
    """A positionally-supplied argument wins over the parameter's dependency"""

    async def default_region() -> str:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    received: list[str] = []

    async def dependent_task(region: str = Depends(default_region)) -> None:
        received.append(region)

    await docket.add(dependent_task)("eu-west-1")

    await worker.run_until_finished()

    assert received == ["eu-west-1"]


async def test_referenced_sibling_dependency_is_entered_once(
    docket: Docket, worker: Worker
):
    """A CallArgument reference shares the sibling's resolution, never repeats it"""

    class Counting(Dependency[str]):
        def __init__(self) -> None:
            self.enters = 0

        async def __aenter__(self) -> str:
            self.enters += 1
            return f"value-{self.enters}"

    counting = Counting()

    async def consume(token: str = CallArgument()) -> str:
        return f"used {token}"

    received: list[tuple[str, str]] = []

    async def dependent_task(
        token: str = cast(str, counting),
        consumer: str = Depends(consume),
    ) -> None:
        received.append((token, consumer))

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert received == [("value-1", "used value-1")]
    assert counting.enters == 1
