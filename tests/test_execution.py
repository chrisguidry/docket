from typing import Annotated

import pytest

from docket import Docket, Worker
from docket.annotations import Logged
from docket.dependencies import CurrentDocket, CurrentWorker, Depends
from docket.execution import TaskFunction, compact_signature, get_signature


async def no_args() -> None: ...  # pragma: no cover


async def one_arg(a: str) -> None: ...  # pragma: no cover


async def two_args(a: str, b: str) -> None: ...  # pragma: no cover


async def optional_args(a: str, b: str, c: str = "c") -> None: ...  # pragma: no cover


async def logged_args(
    a: Annotated[str, Logged()],
    b: Annotated[str, Logged()] = "foo",
) -> None: ...  # pragma: no cover


async def a_dependency() -> str: ...  # pragma: no cover


async def dependencies(
    a: str,
    b: int = 42,
    c: str = Depends(a_dependency),
    docket: Docket = CurrentDocket(),
    worker: Worker = CurrentWorker(),
) -> None: ...  # pragma: no cover


async def only_dependencies(
    a: str = Depends(a_dependency),
    docket: Docket = CurrentDocket(),
    worker: Worker = CurrentWorker(),
) -> None: ...  # pragma: no cover


@pytest.mark.parametrize(
    "function, expected",
    [
        (no_args, ""),
        (one_arg, "a: str"),
        (two_args, "a: str, b: str"),
        (optional_args, "a: str, b: str, c: str = 'c'"),
        (logged_args, "a: str, b: str = 'foo'"),
        (dependencies, "a: str, b: int = 42, ..."),
        (only_dependencies, "..."),
    ],
)
async def test_compact_signature(
    docket: Docket, worker: Worker, function: TaskFunction, expected: str
):
    assert compact_signature(get_signature(function)) == expected
