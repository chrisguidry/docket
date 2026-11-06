"""Tests for dependencies that supply a task parameter's value from Annotated metadata.

    async def process(customer_id: int, db: Annotated[DB, Depends(get_db)] = Provided): ...

`Provided` lets `docket.add(process)(...)` type-check without `db`.  Each place
docket reads the caller's arguments has to accept a call that leaves it out,
with or without the default.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

from docket import ConcurrencyLimit, Disposition, Docket, Execution, Provided, Worker
from docket.dependencies import (
    CurrentDocket,
    CurrentExecution,
    CurrentWorker,
    Depends,
    TaskArgument,
    TaskKey,
    TaskLogger,
)


def get_db() -> str:
    return "db"


DB = Annotated[str, Depends(get_db)]


async def test_annotated_depends_supplies_the_value_with_positional_arguments(
    docket: Docket, worker: Worker
):
    seen: list[tuple[int, str]] = []

    async def task(customer_id: int, db: DB = Provided):
        seen.append((customer_id, db))

    await docket.add(task)(5)
    await worker.run_until_finished()

    assert seen == [(5, "db")]


async def test_annotated_depends_supplies_the_value_with_keyword_arguments(
    docket: Docket, worker: Worker
):
    seen: list[tuple[int, str]] = []

    async def task(customer_id: int, db: DB = Provided):
        seen.append((customer_id, db))

    await docket.add(task)(customer_id=5)
    await worker.run_until_finished()

    assert seen == [(5, "db")]


async def test_contextual_dependencies_supply_values_as_metadata(
    docket: Docket, worker: Worker
):
    seen: dict[str, object] = {}

    async def task(
        the_docket: Annotated[Docket, CurrentDocket()] = Provided,
        the_worker: Annotated[Worker, CurrentWorker()] = Provided,
        the_execution: Annotated[Execution, CurrentExecution()] = Provided,
        key: Annotated[str, TaskKey()] = Provided,
        logger: Annotated[
            logging.LoggerAdapter[logging.Logger], TaskLogger()
        ] = Provided,
    ):
        seen.update(
            docket=the_docket,
            worker=the_worker,
            execution_key=the_execution.key,
            key=key,
            logger=type(logger),
        )

    await docket.add(task, key="the-key")()
    await worker.run_until_finished()

    assert seen == {
        "docket": docket,
        "worker": worker,
        "execution_key": "the-key",
        "key": "the-key",
        "logger": logging.LoggerAdapter,
    }


def lookup_customer(customer_id: Annotated[int, TaskArgument()]) -> str:
    return f"customer {customer_id}"


async def test_task_argument_sees_positional_arguments(docket: Docket, worker: Worker):
    seen: list[str] = []

    async def task(
        customer_id: int,
        name: Annotated[str, Depends(lookup_customer)] = Provided,
    ):
        seen.append(name)

    await docket.add(task)(3)
    await worker.run_until_finished()

    assert seen == ["customer 3"]


async def test_task_argument_sees_keyword_arguments(docket: Docket, worker: Worker):
    seen: list[str] = []

    async def task(
        customer_id: int,
        name: Annotated[str, Depends(lookup_customer)] = Provided,
    ):
        seen.append(name)

    await docket.add(task)(customer_id=3)
    await worker.run_until_finished()

    assert seen == ["customer 3"]


async def test_concurrency_limit_reads_an_argument_beside_a_provider(
    docket: Docket, worker: Worker
):
    seen: list[tuple[int, str]] = []

    async def task(
        customer_id: int,
        db: DB = Provided,
        concurrency: ConcurrencyLimit = ConcurrencyLimit("customer_id", 1),
    ):
        seen.append((customer_id, db))

    await docket.add(task)(customer_id=8)
    await worker.run_until_finished()

    assert seen == [(8, "db")]


async def test_parameter_strike_matches_a_task_with_a_provider(
    docket: Docket, worker: Worker
):
    seen: list[int] = []

    async def task(customer_id: int, db: DB = Provided):
        seen.append(customer_id)

    docket.register(task)
    await docket.strike(task, "customer_id", "==", 7)

    await docket.add(task)(customer_id=7)
    await docket.add(task)(customer_id=9)
    await worker.run_until_finished()

    assert seen == [9]


async def test_caller_value_overrides_an_annotated_provider(
    docket: Docket, worker: Worker
):
    seen: list[str] = []

    async def task(db: DB = Provided):
        seen.append(db)

    await docket.add(task)(db="mine")
    await worker.run_until_finished()

    assert seen == ["mine"]


# Without a Provided default the parameter is required, so a type checker
# rejects these docket.add calls.  Code that is not type-checked still makes
# them, and docket has to run the task correctly.


async def test_strike_matches_a_task_without_provided(docket: Docket, worker: Worker):
    seen: list[int] = []

    async def task(customer_id: int, db: DB):
        seen.append(customer_id)

    docket.register(task)
    await docket.strike(task, "customer_id", "==", 7)

    await docket.add(task)(customer_id=7)  # pyright: ignore[reportCallIssue]
    await docket.add(task)(customer_id=9)  # pyright: ignore[reportCallIssue]
    await worker.run_until_finished()

    assert seen == [9]


async def test_concurrency_limit_reads_an_argument_without_provided(
    docket: Docket, worker: Worker
):
    seen: list[int] = []

    async def task(
        customer_id: int,
        db: DB,
        concurrency: ConcurrencyLimit = ConcurrencyLimit("customer_id", 1),
    ):
        seen.append(customer_id)

    await docket.add(task)(customer_id=8)  # pyright: ignore[reportCallIssue]
    await worker.run_until_finished()

    assert seen == [8]


async def test_task_argument_sees_positional_arguments_without_provided(
    docket: Docket, worker: Worker
):
    seen: list[str] = []

    async def task(customer_id: int, name: Annotated[str, Depends(lookup_customer)]):
        seen.append(name)

    await docket.add(task)(3)  # pyright: ignore[reportCallIssue]
    await worker.run_until_finished()

    assert seen == ["customer 3"]


async def test_strike_does_not_hide_a_call_missing_an_ordinary_argument(
    docket: Docket,
):
    async def task(customer_id: int, region: str, db: DB = Provided): ...

    docket.register(task)
    await docket.strike(task, "customer_id", "==", 7)

    arguments: dict[str, Any] = {"customer_id": 7}
    execution = await docket.add(task)(**arguments)

    assert execution.disposition is not Disposition.STRUCK
