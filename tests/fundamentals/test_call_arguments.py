"""Tests for CallArgument references and Depends keyword bindings."""

import logging
from uuid import uuid4

import pytest

from docket import CallArgument, Depends, Docket, TaskArgument, Worker


async def test_bare_call_argument_on_a_dependency_factory(
    docket: Docket, worker: Worker
):
    """A bare CallArgument takes the name of the parameter it is declared on"""

    called = 0

    async def load_user(user_id: str = CallArgument()) -> str:
        return f"user-{user_id}"

    async def dependent_task(
        user_id: str,
        user: str = Depends(load_user),
    ) -> None:
        assert user == f"user-{user_id}"

        nonlocal called
        called += 1

    await docket.add(dependent_task)(user_id="abc123")

    await worker.run_until_finished()

    assert called == 1


async def test_named_call_argument_on_a_dependency_factory(
    docket: Docket, worker: Worker
):
    """CallArgument("name") reads a differently-named task parameter"""

    called = 0

    async def load_recipient(user_id: str = CallArgument("recipient_id")) -> str:
        return f"user-{user_id}"

    async def dependent_task(
        recipient_id: str,
        recipient: str = Depends(load_recipient),
    ) -> None:
        assert recipient == f"user-{recipient_id}"

        nonlocal called
        called += 1

    await docket.add(dependent_task)(recipient_id="abc123")

    await worker.run_until_finished()

    assert called == 1


async def test_call_argument_reads_a_dependency_backed_parameter(
    docket: Docket, worker: Worker
):
    """A CallArgument can read a task parameter backed by another dependency"""

    called = 0
    tokens_made = 0

    async def make_token() -> str:
        nonlocal tokens_made
        tokens_made += 1
        return f"token-{uuid4()}"

    async def audit(token: str = CallArgument()) -> str:
        return token

    async def dependent_task(
        token: str = Depends(make_token),
        audited: str = Depends(audit),
    ) -> None:
        assert audited == token

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 1
    assert tokens_made == 1


async def test_caller_argument_wins_over_a_dependency_backed_parameter(
    docket: Docket, worker: Worker
):
    """A caller-supplied argument wins and the dependency is never resolved"""

    called = 0

    async def make_token() -> str:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    async def audit(token: str = CallArgument()) -> str:
        return token

    async def dependent_task(
        token: str = Depends(make_token),
        audited: str = Depends(audit),
    ) -> None:
        assert token == "from-the-caller"
        assert audited == "from-the-caller"

        nonlocal called
        called += 1

    await docket.add(dependent_task)(token="from-the-caller")

    await worker.run_until_finished()

    assert called == 1


async def test_bare_call_argument_in_a_depends_binding(docket: Docket, worker: Worker):
    """A bare CallArgument binding resolves the parameter it is bound to"""

    called = 0

    async def load_user(user_id: str) -> str:
        return f"user-{user_id}"

    async def dependent_task(
        user_id: str,
        user: str = Depends(load_user, user_id=CallArgument()),
    ) -> None:
        assert user == f"user-{user_id}"

        nonlocal called
        called += 1

    await docket.add(dependent_task)(user_id="abc123")

    await worker.run_until_finished()

    assert called == 1


async def test_named_call_argument_in_a_depends_binding(docket: Docket, worker: Worker):
    """A named CallArgument binding wires a task parameter to a factory parameter"""

    called = 0

    async def load_user(user_id: str) -> str:
        return f"user-{user_id}"

    async def dependent_task(
        owner: str,
        user: str = Depends(load_user, user_id=CallArgument("owner")),
    ) -> None:
        assert user == f"user-{owner}"

        nonlocal called
        called += 1

    await docket.add(dependent_task)(owner="abc123")

    await worker.run_until_finished()

    assert called == 1


async def test_plain_value_bindings_pass_through(docket: Docket, worker: Worker):
    """A plain value binding passes through to the factory as it is"""

    called = 0

    async def fetch_report(source: str, timeout: int = 30) -> str:
        return f"{source}:{timeout}"

    async def dependent_task(
        report: str = Depends(fetch_report, source="warehouse", timeout=5),
    ) -> None:
        assert report == "warehouse:5"

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 1


async def test_binding_replaces_a_factory_depends_default(
    docket: Docket, worker: Worker
):
    """A binding replaces the factory's own Depends default, which never runs"""

    called = 0

    async def get_replica() -> str:
        raise NotImplementedError("This should not be called")  # pragma: no cover

    async def get_primary() -> str:
        return "primary"

    async def get_orders(db: str = Depends(get_replica)) -> str:
        return f"orders-from-{db}"

    async def dependent_task(
        orders: str = Depends(get_orders, db=Depends(get_primary)),
    ) -> None:
        assert orders == "orders-from-primary"

        nonlocal called
        called += 1

    await docket.add(dependent_task)()

    await worker.run_until_finished()

    assert called == 1


async def test_optional_call_argument_yields_none_when_missing(
    docket: Docket, worker: Worker
):
    """CallArgument(optional=True) yields None for a missing parameter"""

    called = 0

    async def load_config(name: str | None = CallArgument(optional=True)) -> str:
        return name or "defaults"

    async def dependent_task(
        data: str,
        config: str = Depends(load_config),
    ) -> None:
        assert data == "payload"
        assert config == "defaults"

        nonlocal called
        called += 1

    await docket.add(dependent_task)(data="payload")

    await worker.run_until_finished()

    assert called == 1


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


async def test_task_argument_works_alongside_call_argument(
    docket: Docket, worker: Worker
):
    """TaskArgument and CallArgument can serve the same task together"""

    called = 0

    async def greeting(name: str = TaskArgument()) -> str:
        return f"Hello, {name}!"

    async def load_user(name: str = CallArgument()) -> str:
        return f"user-{name}"

    async def dependent_task(
        name: str,
        greet: str = Depends(greeting),
        user: str = Depends(load_user),
    ) -> None:
        assert greet == "Hello, alice!"
        assert user == "user-alice"

        nonlocal called
        called += 1

    await docket.add(dependent_task)(name="alice")

    await worker.run_until_finished()

    assert called == 1
