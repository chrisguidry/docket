"""Static-typing tests for `Depends`.

`assert_type` is a runtime no-op, so these run cleanly under pytest.  The
real value comes from pyright (strict mode over `tests/`), which verifies
that `Depends(factory)` is inferred as the inner value type for each of
the four factory shapes, rather than collapsing to the
`R | Awaitable[R] | AbstractContextManager[R] | AbstractAsyncContextManager[R]`
union that the underlying `DependencyFactory[R]` would otherwise produce.

Each factory is also invoked once at runtime to keep coverage honest --
the typing claims are the point, but exercising the factories rules out
type-only stubs that would never actually work.
"""

from collections.abc import AsyncGenerator, Awaitable, Generator
from contextlib import (
    AbstractAsyncContextManager,
    AbstractContextManager,
    asynccontextmanager,
    contextmanager,
)
from dataclasses import dataclass
from typing import assert_type

from docket.dependencies import Depends


@dataclass
class User:
    id: int


def sync_factory() -> User:
    return User(id=1)


async def async_factory() -> User:
    return User(id=2)


@contextmanager
def sync_ctx_factory() -> Generator[User]:
    yield User(id=3)


@asynccontextmanager
async def async_ctx_factory() -> AsyncGenerator[User]:
    yield User(id=4)


def explicit_sync_ctx_factory() -> AbstractContextManager[User]:
    return sync_ctx_factory()


def explicit_async_ctx_factory() -> AbstractAsyncContextManager[User]:
    return async_ctx_factory()


def explicit_awaitable_factory() -> Awaitable[User]:
    return async_factory()


async def test_depends_unwraps_each_factory_shape() -> None:
    """`Depends` returns the inner value type for every supported factory."""
    assert_type(Depends(sync_factory), User)
    assert sync_factory() == User(id=1)

    assert_type(Depends(async_factory), User)
    assert await async_factory() == User(id=2)

    assert_type(Depends(sync_ctx_factory), User)
    with sync_ctx_factory() as user:
        assert user == User(id=3)

    assert_type(Depends(async_ctx_factory), User)
    async with async_ctx_factory() as user:
        assert user == User(id=4)

    assert_type(Depends(explicit_sync_ctx_factory), User)
    with explicit_sync_ctx_factory() as user:
        assert user == User(id=3)

    assert_type(Depends(explicit_async_ctx_factory), User)
    async with explicit_async_ctx_factory() as user:
        assert user == User(id=4)

    assert_type(Depends(explicit_awaitable_factory), User)
    assert await explicit_awaitable_factory() == User(id=2)
