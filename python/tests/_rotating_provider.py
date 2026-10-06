"""A streaming credential provider that rotates on demand.

It behaves like redis-entraid's EntraIdCredentialsProvider without Azure.  That
provider keeps its callback in redis-py's CredentialsListener, which has one
slot, so each on_next() replaces the callback before it; this one does the
same.  rotate() is the token refresh: it changes the credentials that new
connections get, and pushes them to that one callback as an AUTH token whose
"oid" claim is the username.
"""

from typing import Any, Callable

from redis.auth.token import SimpleToken
from redis.credentials import StreamingCredentialProvider


class RotatingProvider(StreamingCredentialProvider):
    def __init__(self, username: str, password: str) -> None:
        self.credentials = (username, password)
        self._on_next: Callable[[Any], Any] | None = None

    # Only the async method: docket's clients are all asyncio, so redis-py
    # never asks for the credentials synchronously.
    async def get_credentials_async(self) -> tuple[str, str]:
        return self.credentials

    def on_next(self, callback: Callable[[Any], Any]) -> None:
        self._on_next = callback

    def on_error(self, callback: Callable[[Exception], Any]) -> None:
        pass

    def is_streaming(self) -> bool:
        return True

    async def rotate(self, username: str, password: str) -> None:
        self.credentials = (username, password)
        if self._on_next is None:
            return
        token = SimpleToken(password, -1, -1, {"oid": username})
        # redis-py's asyncio clients register a coroutine function.
        await self._on_next(token)
