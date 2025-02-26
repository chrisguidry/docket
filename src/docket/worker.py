import inspect
import logging
import sys
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Protocol, Self, Sequence, cast
from uuid import uuid4

from redis import RedisError

from .docket import Docket, Execution, Retry

logger: logging.Logger = logging.getLogger(__name__)

RedisStreamID = bytes
RedisMessageID = bytes
RedisMessage = dict[bytes, bytes]
RedisStream = tuple[RedisStreamID, Sequence[tuple[RedisMessageID, RedisMessage]]]
RedisReadGroupResponse = Sequence[RedisStream]


class _stream_due_tasks(Protocol):
    async def __call__(
        self, keys: list[str], args: list[str | float]
    ) -> tuple[int, int]: ...


class Worker:
    name: str
    docket: Docket

    def __init__(self, docket: Docket) -> None:
        self.name = f"worker:{uuid4()}"
        self.docket = docket

    async def __aenter__(self) -> Self:
        async with self.docket.redis() as redis:
            try:
                await redis.xgroup_create(
                    groupname="docket",
                    name=f"{self.docket.name}:stream",
                    mkstream=True,
                )
            except RedisError as e:
                assert "BUSYGROUP" in repr(e)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass

    @property
    def queue_key(self) -> str:
        return f"{self.docket.name}:queue"

    @property
    def stream_key(self) -> str:
        return f"{self.docket.name}:stream"

    async def run_until_current(self) -> None:
        log_context = {
            "queue_key": self.queue_key,
            "stream_key": self.stream_key,
        }

        async with self.docket.redis() as redis:
            stream_due_tasks: _stream_due_tasks = cast(
                _stream_due_tasks,
                redis.register_script(
                    # Lua script to atomically move scheduled tasks to the stream
                    # KEYS[1]: queue key (sorted set)
                    # KEYS[2]: stream key
                    # ARGV[1]: current timestamp
                    # ARGV[2]: docket name prefix
                    """
                local total = redis.call('ZCARD', KEYS[1])
                local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
                local moved = 0

                for i, key in ipairs(tasks) do
                    local hash_key = ARGV[2] .. ":" .. key
                    local task_data = redis.call('HGETALL', hash_key)

                    if #task_data > 0 then
                        local task = {}
                        for j = 1, #task_data, 2 do
                            task[task_data[j]] = task_data[j+1]
                        end

                        redis.call('XADD', KEYS[2], '*',
                            'key', task['key'],
                            'when', task['when'],
                            'function', task['function'],
                            'args', task['args'],
                            'kwargs', task['kwargs'],
                            'attempt', task['attempt']
                        )
                        redis.call('DEL', hash_key)
                        moved = moved + 1
                    end
                end

                if moved > 0 then
                    redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
                end

                return {total, moved}
                """
                ),
            )

            total, moved = sys.maxsize, 0
            while total:
                now = datetime.now(timezone.utc)
                total, moved = await stream_due_tasks(
                    keys=[self.queue_key, self.stream_key],
                    args=[now.timestamp(), self.docket.name],
                )
                logger.info(
                    "Moved %d/%d scheduled tasks from %s to %s",
                    moved,
                    total,
                    self.queue_key,
                    self.stream_key,
                    extra=log_context,
                )

                response: RedisReadGroupResponse = await redis.xreadgroup(
                    groupname="docket",
                    consumername=self.name,
                    streams={f"{self.docket.name}:stream": ">"},
                    block=10,
                )
                if not response:
                    continue

                for _, messages in response:
                    for _, message in messages:
                        execution = Execution.from_message(
                            self.docket.tasks[message[b"function"].decode()],
                            message,
                        )

                        logger.info(
                            "Executing task %s with args %s and kwargs %s",
                            execution.key,
                            execution.args,
                            execution.kwargs,
                            extra={
                                **log_context,
                                "function": execution.function.__name__,
                            },
                        )

                        special_arguments = self._get_special_arguments(execution)

                        try:
                            await execution.function(
                                *execution.args,
                                **execution.kwargs,
                                **special_arguments,
                            )
                        except Exception:
                            logger.exception(
                                "Error executing task %s",
                                execution.key,
                                extra=log_context,
                            )
                            await self._retry_if_requested(execution, special_arguments)
                            total += 1  # TODO: hacky, this tells us we might have more

    def _get_special_arguments(
        self,
        execution: Execution,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}

        signature = inspect.signature(execution.function)

        for param_name, param in signature.parameters.items():
            # If the argument is already provided, skip it, which allows users to call
            # the function directly with the arguments they want.
            if param_name in execution.kwargs:
                continue

            if isinstance(param.default, Retry):
                retry_definition = param.default
                retry = Retry(
                    attempts=retry_definition.attempts,
                    delay=retry_definition.delay,
                )
                retry.attempt = execution.attempt

                kwargs[param_name] = retry

        return kwargs

    async def _retry_if_requested(
        self,
        execution: Execution,
        special_arguments: dict[str, Any],
    ) -> None:
        for retry in special_arguments.values():
            if isinstance(retry, Retry):
                if execution.attempt < retry.attempts:
                    execution.when += retry.delay
                    execution.attempt += 1
                    await self.docket.schedule(execution)
                else:
                    logger.error(
                        "Task %s failed after %d attempts",
                        execution.key,
                        retry.attempts,
                    )
