import inspect
import logging
import sys
from datetime import datetime, timezone
from types import TracebackType
from typing import Protocol, Self, Sequence, cast
from uuid import uuid4

from redis import RedisError

from .docket import Docket, Execution, Modifier, Retry

logger: logging.Logger = logging.getLogger(__name__)

RedisStreamID = bytes
RedisMessageID = bytes
RedisMessage = dict[bytes, bytes]
RedisStream = tuple[RedisStreamID, Sequence[tuple[RedisMessageID, RedisMessage]]]
RedisReadGroupResponse = Sequence[RedisStream]


class _stream_due_tasks(Protocol):
    async def __call__(
        self, keys: list[str], args: list[str | float]
    ) -> tuple[int, int]: ...  # pragma: no cover


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
                    groupname=self.consumer_group_name,
                    name=self.docket.stream_key,
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
    def consumer_group_name(self) -> str:
        return "docket"

    @property
    def _log_context(self) -> dict[str, str]:
        return {
            "queue_key": self.docket.queue_key,
            "stream_key": self.docket.stream_key,
        }

    async def run_until_current(self) -> None:
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
                local total_work = redis.call('ZCARD', KEYS[1])
                local due_work = 0
                local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])

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
                        due_work = due_work + 1
                    end
                end

                if due_work > 0 then
                    redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
                end

                return {total_work, due_work}
                """
                ),
            )

            total_work, due_work = sys.maxsize, 0
            while total_work:
                now = datetime.now(timezone.utc)
                total_work, due_work = await stream_due_tasks(
                    keys=[self.docket.queue_key, self.docket.stream_key],
                    args=[now.timestamp(), self.docket.name],
                )
                logger.info(
                    "Moved %d/%d due tasks from %s to %s",
                    due_work,
                    total_work,
                    self.docket.queue_key,
                    self.docket.stream_key,
                    extra=self._log_context,
                )

                response: RedisReadGroupResponse = await redis.xreadgroup(
                    groupname=self.consumer_group_name,
                    consumername=self.name,
                    streams={self.docket.stream_key: ">"},
                    block=10,
                )

                for _, messages in response:
                    for _, message in messages:
                        await self._execute(message)

                        # When executing a task, there's always a chance that it was
                        # either retried or it scheduled another task, so let's give
                        # ourselves one more iteration of the loop to handle that.
                        total_work += 1

    async def _execute(self, message: RedisMessage) -> None:
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
                **self._log_context,
                "function": execution.function.__name__,
            },
        )

        modifiers = self._get_modifiers(execution)

        try:
            await execution.function(
                *execution.args,
                **execution.kwargs,
                **modifiers,
            )
        except Exception:
            logger.exception(
                "Error executing task %s",
                execution.key,
                extra=self._log_context,
            )
            await self._retry_if_requested(execution, modifiers)

    def _get_modifiers(
        self,
        execution: Execution,
    ) -> dict[str, Modifier]:
        modifiers: dict[str, Modifier] = {}

        signature = inspect.signature(execution.function)

        for param_name, param in signature.parameters.items():
            # If the argument is already provided, skip it, which allows users to call
            # the function directly with the arguments they want.
            if param_name in execution.kwargs:
                continue

            if not isinstance(param.default, Modifier):
                continue

            if isinstance(param.default, Retry):  # pragma: no branch
                retry_definition = param.default
                retry = Retry(
                    attempts=retry_definition.attempts,
                    delay=retry_definition.delay,
                )
                retry.attempt = execution.attempt

                modifiers[param_name] = retry

        return modifiers

    async def _retry_if_requested(
        self,
        execution: Execution,
        modifiers: dict[str, Modifier],
    ) -> bool:
        for modifier in modifiers.values():
            if not isinstance(modifier, Retry):
                continue  # pragma: no cover

            retry = modifier

            if execution.attempt < retry.attempts:
                execution.when = datetime.now(timezone.utc) + retry.delay
                execution.attempt += 1
                await self.docket.schedule(execution)
                return True
            else:
                logger.error(
                    "Task %s failed after %d attempts",
                    execution.key,
                    retry.attempts,
                )

        return False
