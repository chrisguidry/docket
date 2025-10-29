from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from redis.exceptions import NoScriptError

if TYPE_CHECKING:
    from docket import Docket


@dataclass
class ProgressInfo:
    """Information about task progress.

    Attributes:
        current: Current progress value
        total: Total expected progress value
    """

    current: int = field(default=0)
    total: int = field(default=100)

    @property
    def percentage(self) -> float:
        return self.current / self.total * 100

    def to_record(self) -> dict[str, int]:
        return {
            "current": self.current,
            "total": self.total,
        }

    @classmethod
    def from_record(cls, record: dict[str, int]) -> "ProgressInfo":
        return cls(
            current=record.get("current", 0),
            total=record.get("total", 100),
        )


@dataclass
class TaskState:
    """Information about task state.

    Attributes:
        current: Current progress value
        total: Total expected progress value, or None if not set
    """

    progress: ProgressInfo
    started_at: datetime
    completed_at: datetime | None

    def to_records(self) -> tuple[dict[str, Any], dict[str, int]]:
        return (
            {
                "started_at": self.started_at.isoformat(),
                "completed_at": self.completed_at.isoformat()
                if self.completed_at
                else None,
            },
            self.progress.to_record(),
        )

    @classmethod
    def from_records(
        cls, state_record: dict[str, Any], progress_record: dict[str, int]
    ) -> "TaskState":
        """Reconstruct TaskState from separate state and progress dicts.

        Args:
            state_record: Dictionary with started_at and completed_at
            progress_record: Dictionary with current and total

        Returns:
            TaskState instance
        """
        return cls(
            progress=ProgressInfo.from_record(progress_record),
            started_at=datetime.fromisoformat(state_record["started_at"]),
            completed_at=datetime.fromisoformat(state_record["completed_at"])
            if state_record.get("completed_at")
            else None,
        )


class TaskStateStore:
    """Manages task state storage in Redis."""

    # Lua script for atomic task completion
    _COMPLETION_SCRIPT = """
    local progress_key = KEYS[1]
    local state_key = KEYS[2]
    local completed_at = ARGV[1]
    local ttl = tonumber(ARGV[2])

    -- Check if progress key exists
    if redis.call('EXISTS', progress_key) == 0 then
        return 0
    end

    -- Get total value
    local total = redis.call('HGET', progress_key, 'total')
    if not total then
        return 0
    end

    -- Set current = total
    redis.call('HSET', progress_key, 'current', total)

    -- Set completed_at timestamp
    redis.call('HSET', state_key, 'completed_at', completed_at)

    -- Update TTLs
    redis.call('EXPIRE', progress_key, ttl)
    redis.call('EXPIRE', state_key, ttl)

    return 1
    """

    # Cached script SHA (class variable shared across instances)
    _completion_script_sha: str | None = None

    def __init__(self, docket: "Docket", record_ttl: int) -> None:
        """
        Args:
            docket: Docket instance
            record_ttl: Time-to-live in seconds for progress records
        """
        self.docket = docket
        self.record_ttl = record_ttl

    def _state_key(self, key: str) -> str:
        """Generate Redis key for task progress."""
        return f"{self.docket.name}:state:{key}"

    def _progress_key(self, key: str) -> str:
        """Generate Redis key for task progress."""
        return f"{self.docket.name}:progress:{key}"

    async def create_task_state(self, key: str) -> None:
        """Create a task state for a task.

        Args:
            key: Task key
        """
        state_key = self._state_key(key)
        progress_key = self._progress_key(key)

        # Create initial task state with default progress
        task_state = TaskState(
            progress=ProgressInfo(),
            started_at=datetime.now(timezone.utc),
            completed_at=None,
        )

        # Destructure the tuple returned by to_records()
        state_dict, progress_dict = task_state.to_records()

        # Convert integer values to strings for Redis
        progress_dict_str = {k: str(v) for k, v in progress_dict.items()}

        # Filter out None values from state_dict (Redis doesn't accept None)
        state_dict_filtered = {k: v for k, v in state_dict.items() if v is not None}

        async with self.docket.redis() as redis:
            await redis.hset(progress_key, mapping=progress_dict_str)  # pyright: ignore[reportGeneralTypeIssues,reportUnknownMemberType]
            await redis.expire(progress_key, self.record_ttl)
            await redis.hset(state_key, mapping=state_dict_filtered)  # pyright: ignore[reportGeneralTypeIssues,reportUnknownMemberType]
            await redis.expire(state_key, self.record_ttl)

    async def set_task_progress(self, key: str, progress: ProgressInfo) -> None:
        """Set progress for a task.

        Args:
            key: Task key
            progress: Progress information
        """
        progress_key = self._progress_key(key)
        # Convert integer values to strings for Redis
        progress_dict = progress.to_record()
        progress_dict_str = {k: str(v) for k, v in progress_dict.items()}
        async with self.docket.redis() as redis:
            await redis.hset(progress_key, mapping=progress_dict_str)  # pyright: ignore[reportGeneralTypeIssues,reportUnknownMemberType]

    async def increment_task_progress(self, key: str, amount: int = 1) -> int:
        """Atomically increment progress for a task.

        Args:
            key: Task key
            amount: Amount to increment by

        Returns:
            New current value after increment
        """
        progress_key = self._progress_key(key)

        async with self.docket.redis() as redis:
            return int(await redis.hincrby(progress_key, "current", amount))  # pyright: ignore[reportUnknownMemberType,reportGeneralTypeIssues,reportUnknownArgumentType]

    async def get_task_progress(self, key: str) -> ProgressInfo | None:
        """Retrieve progress information for a task.

        Args:
            key: Task key

        Returns:
            ProgressInfo if progress exists, None otherwise
        """
        progress_key = self._progress_key(key)
        async with self.docket.redis() as redis:
            data = cast(dict[bytes, bytes], await redis.hgetall(progress_key))  # pyright: ignore[reportUnknownMemberType,reportGeneralTypeIssues]

        if not data:
            return None

        return ProgressInfo(
            current=int(data.get(b"current", b"0")),
            total=int(data.get(b"total", b"100")),
        )

    async def get_task_state(self, key: str) -> TaskState | None:
        """Retrieve complete task state.

        Args:
            key: Task key

        Returns:
            TaskState if state exists, None otherwise
        """
        state_key = self._state_key(key)
        progress_key = self._progress_key(key)

        async with self.docket.redis() as redis:
            state_data = cast(
                dict[bytes | str, bytes | str],
                await redis.hgetall(state_key),  # pyright: ignore[reportUnknownMemberType,reportGeneralTypeIssues]
            )
            progress_data = cast(
                dict[bytes | str, bytes | str],
                await redis.hgetall(progress_key),  # pyright: ignore[reportUnknownMemberType,reportGeneralTypeIssues]
            )

        if not state_data or not progress_data:
            return None

        # Convert bytes keys to strings for the from_records method
        state_dict = {
            k.decode() if isinstance(k, bytes) else k: v.decode()
            if isinstance(v, bytes)
            else v
            for k, v in state_data.items()
        }
        progress_dict = cast(
            dict[str, int],
            {
                k.decode() if isinstance(k, bytes) else k: int(v)
                if isinstance(v, bytes)
                else v
                for k, v in progress_data.items()
            },
        )

        return TaskState.from_records(state_dict, progress_dict)

    async def mark_task_completed(self, key: str) -> None:
        """Mark task as completed atomically using registered Lua script.

        Atomically updates both progress (current=total) and state (completed_at)
        using a pre-registered Lua script that executes on the Redis server.

        Args:
            key: Task key
        """
        progress_key = self._progress_key(key)
        state_key = self._state_key(key)
        now = datetime.now(timezone.utc).isoformat()

        async with self.docket.redis() as redis:
            # Load script if not already cached
            if TaskStateStore._completion_script_sha is None:
                TaskStateStore._completion_script_sha = cast(
                    str,
                    await redis.script_load(self._COMPLETION_SCRIPT),  # pyright: ignore[reportUnknownMemberType,reportGeneralTypeIssues]
                )

            try:
                # Execute using cached SHA
                await redis.evalsha(  # pyright: ignore[reportUnknownMemberType,reportGeneralTypeIssues]
                    TaskStateStore._completion_script_sha,
                    2,  # number of keys
                    progress_key,
                    state_key,
                    now,
                    self.record_ttl,
                )
            except NoScriptError:
                TaskStateStore._completion_script_sha = cast(
                    str,
                    await redis.script_load(self._COMPLETION_SCRIPT),  # pyright: ignore[reportUnknownMemberType,reportGeneralTypeIssues]
                )
                await redis.evalsha(  # pyright: ignore[reportUnknownMemberType,reportGeneralTypeIssues]
                    TaskStateStore._completion_script_sha,
                    2,  # number of keys
                    progress_key,
                    state_key,
                    now,
                    self.record_ttl,
                )
