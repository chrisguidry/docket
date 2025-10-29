"""Tests for TaskStateStore and related state management classes."""

from datetime import datetime, timezone


from docket import Docket, Worker
from docket.state import ProgressInfo, TaskState, TaskStateStore


class TestProgressInfo:
    """Tests for ProgressInfo dataclass."""

    def test_default_values(self):
        """ProgressInfo should have sensible defaults."""
        progress = ProgressInfo()
        assert progress.current == 0
        assert progress.total == 100

    def test_percentage_calculation(self):
        """ProgressInfo.percentage should calculate correctly."""
        progress = ProgressInfo(current=25, total=100)
        assert progress.percentage == 25.0

        progress = ProgressInfo(current=50, total=200)
        assert progress.percentage == 25.0

        progress = ProgressInfo(current=100, total=100)
        assert progress.percentage == 100.0

    def test_to_record(self):
        """ProgressInfo should serialize to dict."""
        progress = ProgressInfo(current=42, total=200)
        record = progress.to_record()

        assert record == {"current": 42, "total": 200}

    def test_from_record(self):
        """ProgressInfo should deserialize from dict."""
        record = {"current": 42, "total": 200}
        progress = ProgressInfo.from_record(record)

        assert progress.current == 42
        assert progress.total == 200

    def test_from_record_with_defaults(self):
        """ProgressInfo should use defaults for missing fields."""
        progress = ProgressInfo.from_record({})

        assert progress.current == 0
        assert progress.total == 100


class TestTaskState:
    """Tests for TaskState dataclass."""

    def test_to_records(self):
        """TaskState should serialize to tuple of dicts."""
        started = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        completed = datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc)
        state = TaskState(
            progress=ProgressInfo(current=100, total=100),
            started_at=started,
            completed_at=completed,
        )

        state_dict, progress_dict = state.to_records()

        assert state_dict == {
            "started_at": "2024-01-01T12:00:00+00:00",
            "completed_at": "2024-01-01T12:05:00+00:00",
        }
        assert progress_dict == {"current": 100, "total": 100}

    def test_to_records_without_completed_at(self):
        """TaskState should handle None completed_at."""
        started = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        state = TaskState(
            progress=ProgressInfo(current=50, total=100),
            started_at=started,
            completed_at=None,
        )

        state_dict, progress_dict = state.to_records()

        assert state_dict == {
            "started_at": "2024-01-01T12:00:00+00:00",
            "completed_at": None,
        }
        assert progress_dict == {"current": 50, "total": 100}

    def test_from_records(self):
        """TaskState should deserialize from separate dicts."""
        state_dict = {
            "started_at": "2024-01-01T12:00:00+00:00",
            "completed_at": "2024-01-01T12:05:00+00:00",
        }
        progress_dict = {"current": 100, "total": 100}

        state = TaskState.from_records(state_dict, progress_dict)

        assert state.progress.current == 100
        assert state.progress.total == 100
        assert state.started_at == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert state.completed_at == datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc)

    def test_from_records_without_completed_at(self):
        """TaskState should handle None completed_at in deserialization."""
        state_dict = {
            "started_at": "2024-01-01T12:00:00+00:00",
            "completed_at": None,
        }
        progress_dict = {"current": 50, "total": 100}

        state = TaskState.from_records(state_dict, progress_dict)

        assert state.completed_at is None


class TestTaskStateStore:
    """Tests for TaskStateStore Redis operations."""

    async def test_create_task_state(self, docket: Docket):
        """Creating task state should initialize with defaults."""
        store = TaskStateStore(docket, record_ttl=3600)

        before = datetime.now(timezone.utc)
        await store.create_task_state("test-task-key")
        after = datetime.now(timezone.utc)

        state = await store.get_task_state("test-task-key")
        assert state is not None
        assert state.progress.current == 0
        assert state.progress.total == 100
        assert before <= state.started_at <= after
        assert state.completed_at is None

    async def test_create_task_state_sets_ttl(self, docket: Docket):
        """Creating task state should set TTL on both keys."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")

        async with docket.redis() as redis:
            state_ttl = await redis.ttl(f"{docket.name}:state:test-task-key")
            progress_ttl = await redis.ttl(f"{docket.name}:progress:test-task-key")

        assert 0 < state_ttl <= 3600
        assert 0 < progress_ttl <= 3600

    async def test_set_task_progress(self, docket: Docket):
        """Setting task progress should update values."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")
        await store.set_task_progress(
            "test-task-key", ProgressInfo(current=50, total=200)
        )

        progress = await store.get_task_progress("test-task-key")
        assert progress is not None
        assert progress.current == 50
        assert progress.total == 200

    async def test_get_task_progress(self, docket: Docket):
        """Getting task progress should retrieve correct values."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")

        progress = await store.get_task_progress("test-task-key")
        assert progress is not None
        assert progress.current == 0
        assert progress.total == 100

    async def test_get_task_progress_nonexistent(self, docket: Docket):
        """Getting nonexistent progress should return None."""
        store = TaskStateStore(docket, record_ttl=3600)

        progress = await store.get_task_progress("nonexistent-key")
        assert progress is None

    async def test_get_task_state(self, docket: Docket):
        """Getting task state should retrieve complete state."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")

        state = await store.get_task_state("test-task-key")
        assert state is not None
        assert state.progress.current == 0
        assert state.progress.total == 100
        assert state.started_at is not None
        assert state.completed_at is None

    async def test_increment_task_progress(self, docket: Docket):
        """Incrementing progress should return new value."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")

        new_value = await store.increment_task_progress("test-task-key")
        assert new_value == 1

        progress = await store.get_task_progress("test-task-key")
        assert progress is not None
        assert progress.current == 1

    async def test_increment_task_progress_multiple(self, docket: Docket):
        """Multiple increments should accumulate correctly."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")

        result1 = await store.increment_task_progress("test-task-key")
        result2 = await store.increment_task_progress("test-task-key")
        result3 = await store.increment_task_progress("test-task-key")

        assert result1 == 1
        assert result2 == 2
        assert result3 == 3

    async def test_increment_task_progress_custom_amount(self, docket: Docket):
        """Incrementing by custom amount should work."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")

        result1 = await store.increment_task_progress("test-task-key", 5)
        result2 = await store.increment_task_progress("test-task-key", 3)

        assert result1 == 5
        assert result2 == 8

    async def test_mark_task_completed_atomic(self, docket: Docket):
        """Marking task completed should set progress and timestamp atomically."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")
        await store.set_task_progress(
            "test-task-key", ProgressInfo(current=50, total=200)
        )

        before = datetime.now(timezone.utc)
        await store.mark_task_completed("test-task-key")
        after = datetime.now(timezone.utc)

        state = await store.get_task_state("test-task-key")
        assert state is not None
        assert state.progress.current == 200
        assert state.progress.total == 200
        assert state.completed_at is not None
        assert before <= state.completed_at <= after

    async def test_get_task_state_nonexistent(self, docket: Docket):
        """Getting nonexistent task state should return None."""
        store = TaskStateStore(docket, record_ttl=3600)

        state = await store.get_task_state("nonexistent-key")
        assert state is None

    async def test_get_task_state_missing_state_key(self, docket: Docket):
        """Getting task state with missing state key should return None."""
        store = TaskStateStore(docket, record_ttl=3600)

        # Manually create only progress key
        progress_key = f"{docket.name}:progress:test-task-key"
        async with docket.redis() as redis:
            progress_dict = ProgressInfo().to_record()
            await redis.hset(  # pyright: ignore[reportUnknownMemberType,reportGeneralTypeIssues]
                progress_key, mapping={k: str(v) for k, v in progress_dict.items()}
            )

        state = await store.get_task_state("test-task-key")
        assert state is None

    async def test_get_task_state_missing_progress_key(self, docket: Docket):
        """Getting task state with missing progress key should return None."""
        store = TaskStateStore(docket, record_ttl=3600)

        # Manually create only state key (omit completed_at since it's None)
        state_key = f"{docket.name}:state:test-task-key"
        async with docket.redis() as redis:
            await redis.hset(  # pyright: ignore[reportUnknownMemberType,reportGeneralTypeIssues]
                state_key,
                mapping={
                    "started_at": datetime.now(timezone.utc).isoformat(),
                },
            )

        state = await store.get_task_state("test-task-key")
        assert state is None

    async def test_mark_task_completed_nonexistent(self, docket: Docket):
        """Marking nonexistent task as completed should not error."""
        store = TaskStateStore(docket, record_ttl=3600)

        # Should not raise an exception
        await store.mark_task_completed("nonexistent-key")

    async def test_mark_task_completed_missing_total(self, docket: Docket):
        """Marking task completed with missing total field should not error."""
        store = TaskStateStore(docket, record_ttl=3600)

        # Manually create progress key without total field (corrupted data)
        progress_key = f"{docket.name}:progress:test-task-key"
        async with docket.redis() as redis:
            await redis.hset(  # pyright: ignore[reportUnknownMemberType,reportGeneralTypeIssues]
                progress_key, mapping={"current": "50"}
            )

        # Should not raise an exception
        await store.mark_task_completed("test-task-key")

    async def test_mark_task_completed_idempotent(self, docket: Docket):
        """Marking task completed multiple times should be safe."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")
        await store.set_task_progress(
            "test-task-key", ProgressInfo(current=50, total=100)
        )

        await store.mark_task_completed("test-task-key")
        first_state = await store.get_task_state("test-task-key")

        # Mark completed again
        await store.mark_task_completed("test-task-key")
        second_state = await store.get_task_state("test-task-key")

        assert first_state is not None
        assert second_state is not None
        assert first_state.progress.current == 100
        assert second_state.progress.current == 100

    async def test_datetime_serialization_roundtrip(self, docket: Docket):
        """Datetime values should survive serialization roundtrip."""
        store = TaskStateStore(docket, record_ttl=3600)

        before = datetime.now(timezone.utc)
        await store.create_task_state("test-task-key")
        after = datetime.now(timezone.utc)

        state = await store.get_task_state("test-task-key")
        assert state is not None
        assert before <= state.started_at <= after
        assert state.started_at.tzinfo is not None

    async def test_completed_at_serialization(self, docket: Docket):
        """completed_at should be ISO format after completion."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")
        await store.mark_task_completed("test-task-key")

        state = await store.get_task_state("test-task-key")
        assert state is not None
        assert state.completed_at is not None
        assert state.completed_at.tzinfo is not None

        # Verify ISO format in Redis
        async with docket.redis() as redis:
            state_data = await redis.hgetall(  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportGeneralTypeIssues]
                f"{docket.name}:state:test-task-key"
            )
            completed_at_bytes = state_data.get(b"completed_at")  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
            assert completed_at_bytes is not None
            assert isinstance(completed_at_bytes, bytes)
            completed_at_str = completed_at_bytes.decode()
            # Should be valid ISO format (no exception)
            datetime.fromisoformat(completed_at_str)

    async def test_state_and_progress_keys_separate(self, docket: Docket):
        """State and progress should be stored in separate Redis keys."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")

        state_key = f"{docket.name}:state:test-task-key"
        progress_key = f"{docket.name}:progress:test-task-key"

        async with docket.redis() as redis:
            state_exists = await redis.exists(state_key)
            progress_exists = await redis.exists(progress_key)

            state_data = await redis.hgetall(state_key)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportGeneralTypeIssues]
            progress_data = await redis.hgetall(progress_key)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportGeneralTypeIssues]

        assert state_exists == 1
        assert progress_exists == 1

        # State key should have timestamps
        # (completed_at is omitted when None, so we don't check for it)
        assert b"started_at" in state_data

        # Progress key should have counters
        assert b"current" in progress_data
        assert b"total" in progress_data

    async def test_state_key_format(self, docket: Docket):
        """State key should have correct format."""
        store = TaskStateStore(docket, record_ttl=3600)

        key = store._state_key("my-task")  # pyright: ignore[reportPrivateUsage]
        assert key == f"{docket.name}:state:my-task"

    async def test_progress_key_format(self, docket: Docket):
        """Progress key should have correct format."""
        store = TaskStateStore(docket, record_ttl=3600)

        key = store._progress_key("my-task")  # pyright: ignore[reportPrivateUsage]
        assert key == f"{docket.name}:progress:my-task"

    async def test_custom_progress_total(self, docket: Docket):
        """Should support custom progress total values."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")
        await store.set_task_progress(
            "test-task-key", ProgressInfo(current=0, total=500)
        )

        progress = await store.get_task_progress("test-task-key")
        assert progress is not None
        assert progress.total == 500

    async def test_mark_completed_with_custom_total(self, docket: Docket):
        """Marking completed should work with custom total values."""
        store = TaskStateStore(docket, record_ttl=3600)

        await store.create_task_state("test-task-key")
        await store.set_task_progress(
            "test-task-key", ProgressInfo(current=250, total=500)
        )

        await store.mark_task_completed("test-task-key")

        progress = await store.get_task_progress("test-task-key")
        assert progress is not None
        assert progress.current == 500
        assert progress.total == 500
        assert progress.percentage == 100.0


class TestWorkerStateIntegration:
    """Tests for worker integration with task state."""

    async def test_worker_creates_state_before_execution(
        self, docket: Docket, worker: Worker
    ):
        """Worker should create task state record before starting execution."""
        task_started = False
        state_checked = False

        async def tracked_task():
            nonlocal task_started, state_checked
            task_started = True

            # Verify state was created before task execution
            store = TaskStateStore(docket, docket.record_ttl)
            state = await store.get_task_state("tracked-task")

            assert state is not None, "Task state should exist during execution"
            assert state.progress.current == 0
            assert state.progress.total == 100
            assert state.started_at is not None
            assert state.completed_at is None

            state_checked = True

        docket.register(tracked_task)
        await docket.add(tracked_task, key="tracked-task")()

        await worker.run_until_finished()

        assert task_started, "Task should have been executed"
        assert state_checked, "State should have been checked during execution"

        # Verify state was marked complete after execution
        store = TaskStateStore(docket, docket.record_ttl)
        final_state = await store.get_task_state("tracked-task")
        assert final_state is not None
        assert final_state.completed_at is not None
        assert final_state.progress.current == final_state.progress.total
