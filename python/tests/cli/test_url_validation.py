import pytest

from tests.cli.run import run_cli


@pytest.mark.parametrize(
    "cli_args",
    [
        ["strike", "--url", "memory://", "--docket", "test-docket", "example_task"],
        ["clear", "--url", "memory://", "--docket", "test-docket"],
        ["restore", "--url", "memory://", "--docket", "test-docket", "example_task"],
        ["tasks", "trace", "--url", "memory://", "--docket", "test-docket", "hello"],
        ["tasks", "fail", "--url", "memory://", "--docket", "test-docket", "test"],
        ["tasks", "sleep", "--url", "memory://", "--docket", "test-docket", "1"],
        ["snapshot", "--url", "memory://", "--docket", "test-docket"],
        ["workers", "ls", "--url", "memory://", "--docket", "test-docket"],
        [
            "workers",
            "for-task",
            "--url",
            "memory://",
            "--docket",
            "test-docket",
            "trace",
        ],
    ],
)
async def test_memory_url_rejected(cli_args: list[str]):
    """Should reject memory:// URLs with a clear error message"""
    result = await run_cli(*cli_args)

    # Should fail with non-zero exit code
    assert result.exit_code != 0, f"Expected non-zero exit code for {cli_args[0]}"

    # Should contain helpful error message
    assert "memory://" in result.output.lower()
    assert "not supported" in result.output.lower() or "error" in result.output.lower()

    # Should mention Redis as an alternative
    assert "redis" in result.output.lower()


@pytest.mark.parametrize(
    "valid_url",
    [
        "redis://localhost:6379/0",
        "redis://user:pass@host:6379/1",
        "rediss://secure.example.com:6380/0",
        "unix:///var/run/redis.sock",
    ],
)
def test_valid_urls_accepted(valid_url: str):
    """Should accept valid Redis URL schemes without raising validation errors"""
    from docket.cli import validate_url

    # Should not raise any exceptions - just testing validation logic
    result = validate_url(valid_url)
    assert result == valid_url


async def test_worker_accepts_memory_url():
    """Worker command should accept memory:// URLs for single-process services"""
    result = await run_cli(
        "worker",
        "--until-finished",
        "--url",
        "memory://",
        "--docket",
        "test-memory-worker",
    )

    # Should NOT fail with URL validation error
    assert "not supported" not in result.output.lower()
    # Exit code 0 means worker ran successfully (no tasks = immediate exit with --until-finished)
    assert result.exit_code == 0
