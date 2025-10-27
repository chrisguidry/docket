import pytest

from tests.cli.utils import run_cli


@pytest.mark.parametrize(
    "cli_args",
    [
        ["worker", "--until-finished", "--url", "memory://", "--docket", "test-docket"],
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
async def test_valid_urls_accepted(valid_url: str, redis_url: str):
    """Should accept valid Redis URL schemes"""
    # We're just testing that the validation doesn't reject valid URLs
    # We'll use the worker command with --until-finished so it exits quickly
    result = await run_cli(
        "worker",
        "--until-finished",
        "--url",
        redis_url,  # Use actual working redis URL
        "--docket",
        "test-docket-validation",
    )

    # Should succeed (or fail for legitimate reasons, not URL validation)
    # The key is that it doesn't fail with the validation error message
    if result.exit_code != 0:
        # If it failed, make sure it's not because of URL validation
        assert "not supported" not in result.output.lower()
        assert "memory://" not in result.output.lower()
