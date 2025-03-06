import asyncio

from typer.testing import CliRunner

from docket.cli import app


async def test_module_invocation_as_cli_entrypoint(runner: CliRunner):
    """Should allow invoking docket as a module with python -m docket."""
    result = await asyncio.get_running_loop().run_in_executor(
        None,
        runner.invoke,
        app,
        ["version"],
    )

    assert result.exit_code == 0
    assert result.output.strip() != ""
