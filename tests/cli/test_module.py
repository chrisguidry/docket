import docket
from tests.cli.run import run_cli


async def test_module_invocation_as_cli_entrypoint():
    """Should allow invoking docket as a module with python -m docket."""
    result = await run_cli("version")

    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip() == docket.__version__
