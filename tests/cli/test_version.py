from packaging.version import Version

from docket import __version__
from tests.cli.run import run_cli


async def test_version_command():
    """Should print the current version of Docket."""
    result = await run_cli("version")
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


async def test_version_matches_semantic_versioning():
    """Should ensure the version follows semantic versioning format."""
    result = await run_cli("version")
    version = result.stdout.strip()

    parsed_version = Version(version)
    assert len(parsed_version.release) >= 2
