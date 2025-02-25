import pytest
from packaging.version import Version
from typer.testing import CliRunner

from docket import __version__
from docket.cli import app


@pytest.fixture
def runner() -> CliRunner:
    """Provides a CLI runner for testing commands."""
    return CliRunner()


def test_version_command(runner: CliRunner):
    """Should print the current version of Docket."""
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_version_matches_semantic_versioning(runner: CliRunner):
    """Should ensure the version follows semantic versioning format."""
    result = runner.invoke(app, ["version"])
    version = result.stdout.strip()

    parsed_version = Version(version)
    assert len(parsed_version.release) >= 2
