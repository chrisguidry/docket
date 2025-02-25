import pytest
from typer.testing import CliRunner


@pytest.fixture
def runner() -> CliRunner:
    """Provides a CLI runner for testing commands."""
    return CliRunner()
