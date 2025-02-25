import subprocess
import sys


def test_module_invocation_as_cli_entrypoint():
    """Should allow invoking docket as a module with python -m docket."""
    result = subprocess.run(
        [sys.executable, "-m", "docket", "version"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    assert result.stdout.strip() != ""
