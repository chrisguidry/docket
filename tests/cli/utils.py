import asyncio
import os
import sys
from typing import NamedTuple


class CliResult(NamedTuple):
    """Result from running a CLI command."""

    exit_code: int
    stdout: str
    stderr: str

    @property
    def output(self) -> str:
        """Combined stdout and stderr for compatibility with CliRunner.Result."""
        return self.stdout + self.stderr


async def run_cli(
    *args: str, env: dict[str, str] | None = None, timeout: float = 90.0
) -> CliResult:
    """Run `python -m docket ...` and capture output."""
    merged_env = {**os.environ, "PYTHONUNBUFFERED": "1"} | (env or {})

    same_python = sys.executable
    has_pytest_cov = any(k.startswith("COV_CORE_") for k in merged_env)

    if has_pytest_cov:
        argv = [same_python, "-m", "docket", *args]  # pragma: no cover
    else:
        # Try sitecustomize auto-start first
        merged_env.setdefault(
            "COVERAGE_PROCESS_START",
            "pyproject.toml"
            if os.environ.get("REDIS_VERSION") != "memory"
            else ".coveragerc-memory",
        )
        # Ensure *this repo* (where sitecustomize.py lives) is on PYTHONPATH
        repo_root = os.path.abspath(os.getcwd())
        merged_env["PYTHONPATH"] = os.pathsep.join(
            [repo_root, merged_env.get("PYTHONPATH", "")]
        )
        argv = [same_python, "-m", "docket", *args]

    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=merged_env,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    return CliResult(proc.returncode or 0, stdout.decode(), stderr.decode())
