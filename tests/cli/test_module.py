import asyncio
import subprocess
import sys

import docket


async def test_module_invocation_as_cli_entrypoint():
    """Should allow invoking docket as a module with python -m docket."""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "docket",
        "version",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout, stderr = await process.communicate()

    assert process.returncode == 0, stderr.decode()
    assert stdout.decode().strip() == docket.__version__
