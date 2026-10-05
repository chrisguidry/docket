"""The docket implementations that the driver can run its agents from.

An implementation is named ``language@version``.  For Python, ``main`` is the
working tree, ``release`` is the newest pydocket tag, and anything else is a
version on PyPI.  Each one gets its own virtual environment, and all of them
run the agent from the working tree.
"""

import asyncio
import logging
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError

logger = logging.getLogger(__name__)

REPOSITORY = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class Implementation:
    name: str
    agent: list[str]


async def prepare(name: str, workdir: Path) -> Implementation:
    language, _, version = name.partition("@")
    if language != "python":
        raise SystemExit(f"There is no {language} implementation yet")
    return await prepare_python(version or "main", workdir)


async def prepare_python(version: str, workdir: Path) -> Implementation:
    if version == "release":
        version = await newest_pydocket_tag()

    if version == "main":
        source = ["--editable", str(REPOSITORY / "python")]
    elif on_pypi(version):
        source = [f"pydocket=={version}"]
    else:
        raise SystemExit(f"pydocket {version} is not on PyPI yet")

    venv = workdir / f"python-{version}"
    python = str(venv / "bin" / "python")
    logger.info("Installing pydocket %s into %s", version, venv)
    await run("uv", "venv", "--quiet", str(venv), "--python", sys.executable)
    await run("uv", "pip", "install", "--quiet", "--python", python, *source)

    agent = str(REPOSITORY / "python" / "conformance-agent")
    return Implementation(f"python@{version}", [python, agent])


async def newest_pydocket_tag() -> str:
    tag = await run(
        "git",
        "describe",
        "--tags",
        "--abbrev=0",
        # pydocket's releases are tagged python/v0.27.0, or bare before the
        # monorepo; other languages' tags are not pydocket versions.
        "--match",
        "python/v*",
        "--match",
        "[0-9]*",
    )
    return tag.removeprefix("python/v")


def on_pypi(version: str) -> bool:
    url = f"https://pypi.org/pypi/pydocket/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.status == 200
    except HTTPError as error:
        if error.code == 404:
            return False
        raise


async def run(*command: str) -> str:
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=REPOSITORY,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await process.communicate()
    if process.returncode != 0:
        raise SystemExit(f"{' '.join(command)} failed:\n{output.decode()}")
    return output.decode().strip()
