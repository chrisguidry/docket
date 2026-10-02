"""Run one scenario against one or more docket implementations.

    python -m conformance SCENARIO [--implementation python@main ...]

Each scenario starts a fresh Redis server, starts agents from the given
implementations, and makes its assertions on the events that the agents'
tasks write.  The command exits 1 when an assertion fails.
"""

import argparse
import asyncio
import logging
import sys
import tempfile
from pathlib import Path

from .harness import Harness
from .implementations import prepare
from .scenarios import SCENARIOS
from .server import run_redis

logger = logging.getLogger("conformance")


async def main(scenario: str, implementation_names: list[str], redis: str) -> bool:
    module = SCENARIOS[scenario]
    workdir = Path(tempfile.mkdtemp(prefix=f"docket-conformance-{scenario}-"))
    implementations = [await prepare(name, workdir) for name in implementation_names]

    async with run_redis(redis) as (url, container):
        harness = Harness(scenario, implementations, url, container, workdir)
        names = ", ".join(i.name for i in implementations)
        logger.info("Running %s on %s with Redis %s", scenario, names, redis)
        try:
            await asyncio.wait_for(module.run(harness), module.TIMEOUT)
        except (AssertionError, asyncio.TimeoutError) as error:
            logger.error(
                "%s failed on %s: %s\n%s",
                scenario,
                names,
                str(error) or f"timed out after {module.TIMEOUT} s",
                harness.report(),
            )
            return False
        finally:
            await harness.stop()

    logger.info("%s passed on %s", scenario, names)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="python -m conformance")
    parser.add_argument("scenario", choices=list(SCENARIOS))
    parser.add_argument(
        "--implementation",
        action="append",
        help="language@version, such as python@main or python@release; "
        "give it more than once to mix implementations",
    )
    parser.add_argument("--redis", default="8.10", help="the redis image tag")
    arguments = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    passed = asyncio.run(
        main(
            arguments.scenario,
            arguments.implementation or ["python@main"],
            arguments.redis,
        )
    )
    sys.exit(0 if passed else 1)
