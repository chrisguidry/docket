"""pydocket's agent for the conformance driver in conformance/.

    python conformance-agent produce|worker --scenario NAME --url URL --docket NAME

Every language ships an agent with this command line.  ``produce`` schedules
the scenario's tasks and exits.  ``worker`` runs a worker for the scenario's
tasks until a signal stops it.  The scenario's tasks record what happens to
them as events, and the driver makes its assertions on those events.

The agent uses only pydocket's public interface, so the driver can run this
same file against a released pydocket.
"""

import argparse
import asyncio
import importlib
import logging
import os
from types import ModuleType

from docket import Docket, Worker
from exporters import exporting


def scenario_module(scenario: str) -> ModuleType:
    return importlib.import_module(f"scenarios.{scenario.replace('-', '_')}")


async def produce(scenario: str, url: str, docket_name: str) -> None:
    async with Docket(name=docket_name, url=url) as docket:
        await scenario_module(scenario).produce(docket)


async def work(scenario: str, url: str, docket_name: str) -> None:
    module = scenario_module(scenario)
    # `docket worker` reads this variable, and Worker.run does not.
    metrics_port = os.environ.get("DOCKET_WORKER_METRICS_PORT")
    # Worker.run is what `docket worker` runs, so the worker stops on SIGTERM
    # and SIGINT the way a deployed one does.
    await Worker.run(
        docket_name=docket_name,
        url=url,
        tasks=[f"{module.__name__}:tasks"],
        metrics_port=int(metrics_port) if metrics_port else None,
        **module.WORKER,
    )


def main() -> None:
    parser = argparse.ArgumentParser(prog="conformance-agent")
    parser.add_argument("role", choices=["produce", "worker"])
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--docket", required=True)
    arguments = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    role = produce if arguments.role == "produce" else work
    with exporting():
        asyncio.run(role(arguments.scenario, arguments.url, arguments.docket))


if __name__ == "__main__":
    main()
