"""Every implementation emits the same metrics, traces, and Prometheus series.

Each implementation runs the telemetry workload twice, on its own docket:

1. In the ``otlp`` phase, its worker and producer export traces and metrics
   over OTLP/HTTP to a receiver in the driver.
2. In the ``prometheus`` phase, its worker serves Prometheus metrics, and the
   driver scrapes them.

The phases need separate workers, because pydocket's metrics server installs
the global meter provider, and OpenTelemetry for Python lets a process do
that only once.

The driver normalizes what it receives, compares it with the tables in
``telemetry.expected``, and, with more than one implementation, compares the
implementations with each other.  It writes each implementation's normalized
telemetry to a JSON file beside the agents' logs.
"""

import json
import logging
import signal
from collections import Counter
from uuid import uuid4

from ..harness import TERMINAL_STATES, Harness
from ..implementations import Implementation
from ..server import get_free_port
from ..telemetry import expected
from ..telemetry.compare import differences
from ..telemetry.metrics import otlp_metrics
from ..telemetry.names import Names
from ..telemetry.prometheus import prometheus_metrics, settled_scrape
from ..telemetry.receiver import receiving
from ..telemetry.spans import otlp_spans

logger = logging.getLogger(__name__)

TIMEOUT = 150

# The runs that the workload's tasks record, by task.
RUNS = {"succeed": 1, "fail": 1, "flaky": 2, "perpetual": 3, "later": 1}

# Short enough that the exporters send during the phase, not only at exit.
OTLP_ENVIRONMENT = {
    "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
    "OTEL_METRIC_EXPORT_INTERVAL": "500",
    "OTEL_BSP_SCHEDULE_DELAY": "200",
}

Telemetry = dict[str, object]


async def run(harness: Harness) -> None:
    observed: dict[str, Telemetry] = {}
    for implementation in harness.implementations:
        # Each implementation gets its own docket, so their keys, run states,
        # and strikes stay apart.
        harness.docket = f"conformance-{uuid4()}"
        telemetry = {
            **await otlp_phase(harness, implementation),
            **await prometheus_phase(harness, implementation),
        }
        saved = harness.workdir / f"telemetry-{implementation.name}.json"
        saved.write_text(json.dumps(telemetry, indent=2, sort_keys=True))
        logger.info("Wrote %s's telemetry to %s", implementation.name, saved)

        found = differences(expected.TELEMETRY, telemetry)
        assert not found, (
            f"{implementation.name}'s telemetry differs from the expected tables:\n"
            + "\n".join(found)
        )
        observed[implementation.name] = telemetry

    (first, reference), *others = observed.items()
    for name, telemetry in others:
        found = differences(reference, telemetry)
        assert not found, f"{name}'s telemetry differs from {first}'s:\n" + "\n".join(
            found
        )


async def otlp_phase(harness: Harness, implementation: Implementation) -> Telemetry:
    phase = "otlp"
    with receiving() as receiver:
        environment = {
            **OTLP_ENVIRONMENT,
            "OTEL_EXPORTER_OTLP_ENDPOINT": receiver.endpoint,
            "CONFORMANCE_PHASE": phase,
        }
        since = len(harness.events.seen)
        worker = await harness.start(
            "worker",
            implementation=implementation,
            env={**environment, "OTEL_SERVICE_NAME": "conformance-worker"},
        )
        await harness.produce(
            implementation=implementation,
            env={**environment, "OTEL_SERVICE_NAME": "conformance-producer"},
        )
        names = await finished(harness, phase, since)

        # The agents flush their exporters when they exit.
        harness.signal(worker, signal.SIGTERM)
        await harness.exited([worker], timeout=15)
        assert worker.process.returncode == 0, (
            f"The worker exited {worker.process.returncode} after SIGTERM"
        )

        return {
            "metrics": otlp_metrics(receiver.metrics, names),
            "spans": otlp_spans(receiver.traces, names),
        }


async def prometheus_phase(
    harness: Harness, implementation: Implementation
) -> Telemetry:
    phase = "prometheus"
    port = get_free_port()
    since = len(harness.events.seen)
    worker = await harness.start(
        "worker",
        implementation=implementation,
        env={"DOCKET_WORKER_METRICS_PORT": str(port), "CONFORMANCE_PHASE": phase},
    )
    await harness.produce(
        implementation=implementation, env={"CONFORMANCE_PHASE": phase}
    )
    names = await finished(harness, phase, since)
    text = await settled_scrape(port)

    harness.signal(worker, signal.SIGTERM)
    await harness.exited([worker], timeout=15)
    assert worker.process.returncode == 0, (
        f"The worker exited {worker.process.returncode} after SIGTERM"
    )
    return {"prometheus": prometheus_metrics(text, names)}


async def finished(harness: Harness, phase: str, since: int) -> Names:
    """Wait for the phase's runs and their final run states.

    Returns the names to scrub from the phase's telemetry.
    """

    def runs() -> Counter[str]:
        return Counter(event.task for event in harness.events.seen[since:])

    await harness.events.wait_for(
        lambda: runs() >= Counter(RUNS),
        timeout=30,
        waiting_for=f"the {phase} phase's runs, {RUNS}",
    )
    for task in RUNS:
        state = await harness.settled_state(f"{phase}:{task}")
        assert state in TERMINAL_STATES, f"{phase}:{task} ended {state}"

    assert runs() == Counter(RUNS), f"Expected the runs {RUNS}, saw {dict(runs())}"
    workers = {event.worker for event in harness.events.seen[since:]}
    return Names(harness.docket, frozenset(workers), phase)
