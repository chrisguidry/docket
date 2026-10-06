"""Scrape and normalize the Prometheus metrics that a worker serves.

Each docket family keeps its type, its help text, and its samples.  A sample
is named with its label names and values, and keeps its value only when the
value does not depend on timing.
"""

import asyncio
import urllib.request

from prometheus_client.metrics_core import Metric
from prometheus_client.parser import text_string_to_metric_families

from .metrics import IGNORED, PRESENT, number
from .names import Names, labels

# Gauges whose values come from the heartbeat, at a moment the driver does
# not control.  docket_tasks_running and docket_strikes_in_effect are gauges
# here too, but up-down counters in OpenTelemetry, with values that settle.
TIMING_GAUGES = {"docket_queue_depth", "docket_schedule_depth"}

# A counter's creation time, a histogram's sum, and its buckets depend on the
# clock.  The bucket boundaries still show in the samples' labels.
TIMING_SUFFIXES = ("_created", "_sum", "_bucket")

Families = dict[str, dict[str, object]]


def sample_value(family: Metric, name: str, value: float) -> object:
    if family.name in TIMING_GAUGES or name.endswith(TIMING_SUFFIXES):
        return PRESENT
    return number(value)


def prometheus_metrics(text: str, names: Names) -> Families:
    families: Families = {}
    for family in text_string_to_metric_families(text):
        if family.name == "target_info":
            # The resource attributes differ by language and SDK.
            families[family.name] = {"type": family.type}
            continue
        if not family.name.startswith("docket_") or family.name in IGNORED:
            continue
        samples: dict[str, object] = {}
        for sample in family.samples:
            scrubbed = {key: names.scrub(value) for key, value in sample.labels.items()}
            samples[f"{sample.name}{{{labels(scrubbed)}}}"] = sample_value(
                family, sample.name, sample.value
            )
        families[family.name] = {
            "type": family.type,
            "help": family.documentation,
            "samples": dict(sorted(samples.items())),
        }
    return dict(sorted(families.items()))


def sample_total(text: str, sample_name: str) -> float:
    return sum(
        sample.value
        for family in text_string_to_metric_families(text)
        for sample in family.samples
        if sample.name == sample_name
    )


async def scrape(port: int) -> str:
    def get() -> str:
        url = f"http://127.0.0.1:{port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.read().decode()

    return await asyncio.to_thread(get)


async def settled_scrape(port: int, timeout: float = 10) -> str:
    """A scrape taken after every started run has completed.

    A task records its last event, and its run state, before its worker
    counts the run complete, so an early scrape can miss that count.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        text = await scrape(port)
        started = sample_total(text, "docket_tasks_started_total")
        completed = sample_total(text, "docket_tasks_completed_total")
        if started and started == completed:
            return text
        if loop.time() >= deadline:
            raise AssertionError(
                f"After {timeout} s, the worker counted {started} runs started "
                f"and {completed} completed"
            )
        await asyncio.sleep(0.2)
