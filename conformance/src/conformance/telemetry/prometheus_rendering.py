"""How the Prometheus exporter renders the worker's metrics.

The Prometheus phase runs the same workload as the OTLP phase, so the driver
expects the worker's OTLP metrics, rendered by these rules:

- A counter is the family ``NAME`` with the sample ``NAME_total``.  pydocket
  writes no ``NAME_created`` sample.
- An up-down counter and a gauge are gauges, with the sample ``NAME``.
- A histogram is the family ``NAME_UNIT``, with ``_bucket`` samples for the
  default boundaries, and ``_count`` and ``_sum`` samples.  The unit ``s`` is
  ``seconds``.
- A label name is the attribute key with each run of characters other than
  letters and digits replaced by ``_``.
"""

import re
from typing import cast

from .metrics import PRESENT
from .names import labels

UNITS = {"s": "seconds"}
TYPES = {
    "counter": "counter",
    "up-down counter": "gauge",
    "gauge": "gauge",
    "histogram": "histogram",
}

# OpenTelemetry's default explicit bucket boundaries, as Prometheus labels.
BUCKETS = [
    "0.0",
    "5.0",
    "10.0",
    "25.0",
    "50.0",
    "75.0",
    "100.0",
    "250.0",
    "500.0",
    "750.0",
    "1000.0",
    "2500.0",
    "5000.0",
    "7500.0",
    "10000.0",
    "+Inf",
]


def label_name(key: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", key)


def relabel(point: str) -> dict[str, str]:
    """An expected data point's attributes, as Prometheus labels."""
    pairs = [pair.split("=", 1) for pair in point.split(", ") if pair]
    return {label_name(key): value for key, value in pairs}


def family(name: str, metric: dict[str, object]) -> tuple[str, dict[str, object]]:
    kind = str(metric["kind"])
    points = cast(dict[str, object], metric["points"])
    samples: dict[str, object] = {}

    if kind == "histogram":
        name = f"{name}_{UNITS[str(metric['unit'])]}"
    for point, value in points.items():
        point_labels = relabel(point)
        if kind == "counter":
            samples[f"{name}_total{{{labels(point_labels)}}}"] = value
        elif kind == "histogram":
            for bucket in BUCKETS:
                bucket_labels = labels({**point_labels, "le": bucket})
                samples[f"{name}_bucket{{{bucket_labels}}}"] = PRESENT
            samples[f"{name}_count{{{labels(point_labels)}}}"] = value
            samples[f"{name}_sum{{{labels(point_labels)}}}"] = PRESENT
        else:
            samples[f"{name}{{{labels(point_labels)}}}"] = value

    return name, {
        "type": TYPES[kind],
        "help": metric["description"],
        "samples": dict(sorted(samples.items())),
    }


def prometheus(metrics: dict[str, dict[str, object]]) -> dict[str, object]:
    families: dict[str, object] = {"target_info": {"type": "gauge"}}
    for name, metric in metrics.items():
        family_name, rendered = family(name, metric)
        families[family_name] = rendered
    return dict(sorted(families.items()))
