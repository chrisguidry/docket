"""Vendored OpenTelemetry Prometheus exporter.

This module provides a minimal implementation of PrometheusMetricReader
from opentelemetry-exporter-prometheus. Vendoring removes the exact SDK
version pins that the exporter package brings in as transitive dependencies.

Original source: opentelemetry-exporter-prometheus (Apache 2.0)
https://github.com/open-telemetry/opentelemetry-python-contrib

Copyright The OpenTelemetry Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from collections import deque
from itertools import chain
from json import dumps
from logging import getLogger
from re import UNICODE, compile
from typing import Deque, Dict, Iterable, Sequence, Tuple, Union

from prometheus_client.core import (
    REGISTRY,
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
    InfoMetricFamily,
)
from prometheus_client.core import Metric as PrometheusMetric

from opentelemetry.sdk.metrics import (
    Counter,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
)
from opentelemetry.sdk.metrics import Histogram as HistogramInstrument
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    Gauge,
    Histogram,
    HistogramDataPoint,
    MetricReader,
    MetricsData,
    Sum,
)
from opentelemetry.util.types import Attributes

_logger = getLogger(__name__)

_TARGET_INFO_NAME = "target"
_TARGET_INFO_DESCRIPTION = "Target metadata"

# Sanitization regexes
_SANITIZE_NAME_RE = compile(r"[^a-zA-Z0-9:]+", UNICODE)
_SANITIZE_ATTRIBUTE_KEY_RE = compile(r"[^a-zA-Z0-9]+", UNICODE)
_UNIT_ANNOTATION = compile(r"{.*}")

# Unit mappings from UCUM/SI to Prometheus conventions
_UNIT_MAPPINGS = {
    "d": "days",
    "h": "hours",
    "min": "minutes",
    "s": "seconds",
    "ms": "milliseconds",
    "us": "microseconds",
    "ns": "nanoseconds",
    "By": "bytes",
    "KiBy": "kibibytes",
    "MiBy": "mebibytes",
    "GiBy": "gibibytes",
    "TiBy": "tibibytes",
    "KBy": "kilobytes",
    "MBy": "megabytes",
    "GBy": "gigabytes",
    "TBy": "terabytes",
    "m": "meters",
    "V": "volts",
    "A": "amperes",
    "J": "joules",
    "W": "watts",
    "g": "grams",
    "Cel": "celsius",
    "Hz": "hertz",
    "1": "",
    "%": "percent",
}

_PER_UNIT_MAPPINGS = {
    "s": "second",
    "m": "minute",
    "h": "hour",
    "d": "day",
    "w": "week",
    "mo": "month",
    "y": "year",
}


def _sanitize_name(name: str) -> str:
    """Sanitize metric name according to Prometheus rules."""
    return _SANITIZE_NAME_RE.sub("_", name)


def sanitize_full_name(name: str) -> str:
    """Sanitize metric name including leading digits."""
    if name and name[0].isdigit():
        name = "_" + name[1:]
    return _sanitize_name(name)


def sanitize_attribute(key: str) -> str:
    """Sanitize attribute key according to Prometheus rules."""
    if key and key[0].isdigit():
        key = "_" + key[1:]
    return _SANITIZE_ATTRIBUTE_KEY_RE.sub("_", key)


def map_unit(unit: str) -> str:
    """Map unit to Prometheus metric name conventions."""
    unit = _UNIT_ANNOTATION.sub("", unit)

    if unit in _UNIT_MAPPINGS:
        return _UNIT_MAPPINGS[unit]

    ratio_unit_subparts = unit.split("/", maxsplit=1)
    if len(ratio_unit_subparts) == 2:
        bottom = _sanitize_name(ratio_unit_subparts[1])
        if bottom:
            top = _sanitize_name(ratio_unit_subparts[0])
            top = _UNIT_MAPPINGS.get(top, top)
            bottom = _PER_UNIT_MAPPINGS.get(bottom, bottom)
            return f"{top}_per_{bottom}" if top else f"per_{bottom}"

    return _sanitize_name(unit).strip("_")


def _convert_buckets(
    bucket_counts: Sequence[int], explicit_bounds: Sequence[float]
) -> Sequence[Tuple[str, int]]:
    """Convert histogram buckets to Prometheus format."""
    buckets = []
    total_count = 0
    for upper_bound, count in zip(
        chain(explicit_bounds, ["+Inf"]),
        bucket_counts,
    ):
        total_count += count
        buckets.append((f"{upper_bound}", total_count))
    return buckets


class PrometheusMetricReader(MetricReader):
    """Prometheus metric exporter for OpenTelemetry."""

    def __init__(self, disable_target_info: bool = False) -> None:
        super().__init__(
            preferred_temporality={
                Counter: AggregationTemporality.CUMULATIVE,
                UpDownCounter: AggregationTemporality.CUMULATIVE,
                HistogramInstrument: AggregationTemporality.CUMULATIVE,
                ObservableCounter: AggregationTemporality.CUMULATIVE,
                ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
                ObservableGauge: AggregationTemporality.CUMULATIVE,
            }
        )
        self._collector = _CustomCollector(disable_target_info)
        REGISTRY.register(self._collector)
        self._collector._callback = self.collect

    def _receive_metrics(
        self,
        metrics_data: MetricsData,
        timeout_millis: float = 10_000,
        **kwargs: object,
    ) -> None:
        if metrics_data is None:
            return
        self._collector.add_metrics_data(metrics_data)

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: object) -> None:
        REGISTRY.unregister(self._collector)


class _CustomCollector:
    """Custom Prometheus collector that bridges OTel metrics to Prometheus."""

    def __init__(self, disable_target_info: bool = False):
        self._callback: object = None
        self._metrics_datas: Deque[MetricsData] = deque()
        self._disable_target_info = disable_target_info
        self._target_info: InfoMetricFamily | None = None

    def add_metrics_data(self, metrics_data: MetricsData) -> None:
        """Add metrics to Prometheus data."""
        self._metrics_datas.append(metrics_data)

    def collect(self) -> Iterable[PrometheusMetric]:
        """Collect metrics for Prometheus exposition."""
        if self._callback is not None:
            self._callback()  # type: ignore[operator]

        metric_family_id_metric_family: Dict[str, PrometheusMetric] = {}

        if len(self._metrics_datas):
            if not self._disable_target_info:
                if self._target_info is None:
                    attributes: Attributes = {}
                    for res in self._metrics_datas[0].resource_metrics:
                        attributes = {**attributes, **res.resource.attributes}

                    self._target_info = self._create_info_metric(
                        _TARGET_INFO_NAME, _TARGET_INFO_DESCRIPTION, attributes
                    )
                metric_family_id_metric_family[_TARGET_INFO_NAME] = self._target_info

        while self._metrics_datas:
            self._translate_to_prometheus(
                self._metrics_datas.popleft(), metric_family_id_metric_family
            )

            if metric_family_id_metric_family:
                yield from metric_family_id_metric_family.values()

    def _translate_to_prometheus(
        self,
        metrics_data: MetricsData,
        metric_family_id_metric_family: Dict[str, PrometheusMetric],
    ) -> None:
        metrics = []

        for resource_metrics in metrics_data.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    metrics.append(metric)

        for metric in metrics:
            label_values_data_points = []
            label_keys_data_points = []
            values: list[object] = []

            per_metric_family_ids = []

            metric_name = sanitize_full_name(metric.name)
            metric_description = metric.description or ""
            metric_unit = map_unit(metric.unit)

            for number_data_point in metric.data.data_points:
                label_keys = []
                label_values = []

                for key, value in sorted(number_data_point.attributes.items()):
                    label_keys.append(sanitize_attribute(key))
                    label_values.append(self._check_value(value))

                per_metric_family_ids.append(
                    "|".join(
                        [
                            metric_name,
                            metric_description,
                            "%".join(label_keys),
                            metric_unit,
                        ]
                    )
                )

                label_values_data_points.append(label_values)
                label_keys_data_points.append(label_keys)
                if isinstance(number_data_point, HistogramDataPoint):
                    values.append(
                        {
                            "bucket_counts": number_data_point.bucket_counts,
                            "explicit_bounds": number_data_point.explicit_bounds,
                            "sum": number_data_point.sum,
                        }
                    )
                else:
                    values.append(number_data_point.value)

            for per_metric_family_id, label_keys, label_values, value in zip(
                per_metric_family_ids,
                label_keys_data_points,
                label_values_data_points,
                values,
            ):
                is_non_monotonic_sum = (
                    isinstance(metric.data, Sum) and metric.data.is_monotonic is False
                )
                is_cumulative = (
                    isinstance(metric.data, Sum)
                    and metric.data.aggregation_temporality
                    == AggregationTemporality.CUMULATIVE
                )
                should_convert_sum_to_gauge = is_non_monotonic_sum and is_cumulative

                if isinstance(metric.data, Sum) and not should_convert_sum_to_gauge:
                    metric_family_id = "|".join(
                        [per_metric_family_id, CounterMetricFamily.__name__]
                    )

                    if metric_family_id not in metric_family_id_metric_family:
                        metric_family_id_metric_family[metric_family_id] = (
                            CounterMetricFamily(
                                name=metric_name,
                                documentation=metric_description,
                                labels=label_keys,
                                unit=metric_unit,
                            )
                        )
                    metric_family_id_metric_family[metric_family_id].add_metric(
                        labels=label_values, value=value
                    )
                elif isinstance(metric.data, Gauge) or should_convert_sum_to_gauge:
                    metric_family_id = "|".join(
                        [per_metric_family_id, GaugeMetricFamily.__name__]
                    )

                    if metric_family_id not in metric_family_id_metric_family.keys():
                        metric_family_id_metric_family[metric_family_id] = (
                            GaugeMetricFamily(
                                name=metric_name,
                                documentation=metric_description,
                                labels=label_keys,
                                unit=metric_unit,
                            )
                        )
                    metric_family_id_metric_family[metric_family_id].add_metric(
                        labels=label_values, value=value
                    )
                elif isinstance(metric.data, Histogram):
                    metric_family_id = "|".join(
                        [per_metric_family_id, HistogramMetricFamily.__name__]
                    )

                    if metric_family_id not in metric_family_id_metric_family.keys():
                        metric_family_id_metric_family[metric_family_id] = (
                            HistogramMetricFamily(
                                name=metric_name,
                                documentation=metric_description,
                                labels=label_keys,
                                unit=metric_unit,
                            )
                        )
                    metric_family_id_metric_family[metric_family_id].add_metric(
                        labels=label_values,
                        buckets=_convert_buckets(
                            value["bucket_counts"],
                            value["explicit_bounds"],  # type: ignore[index]
                        ),
                        sum_value=value["sum"],  # type: ignore[index]
                    )
                else:
                    _logger.warning("Unsupported metric data. %s", type(metric.data))

    def _check_value(self, value: Union[int, float, str, Sequence[object]]) -> str:
        """Check the label value and return appropriate representation."""
        if not isinstance(value, str):
            return dumps(value, default=str)
        return str(value)

    def _create_info_metric(
        self, name: str, description: str, attributes: Attributes
    ) -> InfoMetricFamily:
        """Create an Info Metric Family with list of attributes."""
        sanitized_attributes = {
            sanitize_attribute(str(key)): self._check_value(value)
            for key, value in (attributes or {}).items()
        }
        info = InfoMetricFamily(name, description, labels=sanitized_attributes)
        info.add_metric(
            labels=list(sanitized_attributes.keys()), value=sanitized_attributes
        )
        return info
