# Vendored OpenTelemetry Prometheus Exporter

This directory contains a vendored copy of the OpenTelemetry Prometheus exporter
from the [opentelemetry-python](https://github.com/open-telemetry/opentelemetry-python)
project.

## Why Vendor?

The `opentelemetry-exporter-prometheus` package is currently only available as a
pre-release version (e.g., `>=0.51b0`). Some package managers and dependency
resolvers refuse to install pre-release packages by default, causing installation
failures for docket users. See [#226](https://github.com/chrisguidry/docket/issues/226).

By vendoring this code, we can provide Prometheus metrics export functionality
without requiring users to opt-in to pre-release dependencies.

## Source

- **Repository**: https://github.com/open-telemetry/opentelemetry-python
- **Path**: `exporter/opentelemetry-exporter-prometheus/src/opentelemetry/exporter/prometheus/`
- **License**: Apache License 2.0

## Updates

If the upstream package is eventually released as a stable version, we should
consider removing this vendored copy and depending on the official package
directly.

To update this vendored copy:

1. Clone or update your local copy of opentelemetry-python
2. Copy the relevant files from `exporter/opentelemetry-exporter-prometheus/src/opentelemetry/exporter/prometheus/`
3. Update imports from `opentelemetry.exporter.prometheus._mapping` to `._mapping`
4. Add any necessary type annotations for strict type checking
5. Run the test suite to verify compatibility
