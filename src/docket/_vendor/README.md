# Vendored Dependencies

This directory contains vendored third-party code to avoid problematic dependencies.

## Updating Vendored Code

Run `make` in this directory to update vendored dependencies from upstream:

```bash
cd src/docket/_vendor
make
```

The Makefile fetches files from upstream repositories and applies necessary patches
(e.g., fixing import paths). Changing `OTEL_VERSION` in the Makefile and running
`make` will fetch the new version.

## otel_prometheus/

Vendored copy of the OpenTelemetry Prometheus exporter.

- **Upstream**: https://github.com/open-telemetry/opentelemetry-python
- **Path**: `exporter/opentelemetry-exporter-prometheus/src/opentelemetry/exporter/prometheus/`
- **License**: Apache License 2.0
- **Why**: The `opentelemetry-exporter-prometheus` package is only available as a
  pre-release version. Some package managers refuse to install pre-release packages
  by default. See [#226](https://github.com/chrisguidry/docket/issues/226).

If upstream releases a stable version, consider removing this and using the official
package directly.

## _uuid7.py

UUID v7 polyfill for Python < 3.14.

- **Upstream**: https://github.com/stevesimmons/uuid7
- **License**: MIT
- **Why**: Vendored to avoid external dependency. The docket version includes
  wrapper code to use stdlib `uuid.uuid7()` on Python 3.14+.

This file is maintained manually (not via Makefile) due to the significant wrapper
code around the upstream implementation.
