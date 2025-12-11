# Upstream version - change this to update
OTEL_VERSION := v1.39.0

# Upstream repository
OTEL_REPO := https://github.com/open-telemetry/opentelemetry-python
OTEL_UPSTREAM_PATH := exporter/opentelemetry-exporter-prometheus/src/opentelemetry/exporter/prometheus

# Local paths
OTEL_DIR := otel_prometheus

.PHONY: all

all: $(OTEL_DIR)/__init__.py $(OTEL_DIR)/_mapping.py

# The Makefile itself is a dependency - changing OTEL_VERSION triggers rebuild
$(OTEL_DIR)/__init__.py: Makefile
	curl -sL "$(OTEL_REPO)/raw/$(OTEL_VERSION)/$(OTEL_UPSTREAM_PATH)/__init__.py" -o $@
	sed -i 's/from opentelemetry\.exporter\.prometheus\._mapping/from ._mapping/' $@
	@echo "Fetched otel_prometheus/__init__.py from $(OTEL_VERSION)"

$(OTEL_DIR)/_mapping.py: Makefile
	curl -sL "$(OTEL_REPO)/raw/$(OTEL_VERSION)/$(OTEL_UPSTREAM_PATH)/_mapping.py" -o $@
	@echo "Fetched otel_prometheus/_mapping.py from $(OTEL_VERSION)"
