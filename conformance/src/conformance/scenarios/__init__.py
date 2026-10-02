"""One module for each scenario, with ``TIMEOUT`` and ``run(harness)``.

Each language's agent has a module with the same name, which holds the
scenario's tasks and its producer.
"""

from types import ModuleType

from . import backoff, cancel_before_start, chaos, graceful_drain, perpetual

SCENARIOS: dict[str, ModuleType] = {
    "backoff": backoff,
    "perpetual": perpetual,
    "cancel-before-start": cancel_before_start,
    "graceful-drain": graceful_drain,
    "chaos": chaos,
}
