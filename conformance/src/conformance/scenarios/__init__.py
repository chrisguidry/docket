"""One module for each scenario, with ``TIMEOUT`` and ``run(harness)``.

Each language's agent has a module with the same name, which holds the
scenario's tasks and its producer.
"""

from types import ModuleType

from . import (
    backoff,
    cancel_before_start,
    chaos,
    concurrency_limit,
    graceful_drain,
    perpetual,
    perpetual_single_flight,
    redelivery,
    same_key,
)

SCENARIOS: dict[str, ModuleType] = {
    "backoff": backoff,
    "perpetual": perpetual,
    "cancel-before-start": cancel_before_start,
    "graceful-drain": graceful_drain,
    "concurrency-limit": concurrency_limit,
    "redelivery": redelivery,
    "perpetual-single-flight": perpetual_single_flight,
    "same-key": same_key,
    "chaos": chaos,
}
