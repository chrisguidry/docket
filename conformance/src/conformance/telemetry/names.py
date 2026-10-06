"""The names that change from run to run, and the placeholders that replace them."""

from collections.abc import Mapping
from dataclasses import dataclass

DOCKET = "<docket>"
WORKER = "<worker>"


@dataclass(frozen=True)
class Names:
    """The docket, the workers, and the phase prefix of one phase's keys."""

    docket: str
    workers: frozenset[str]
    phase: str

    def scrub(self, value: object) -> object:
        """Replace a name that differs on every run with its placeholder."""
        if not isinstance(value, str):
            return value
        if value == self.docket:
            return DOCKET
        if value in self.workers:
            return WORKER
        return value.removeprefix(f"{self.phase}:")


def labels(attributes: Mapping[str, object]) -> str:
    """One data point's attributes, in one stable line."""
    return ", ".join(f"{key}={value}" for key, value in sorted(attributes.items()))
