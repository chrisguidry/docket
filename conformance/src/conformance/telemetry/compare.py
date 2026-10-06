"""Readable differences between two sets of normalized telemetry."""

import json
from collections import Counter
from typing import cast


def line(value: object) -> str:
    return json.dumps(value, sort_keys=True)


def differences(expected: object, actual: object, path: str = "") -> list[str]:
    """Each place where ``actual`` differs from ``expected``, one per line.

    Dictionaries compare key by key.  Lists compare as multisets, because the
    order of spans and events does not depend on docket alone.
    """
    # The checks below narrow ``expected`` and ``actual`` to containers of
    # unknown types, which the strict type checker refuses to pass on.
    expected_value, actual_value = expected, actual
    if isinstance(expected, dict) and isinstance(actual, dict):
        wanted = cast(dict[str, object], expected)
        seen = cast(dict[str, object], actual)
        found: list[str] = []
        for key in sorted({*wanted, *seen}):
            where = f"{path}.{key}" if path else key
            if key not in seen:
                found.append(f"{where}: missing, expected {line(wanted[key])}")
            elif key not in wanted:
                found.append(f"{where}: unexpected {line(seen[key])}")
            else:
                found += differences(wanted[key], seen[key], where)
        return found

    if isinstance(expected, list) and isinstance(actual, list):
        wanted_items = Counter(line(item) for item in cast(list[object], expected))
        seen_items = Counter(line(item) for item in cast(list[object], actual))
        return [
            f"{path}: missing {item}"
            for item in sorted((wanted_items - seen_items).elements())
        ] + [
            f"{path}: unexpected {item}"
            for item in sorted((seen_items - wanted_items).elements())
        ]

    if expected_value != actual_value:
        return [f"{path}: expected {line(expected_value)}, saw {line(actual_value)}"]
    return []
