"""The telemetry that every implementation emits for the telemetry workload.

These tables follow plans/telemetry-parity.md, and pydocket confirms them.
The workload's runs, by task: ``succeed`` 1, ``fail`` 1, ``flaky`` 2,
``perpetual`` 3, and ``later`` 1.
"""

from .metrics import PRESENT
from .names import DOCKET, WORKER, labels
from .prometheus_rendering import prometheus

Points = dict[str, object]


def on_docket(**values: object) -> Points:
    """Data points with the docket and task attributes, by task."""
    return {
        labels({"docket.name": DOCKET, "docket.task": task}): value
        for task, value in values.items()
    }


def on_worker(**values: object) -> Points:
    """Data points with the docket, worker, and task attributes, by task."""
    return {
        labels({"docket.name": DOCKET, "docket.task": task, "docket.worker": WORKER}): (
            value
        )
        for task, value in values.items()
    }


def metric(kind: str, description: str, points: Points, unit: str = "1") -> Points:
    return {
        "kind": kind,
        "unit": unit,
        "description": description,
        "meter": "docket",
        "points": points,
    }


PRODUCER_METRICS = {
    "docket_tasks_added": metric(
        "counter",
        "How many tasks added to the docket",
        on_docket(succeed=1, fail=1, flaky=1, perpetual=1, doomed=1, later=1),
    ),
    "docket_tasks_replaced": metric(
        "counter",
        "How many tasks replaced on the docket",
        on_docket(later=1),
    ),
    "docket_tasks_scheduled": metric(
        "counter",
        "How many tasks added or replaced on the docket",
        on_docket(succeed=1, fail=1, flaky=1, perpetual=1, doomed=1, later=2),
    ),
    "docket_tasks_cancelled": metric(
        "counter",
        "How many tasks cancelled from the docket",
        # The replace of later, then the cancel of doomed.
        {**on_docket(later=1), labels({"docket.name": DOCKET}): 1},
    ),
    "docket_tasks_stricken": metric(
        "counter",
        "How many tasks have been stricken from executing",
        {
            labels(
                {
                    "docket.name": DOCKET,
                    "docket.task": "struck",
                    "docket.where": "docket",
                }
            ): 1
        },
    ),
}

WORKER_METRICS = {
    "docket_tasks_started": metric(
        "counter",
        "How many tasks started",
        on_worker(succeed=1, fail=1, flaky=2, perpetual=3, later=1),
    ),
    "docket_tasks_running": metric(
        "up-down counter",
        "How many tasks that are currently running",
        on_worker(succeed=0, fail=0, flaky=0, perpetual=0, later=0),
    ),
    "docket_task_punctuality": metric(
        "histogram",
        "How close a task was to its scheduled time",
        on_worker(succeed=1, fail=1, flaky=2, perpetual=3, later=1),
        unit="s",
    ),
    "docket_tasks_succeeded": metric(
        "counter",
        "How many tasks that have succeeded",
        on_worker(succeed=1, flaky=1, perpetual=3, later=1),
    ),
    "docket_tasks_failed": metric(
        "counter",
        "How many tasks that have failed",
        on_worker(fail=1, flaky=1),
    ),
    "docket_tasks_retried": metric(
        "counter",
        "How many tasks that have been retried",
        on_worker(flaky=1),
    ),
    # The third run cancels itself and does not count.
    "docket_tasks_perpetuated": metric(
        "counter",
        "How many tasks that have been self-perpetuated",
        on_worker(perpetual=2),
    ),
    "docket_tasks_completed": metric(
        "counter",
        "How many tasks that have completed in any state",
        on_worker(succeed=1, fail=1, flaky=2, perpetual=3, later=1),
    ),
    "docket_task_duration": metric(
        "histogram",
        "How long tasks take to complete",
        on_worker(succeed=1, fail=1, flaky=2, perpetual=3, later=1),
        unit="s",
    ),
    # A perpetual task's reschedule is a replace.  The cancel that stops it
    # after its third run does not count.
    "docket_tasks_replaced": metric(
        "counter",
        "How many tasks replaced on the docket",
        on_docket(perpetual=2),
    ),
    "docket_tasks_cancelled": metric(
        "counter",
        "How many tasks cancelled from the docket",
        on_docket(perpetual=2),
    ),
    "docket_tasks_scheduled": metric(
        "counter",
        "How many tasks added or replaced on the docket",
        on_docket(perpetual=2),
    ),
    # The worker reads the strike and the restore.
    "docket_strikes_in_effect": metric(
        "up-down counter",
        "How many strikes are currently in effect",
        on_docket(struck=0),
    ),
    "docket_queue_depth": metric(
        "gauge",
        "How many tasks are due to be executed now",
        {labels({"docket.name": DOCKET}): PRESENT},
    ),
    "docket_schedule_depth": metric(
        "gauge",
        "How many tasks are scheduled to be executed in the future",
        {labels({"docket.name": DOCKET}): PRESENT},
    ),
}


def scheduling(
    name: str,
    task: str,
    *,
    process: str = "producer",
    disposition: str = "scheduled",
    parent: str | None = None,
) -> dict[str, object]:
    """A ``docket.add`` or ``docket.replace`` span."""
    return {
        "process": process,
        "name": name,
        "tracer": "docket.docket",
        "kind": "internal",
        "status": "unset",
        "attributes": {
            "code.function.name": task,
            "docket.attempt": 1,
            "docket.disposition": disposition,
            "docket.key": task,
            "docket.name": DOCKET,
            "docket.task": task,
            "docket.when": "<iso8601>",
        },
        "events": [],
        "parent": parent,
        "links": [],
    }


def command(name: str, **attributes: object) -> dict[str, object]:
    """A producer's span for a command that does not schedule a task."""
    return {
        "process": "producer",
        "name": name,
        "tracer": "docket.docket",
        "kind": "internal",
        "status": "unset",
        "attributes": {"docket.name": DOCKET, **attributes},
        "events": [],
        "parent": None,
        "links": [],
    }


def run(
    task: str, attempt: int = 1, *, error: str | None = None, link: str | None = None
) -> dict[str, object]:
    """A worker's consumer span for one run, which starts a new trace."""
    return {
        "process": "worker",
        "name": task,
        "tracer": "docket.worker",
        "kind": "consumer",
        "status": f"error: {error}" if error else "ok",
        "attributes": {
            "code.function.name": task,
            "docket.attempt": attempt,
            "docket.key": task,
            "docket.name": DOCKET,
            "docket.task": task,
            "docket.when": "<iso8601>",
            "docket.worker": WORKER,
        },
        "events": [f"exception: {error}"] if error else [],
        "parent": None,
        "links": [link] if link else [],
    }


SPANS = [
    scheduling("docket.add", "succeed"),
    scheduling("docket.add", "fail"),
    scheduling("docket.add", "flaky"),
    scheduling("docket.add", "perpetual"),
    scheduling("docket.add", "doomed"),
    command("docket.cancel", **{"docket.key": "doomed"}),
    scheduling("docket.add", "later"),
    scheduling("docket.replace", "later"),
    command("docket.strike", **{"docket.task": "struck"}),
    scheduling("docket.add", "struck", disposition="struck"),
    command("docket.restore", **{"docket.task": "struck"}),
    run("succeed", link="producer docket.add key=succeed attempt=1"),
    run("fail", error="boom", link="producer docket.add key=fail attempt=1"),
    run("flaky", error="boom", link="producer docket.add key=flaky attempt=1"),
    run("flaky", attempt=2, link="worker flaky key=flaky attempt=1"),
    run("perpetual", link="producer docket.add key=perpetual attempt=1"),
    scheduling(
        "docket.replace",
        "perpetual",
        process="worker",
        parent="worker perpetual key=perpetual attempt=1",
    ),
    # The next two runs have no links: protocol/stream_due_tasks.lua moves a
    # due task from the queue to the stream without its trace context.
    run("perpetual"),
    scheduling(
        "docket.replace",
        "perpetual",
        process="worker",
        parent="worker perpetual key=perpetual attempt=1",
    ),
    run("perpetual"),
    run("later", link="producer docket.replace key=later attempt=1"),
]

TELEMETRY: dict[str, object] = {
    "metrics": {"producer": PRODUCER_METRICS, "worker": WORKER_METRICS},
    "spans": SPANS,
    "prometheus": prometheus(WORKER_METRICS),
}
