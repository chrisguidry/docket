import threading
from contextlib import contextmanager
from typing import Generator, cast

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import set_meter_provider
from opentelemetry.propagators.textmap import Getter, Setter
from opentelemetry.sdk.metrics import MeterProvider

meter: metrics.Meter = metrics.get_meter("docket")

TASKS_ADDED = meter.create_counter(
    "docket_tasks_added",
    description="How many tasks added to the docket",
    unit="1",
)

TASKS_REPLACED = meter.create_counter(
    "docket_tasks_replaced",
    description="How many tasks replaced on the docket",
    unit="1",
)

TASKS_SCHEDULED = meter.create_counter(
    "docket_tasks_scheduled",
    description="How many tasks added or replaced on the docket",
    unit="1",
)

TASKS_CANCELLED = meter.create_counter(
    "docket_tasks_cancelled",
    description="How many tasks cancelled from the docket",
    unit="1",
)

TASKS_STARTED = meter.create_counter(
    "docket_tasks_started",
    description="How many tasks started",
    unit="1",
)

TASKS_STRICKEN = meter.create_counter(
    "docket_tasks_stricken",
    description="How many tasks have been stricken from executing",
    unit="1",
)

TASKS_COMPLETED = meter.create_counter(
    "docket_tasks_completed",
    description="How many tasks that have completed in any state",
    unit="1",
)

TASKS_FAILED = meter.create_counter(
    "docket_tasks_failed",
    description="How many tasks that have failed",
    unit="1",
)

TASKS_SUCCEEDED = meter.create_counter(
    "docket_tasks_succeeded",
    description="How many tasks that have succeeded",
    unit="1",
)

TASKS_RETRIED = meter.create_counter(
    "docket_tasks_retried",
    description="How many tasks that have been retried",
    unit="1",
)

TASKS_PERPETUATED = meter.create_counter(
    "docket_tasks_perpetuated",
    description="How many tasks that have been self-perpetuated",
    unit="1",
)

TASK_DURATION = meter.create_histogram(
    "docket_task_duration",
    description="How long tasks take to complete",
    unit="s",
)

TASK_PUNCTUALITY = meter.create_histogram(
    "docket_task_punctuality",
    description="How close a task was to its scheduled time",
    unit="s",
)

TASKS_RUNNING = meter.create_up_down_counter(
    "docket_tasks_running",
    description="How many tasks that are currently running",
    unit="1",
)

REDIS_DISRUPTIONS = meter.create_counter(
    "docket_redis_disruptions",
    description="How many times the Redis connection has been disrupted",
    unit="1",
)

STRIKES_IN_EFFECT = meter.create_up_down_counter(
    "docket_strikes_in_effect",
    description="How many strikes are currently in effect",
    unit="1",
)

QUEUE_DEPTH = meter.create_gauge(
    "docket_queue_depth",
    description="How many tasks are due to be executed now",
    unit="1",
)
SCHEDULE_DEPTH = meter.create_gauge(
    "docket_schedule_depth",
    description="How many tasks are scheduled to be executed in the future",
    unit="1",
)

Message = dict[bytes, bytes]


class MessageGetter(Getter[Message]):
    def get(self, carrier: Message, key: str) -> list[str] | None:
        val = carrier.get(key.encode(), None)
        if val is None:
            return None
        return [val.decode()]

    def keys(self, carrier: Message) -> list[str]:
        return [key.decode() for key in carrier.keys()]


class MessageSetter(Setter[Message]):
    def set(
        self,
        carrier: Message,
        key: str,
        value: str,
    ) -> None:
        carrier[key.encode()] = value.encode()


message_getter: MessageGetter = MessageGetter()
message_setter: MessageSetter = MessageSetter()


@contextmanager
def metrics_server(
    host: str = "0.0.0.0", port: int | None = None
) -> Generator[None, None, None]:
    if port is None:
        yield
        return

    from wsgiref.types import WSGIApplication

    from prometheus_client import REGISTRY
    from prometheus_client.exposition import (
        ThreadingWSGIServer,
        _SilentHandler,  # type: ignore[member-access]
        make_server,  # type: ignore[import]
        make_wsgi_app,  # type: ignore[import]
    )

    set_meter_provider(MeterProvider(metric_readers=[PrometheusMetricReader()]))

    server = make_server(
        host,
        port,
        cast(WSGIApplication, make_wsgi_app(registry=REGISTRY)),
        ThreadingWSGIServer,
        handler_class=_SilentHandler,
    )
    with server:
        t = threading.Thread(target=server.serve_forever)
        t.daemon = True
        t.start()

        yield
