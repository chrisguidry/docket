"""An OTLP/HTTP receiver that keeps every trace and metric export it gets."""

import gzip
import threading
from collections.abc import Generator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)

from ..server import get_free_port


class Receiver:
    def __init__(self, port: int) -> None:
        self.port = port
        self.endpoint = f"http://127.0.0.1:{port}"
        self.traces: list[ExportTraceServiceRequest] = []
        self.metrics: list[ExportMetricsServiceRequest] = []
        self.lock = threading.Lock()


@contextmanager
def receiving() -> Generator[Receiver, None, None]:
    """Run a receiver on a free port until the block ends."""
    receiver = Receiver(get_free_port())

    class Handler(BaseHTTPRequestHandler):
        # The exporters keep their connections open between exports.
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            if self.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)

            if self.path == "/v1/traces":
                traces = ExportTraceServiceRequest.FromString(body)
                with receiver.lock:
                    receiver.traces.append(traces)
                response = ExportTraceServiceResponse().SerializeToString()
            elif self.path == "/v1/metrics":
                metrics = ExportMetricsServiceRequest.FromString(body)
                with receiver.lock:
                    receiver.metrics.append(metrics)
                response = ExportMetricsServiceResponse().SerializeToString()
            else:
                self.send_error(404)
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/x-protobuf")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, format: str, *args: object) -> None:
            """Keep each export out of the driver's output."""

    server = ThreadingHTTPServer(("127.0.0.1", receiver.port), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield receiver
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
