from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable

import structlog

logger = structlog.get_logger()


class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler. ``status_func`` is set at server level."""

    status_func: Callable[[], dict[str, Any]]

    def do_GET(self) -> None:
        if self.path == "/health":
            status = self.status_func()
            status["timestamp"] = datetime.now(timezone.utc).isoformat()
            code = 200 if status.get("healthy") else 503
            body = json.dumps(status).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def log_message(self, format: str, *args: Any) -> None:
        # Suppress default stderr logging; we use structlog
        pass


class HealthCheckServer:
    """Lightweight HTTP health-check server running on a daemon thread."""

    def __init__(
        self,
        port: int,
        status_func: Callable[[], dict[str, Any]],
    ) -> None:
        _HealthHandler.status_func = staticmethod(status_func)  # type: ignore[assignment]
        self._server = HTTPServer(("0.0.0.0", port), _HealthHandler)
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            daemon=True,
            name="health-check",
        )
        self._thread.start()
        logger.info("health_check_started", port=self._server.server_port)

    def stop(self) -> None:
        self._server.shutdown()
