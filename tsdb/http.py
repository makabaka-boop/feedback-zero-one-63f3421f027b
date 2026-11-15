"""Minimal JSON-over-HTTP server helpers (stdlib only)."""

from __future__ import annotations

import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .common import RequestError, canonical_json

MAX_BODY = 64 * 1024 * 1024


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "tsdb/1.0"
    app = None  # set by serve()

    # -- plumbing ---------------------------------------------------------

    def log_message(self, fmt, *args):  # quiet by default
        if getattr(self.server, "verbose", False):
            super().log_message(fmt, *args)

    def _read_body(self) -> bytes:
        length = self.headers.get("Content-Length")
        if length is None:
            return b""
        try:
            size = int(length)
        except ValueError:
            raise RequestError("$")
        if size < 0 or size > MAX_BODY:
            raise RequestError("$")
        data = self.rfile.read(size)
        if len(data) != size:
            raise RequestError("$")
        return data

    def _respond(self, status: int, obj) -> None:
        payload = canonical_json(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _dispatch(self, method: str) -> None:
        parts = urlsplit(self.path)
        query = {k: v[-1] for k, v in parse_qs(parts.query, keep_blank_values=True).items()}
        try:
            body = self._read_body() if method == "POST" else b""
            status, obj = self.app(method, parts.path, query, body)
        except RequestError as exc:
            status, obj = 400, {"error": exc.path}
        except Exception:  # pragma: no cover - defensive
            traceback.print_exc()
            status, obj = 500, {"error": "$"}
        self._respond(status, obj)

    def do_GET(self):  # noqa: N802
        self._dispatch("GET")

    def do_POST(self):  # noqa: N802
        self._dispatch("POST")


def serve(app, host: str, port: int, verbose: bool = False) -> ThreadingHTTPServer:
    handler = type("Handler", (_Handler,), {"app": staticmethod(app)})
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.daemon_threads = True
    httpd.verbose = verbose
    httpd.allow_reuse_address = True
    return httpd
