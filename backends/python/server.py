"""Sample Python backend (stdlib only).

GET /health -> 200. Any other request -> JSON echo. Test knobs (query string):
  delay_ms=N / delay_ms.<name>=N   sleep before answering
  status=N   / status.<name>=N     respond with this status code
"""
import json
import os
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

NAME = os.environ.get("BACKEND_NAME", "python")
PORT = int(os.environ.get("PORT", "8001"))


def knob(q, key, default):
    for k in (f"{key}.{NAME}", key):
        if k in q:
            try:
                return int(q[k][0])
            except ValueError:
                pass
    return default


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # quiet; the proxy records every request
        pass

    def _send(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Backend-Name", NAME)
        self.send_header("X-Backend-Runtime", "python")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _handle(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        url = urlsplit(self.path)
        if url.path == "/health":
            return self._send(200, {"status": "ok", "backend": NAME})
        q = parse_qs(url.query)
        delay = knob(q, "delay_ms", 0)
        if delay > 0:
            time.sleep(delay / 1000)
        self._send(knob(q, "status", 200), {
            "backend": NAME, "runtime": "python", "method": self.command, "path": url.path,
            "query": url.query, "body_bytes": len(body),
            "request_id": self.headers.get("X-Request-Id", ""),
            "config_version": self.headers.get("X-Mb-Config-Version", ""),
            "fanout": self.headers.get("X-Mb-Fanout") == "true",
            "delay_ms": delay, "time": datetime.now(timezone.utc).isoformat(),
        })

    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = do_OPTIONS = _handle


if __name__ == "__main__":
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer.request_queue_size = 1024
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"python backend {NAME} listening on 127.0.0.1:{PORT}", flush=True)
    server.serve_forever()
