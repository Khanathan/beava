#!/usr/bin/env python3
"""Tiny dev-only proxy: serves demo/index.html on /, proxies API calls to beava.

Why: avoids CORS by giving the browser a single origin (us). The Beava server
is the upstream; we forward /register, /push/*, /get/*, /get, /registry,
/health, /ready transparently.

Run: python3 proxy.py [--port 8000] [--backend http://127.0.0.1:8080]
"""
import argparse
import http.server
import os
import socketserver
import sys
import urllib.request
import urllib.error

DEFAULT_PORT = 8000
DEFAULT_BACKEND = "http://127.0.0.1:8080"
PROXY_PREFIXES = ("/health", "/ready", "/registry", "/register", "/get", "/push/", "/dev/")


class Handler(http.server.SimpleHTTPRequestHandler):
    backend = DEFAULT_BACKEND

    def do_GET(self):  # noqa: N802
        if self._is_proxied():
            self._proxy("GET", None)
            return
        super().do_GET()

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", 0) or 0)
        body = self.rfile.read(length) if length else b""
        self._proxy("POST", body)

    def _is_proxied(self) -> bool:
        return self.path.startswith(PROXY_PREFIXES)

    def _proxy(self, method: str, body):
        url = self.backend.rstrip("/") + self.path
        headers = {
            k: v
            for k, v in self.headers.items()
            if k.lower() not in ("host", "connection", "content-length")
        }
        if body is not None:
            headers["Content-Length"] = str(len(body))
        req = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                self._relay(resp.status, resp.headers, resp.read())
        except urllib.error.HTTPError as e:
            self._relay(e.code, e.headers, e.read())
        except urllib.error.URLError as e:
            msg = f'{{"error":"backend_unreachable","detail":"{e.reason}"}}'.encode()
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)

    def _relay(self, status, headers, body):
        self.send_response(status)
        for k, v in headers.items():
            if k.lower() in ("transfer-encoding", "connection", "content-length"):
                continue
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[proxy] {self.address_string()} - {fmt % args}\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--backend", default=DEFAULT_BACKEND)
    args = p.parse_args()
    Handler.backend = args.backend
    os.chdir(os.path.dirname(os.path.abspath(__file__)) or ".")
    with socketserver.TCPServer(("127.0.0.1", args.port), Handler) as srv:
        print(f"[proxy] listening on http://127.0.0.1:{args.port}/")
        print(f"[proxy] forwarding {PROXY_PREFIXES} → {args.backend}")
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            print("\n[proxy] shutting down")


if __name__ == "__main__":
    main()
