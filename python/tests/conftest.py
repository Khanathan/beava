"""Pytest fixtures for Beava integration tests.

Provides a ``beava_server`` session-scoped fixture that:
1. Builds the Beava binary via ``cargo build``
2. Finds two free TCP ports for test isolation
3. Starts the server subprocess with BEAVA_TCP_PORT / BEAVA_HTTP_PORT env vars
4. Waits for server readiness (TCP connect with retries, max 10s)
5. Yields ``(host, tcp_port, http_port)``
6. Kills the server on teardown (SIGTERM then SIGKILL)

Also provides an ``app`` fixture returning a connected ``beava.App`` instance.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time

import pytest

import beava as bv

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_BINARY_PATH = os.path.join(_PROJECT_ROOT, "target", "debug", "beava")


def _find_free_port() -> int:
    """Bind to port 0, read the assigned port, then close the socket."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_tcp(host: str, port: int, timeout: float = 10.0) -> None:
    """Block until a TCP connection to *host:port* succeeds, or raise."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(
        f"Beava server did not become ready on {host}:{port} within {timeout}s"
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def beava_server():
    """Build and start a Beava server, yield ``(host, tcp_port, http_port)``.

    Session-scoped: one server instance shared across all integration tests.
    """
    # 1. Build the binary (idempotent -- cargo skips if up-to-date)
    cargo_bin = os.path.expanduser("~/.cargo/bin/cargo")
    if not os.path.exists(cargo_bin):
        cargo_bin = "cargo"
    result = subprocess.run(
        [cargo_bin, "build"],
        cwd=_PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        pytest.fail(f"cargo build failed:\n{result.stderr}")

    # 2. Pick random free ports
    tcp_port = _find_free_port()
    http_port = _find_free_port()
    host = "127.0.0.1"

    # 3. Start the server subprocess
    env = os.environ.copy()
    env["BEAVA_TCP_PORT"] = str(tcp_port)
    env["BEAVA_HTTP_PORT"] = str(http_port)

    proc = subprocess.Popen(
        [_BINARY_PATH],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # 4. Wait for readiness
        _wait_for_tcp(host, tcp_port, timeout=10.0)
        yield host, tcp_port, http_port
    finally:
        # 5. Teardown: graceful stop then force-kill
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)


@pytest.fixture(scope="session")
def app(beava_server):
    """Return a ``beava.App`` connected to the test server."""
    host, tcp_port, _http_port = beava_server
    application = bv.App(f"{host}:{tcp_port}")
    yield application
    application.close()
