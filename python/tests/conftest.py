"""Pytest fixtures for beava Python SDK tests.

Session fixture `beava_binary` runs `cargo build --bin beava --quiet` once per test
session (cached across tests). Each test that uses `beava_server` pays ~150ms spawn
cost; the `beava_binary` session fixture caches the cargo build.

Port-override mechanism (verified from crates/beava-server/src/cli.rs and
crates/beava-core/src/config.rs):
  - The binary reads a YAML config via --config flag.
  - Env var overrides: BEAVA_LISTEN_ADDR=127.0.0.1:0 for HTTP (port 0 = OS-assigns);
    BEAVA_TCP_PORT=0 for TCP (port 0 = OS-assigns).
  - Both kinds are exposed in the server startup JSON log lines on stderr:
      {"kind":"server.http_bound","addr":"127.0.0.1:NNNN",...}
      {"kind":"server.tcp_bound","addr":"127.0.0.1:NNNN",...}
  - BEAVA_DEV_ENDPOINTS=1 mounts GET /registry (used by Plan 03-06 smoke).
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
from pathlib import Path
from typing import Generator

import pytest


@pytest.fixture(scope="session")
def beava_binary(pytestconfig: pytest.Config) -> Path:
    """Build the beava binary once per test session via cargo.

    pytestconfig.rootpath is the pytest rootdir (python/); the repo root is one
    level up.  Returns the path to target/debug/beava.  Raises pytest.fail on
    build failure so the whole session fails fast with a clear message.
    """
    repo_root = Path(str(pytestconfig.rootpath)).parent
    result = subprocess.run(
        ["cargo", "build", "--bin", "beava", "--quiet"],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(
            f"cargo build --bin beava failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )
    binary = repo_root / "target" / "debug" / "beava"
    if not binary.is_file():
        pytest.fail(f"Binary not found at {binary} after successful build")
    return binary


@pytest.fixture
def beava_server(beava_binary: Path) -> Generator[tuple[str, str], None, None]:
    """Spawn a beava server on ephemeral HTTP and TCP ports.

    Parses stderr for server.http_bound + server.tcp_bound JSON log lines to
    discover the OS-assigned ports.  Yields (http_url, tcp_url).  Sends SIGTERM
    on teardown and waits up to 5s; SIGKILL if the process doesn't exit.

    Spawn uses env var overrides (not CLI flags) because the binary only accepts
    --config for a YAML file; there is no --http-port / --tcp-port CLI flag.
    Port overrides via env vars:
      BEAVA_LISTEN_ADDR=127.0.0.1:0  (HTTP listener — port 0 → OS assigns)
      BEAVA_TCP_PORT=0                (TCP listener — port 0 → OS assigns)
    """
    env = {
        **os.environ,
        "BEAVA_LISTEN_ADDR": "127.0.0.1:0",
        "BEAVA_TCP_PORT": "0",
        "BEAVA_DEV_ENDPOINTS": "1",  # expose GET /registry for Plan 03-06 smoke
    }

    proc = subprocess.Popen(
        [str(beava_binary)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=env,
    )

    http_addr: list[str] = []
    tcp_addr: list[str] = []
    ready = threading.Event()
    error: list[str] = []

    def _reader() -> None:
        assert proc.stderr is not None
        for raw in proc.stderr:
            line = raw.decode("utf-8", errors="replace").rstrip()
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            kind = rec.get("kind", "")
            if kind == "server.http_bound":
                http_addr.append(rec["addr"])
            elif kind == "server.tcp_bound":
                tcp_addr.append(rec["addr"])
            if http_addr and tcp_addr:
                ready.set()

    t = threading.Thread(target=_reader, daemon=True)
    t.start()

    if not ready.wait(timeout=5.0):
        proc.kill()
        proc.wait()
        error_detail = (
            f"http_addr={http_addr}, tcp_addr={tcp_addr}"
        )
        pytest.fail(
            f"beava server did not emit both bind log lines within 5s: {error_detail}"
        )

    http_url = f"http://{http_addr[0]}"
    tcp_url = f"tcp://{tcp_addr[0]}"

    yield http_url, tcp_url

    proc.terminate()
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
