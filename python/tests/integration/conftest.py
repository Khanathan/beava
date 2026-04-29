"""Plan 12-09: integration-test conftest overriding `beava_server` to set
per-test WAL/snapshot/admin tempdirs.

The session-shared `python/tests/conftest.py::beava_server` fixture only
sets BEAVA_LISTEN_ADDR / BEAVA_TCP_PORT, leaving WAL_DIR / SNAPSHOT_DIR
unset (binary uses cwd defaults), which collides across test runs and
fails with `os error 17 (File exists)`. Plan 12-07 also added
admin_addr (default 127.0.0.1:8090) which can clash if any other beava
is running.

This integration-only override:
  - Allocates per-test wal_dir, snapshot_dir tempdirs.
  - Sets BEAVA_WAL_DIR / BEAVA_SNAPSHOT_DIR / BEAVA_ADMIN_ADDR
    (admin_addr 127.0.0.1:0 → OS-assign).
  - Yields (http_url, tcp_url) like the parent fixture.
  - Uses a 15-second bind timeout (debug-build cold spawn can take
    >5s on a busy laptop).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import Generator

import pytest


@pytest.fixture
def beava_server(beava_binary: Path) -> Generator[tuple[str, str], None, None]:
    wal_dir = Path(tempfile.mkdtemp(prefix="beava-12-09-wal-"))
    snap_dir = Path(tempfile.mkdtemp(prefix="beava-12-09-snap-"))
    env = {
        **os.environ,
        "BEAVA_LISTEN_ADDR": "127.0.0.1:0",
        "BEAVA_TCP_PORT": "0",
        "BEAVA_ADMIN_ADDR": "127.0.0.1:0",  # Plan 12-07: admin_addr separate port
        "BEAVA_WAL_DIR": str(wal_dir),
        "BEAVA_SNAPSHOT_DIR": str(snap_dir),
        "BEAVA_DEV_ENDPOINTS": "1",
    }

    proc = subprocess.Popen(
        [str(beava_binary), "--config", "/dev/null"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )

    http_addr: list[str] = []
    tcp_addr: list[str] = []
    ready = threading.Event()
    stderr_acc: list[str] = []

    def _stdout_reader() -> None:
        assert proc.stdout is not None
        for raw in proc.stdout:
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

    def _stderr_reader() -> None:
        assert proc.stderr is not None
        for raw in proc.stderr:
            stderr_acc.append(raw.decode("utf-8", errors="replace").rstrip())

    t_out = threading.Thread(target=_stdout_reader, daemon=True)
    t_err = threading.Thread(target=_stderr_reader, daemon=True)
    t_out.start()
    t_err.start()

    try:
        if not ready.wait(timeout=15.0):
            proc.kill()
            proc.wait()
            if proc.stdout:
                proc.stdout.close()
            if proc.stderr:
                proc.stderr.close()
            stderr_text = "\n".join(stderr_acc[-30:])
            pytest.fail(
                "beava server did not emit both bind log lines within 15s. "
                f"http_addr={http_addr}, tcp_addr={tcp_addr}\n"
                f"stderr tail:\n{stderr_text}"
            )

        http_url = f"http://{http_addr[0]}"
        tcp_url = f"tcp://{tcp_addr[0]}"

        yield http_url, tcp_url

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        for p in (wal_dir, snap_dir):
            try:
                shutil.rmtree(p, ignore_errors=True)
            except OSError:
                pass
