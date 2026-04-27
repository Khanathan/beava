"""Local conftest for ``python/tests/bench/`` — Phase 19 smoke fixtures.

The repo-wide ``python/tests/conftest.py`` provides ``beava_server`` which spawns
the beava binary with a hard-coded default WAL dir (``./beava-wal``). Phase 19's
smoke test runs in environments where a stale WAL from a prior test session can
exist on disk, which makes the binary fail at startup with
``failed to spawn WAL sink: io: File exists (os error 17)``.

This local fixture wraps the binary spawn the same way conftest.py does, but
isolates the WAL directory and snapshot directory to a per-test ``tmp_path`` so
the smoke test is robust regardless of stale state on disk. It's a Rule 3
auto-fix (blocking issue): the smoke test cannot complete without WAL
isolation, but the repo-wide conftest is shared with many other tests and
mutating it would risk regressions elsewhere.

Yields ``(http_url, tcp_url)`` exactly like the upstream fixture.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
from pathlib import Path
from typing import Generator

import pytest


@pytest.fixture
def beava_server_isolated(
    beava_binary: Path,
    tmp_path: Path,
) -> Generator[tuple[str, str], None, None]:
    """Spawn beava on ephemeral ports with WAL+snapshot dirs in tmp_path.

    Mirrors python/tests/conftest.py::beava_server but adds:
      - BEAVA_WAL_DIR=tmp_path/wal       (avoids the 'File exists' WAL collision)
      - BEAVA_SNAPSHOT_DIR=tmp_path/snap (mirror isolation for the snapshot writer)

    Returns the same (http_url, tcp_url) tuple shape so tests can swap in this
    fixture without changing their bodies.
    """
    wal_dir = tmp_path / "wal"
    snap_dir = tmp_path / "snap"
    # Don't pre-create — the server creates them. If they pre-exist with prior
    # WAL files, we hit the same File-exists bug we're trying to dodge.

    env = {
        **os.environ,
        "BEAVA_LISTEN_ADDR": "127.0.0.1:0",
        "BEAVA_TCP_PORT": "0",
        "BEAVA_DEV_ENDPOINTS": "1",
        "BEAVA_WAL_DIR": str(wal_dir),
        "BEAVA_SNAPSHOT_DIR": str(snap_dir),
    }

    proc = subprocess.Popen(
        [str(beava_binary), "--config", "/dev/null"],
        stdout=subprocess.PIPE,  # JSON log lines land on stdout
        stderr=subprocess.DEVNULL,
        env=env,
    )

    http_addr: list[str] = []
    tcp_addr: list[str] = []
    ready = threading.Event()

    def _reader() -> None:
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

    t = threading.Thread(target=_reader, daemon=True)
    t.start()

    if not ready.wait(timeout=10.0):
        proc.kill()
        proc.wait()
        if proc.stdout:
            proc.stdout.close()
        # If we couldn't bind within 10s, surface the binary's exit status to help debug.
        rc = proc.poll()
        pytest.fail(
            f"beava server did not emit both bind log lines within 10s; "
            f"http_addr={http_addr}, tcp_addr={tcp_addr}, "
            f"proc_returncode={rc} (None means still running but no bind seen)"
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
