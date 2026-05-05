"""Embed-mode subprocess launcher for Beava.

Provides binary discovery, server spawning, and graceful teardown.

Binary discovery order (D-10, locked):
  1. ``$BEAVA_BINARY`` env var — if set, the path MUST exist and be executable;
     otherwise raises :class:`BinaryNotFoundError` immediately (no fallthrough).
  2. ``beava`` on PATH via ``shutil.which``.
  3. Walk CWD upward looking for ``target/debug/beava`` (dev-loop convenience).
  4. Raise :class:`BinaryNotFoundError` with install guidance.

Security (T-03-04-03): only executes paths from the 4-step discovery order;
no shell interpolation; no arbitrary-command execution.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from beava._errors import BinaryNotFoundError

_log = logging.getLogger("beava.embed")

# ─── Binary discovery ────────────────────────────────────────────────────────


def discover_binary() -> Path:
    """Locate the beava binary using the 4-step discovery order.

    Returns:
        Path to an executable beava binary.

    Raises:
        BinaryNotFoundError: Binary cannot be found using any of the 4 steps.
    """
    # Step 1: BEAVA_BINARY env var — explicit override; MUST be valid if set.
    env_val = os.environ.get("BEAVA_BINARY")
    if env_val is not None:
        p = Path(env_val)
        if p.is_file() and os.access(p, os.X_OK):
            return p
        raise BinaryNotFoundError(
            f"BEAVA_BINARY={env_val!r} is set but the path is not an executable file. "
            f"Unset BEAVA_BINARY or fix the path."
        )

    # Step 2: beava on PATH.
    on_path = shutil.which("beava")
    if on_path is not None:
        return Path(on_path)

    # Step 3: Walk upward from CWD looking for target/debug/beava.
    for parent in [Path.cwd(), *Path.cwd().parents]:
        candidate = parent / "target" / "debug" / "beava"
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate

    # Step 4: Not found — raise with install guidance.
    raise BinaryNotFoundError(
        "beava binary not found. Install with one of:\n"
        "  brew install beava\n"
        "  pip install beava[server]\n"
        "  docker pull beava/beava\n"
        "Or set BEAVA_BINARY=/path/to/beava."
    )


# ─── Server spawn + teardown ─────────────────────────────────────────────────


def spawn_embedded_server(
    startup_timeout: float = 5.0,
    *,
    test_mode: bool = False,
) -> tuple[subprocess.Popen[bytes], str, str, dict[str, str]]:
    """Spawn a local beava server on ephemeral ports and wait until it is ready.

    Reads stderr line-by-line (in a background thread) until both
    ``{"kind":"server.http_bound","addr":"..."}`` and
    ``{"kind":"server.tcp_bound","addr":"..."}`` appear.

    After both bind events are received, remaining stderr lines are forwarded
    to the ``beava.embed`` logger at DEBUG level.

    **Disk lifecycle (Phase 13.5.1 Plan 07e formalization):** Each spawn
    allocates a unique tmpdir at ``$TMPDIR/beava-embed-<pid>-<unix-ms>-<hex>/``
    holding the WAL (``./wal``) and snapshot (``./snapshots``) sub-dirs.
    The path is registered with ``atexit.register(shutil.rmtree, ...,
    ignore_errors=True)`` so the dir is reaped at Python interpreter
    shutdown. SIGKILL'd Python processes leave the dir for the OS tmpfs
    reaper to handle (typical reapers handle ``$TMPDIR`` aging within
    days).

    Args:
        startup_timeout: Seconds to wait for both bind log lines.

    Returns:
        ``(proc, http_url, tcp_url)`` where ``http_url = "http://127.0.0.1:PORT"``
        and ``tcp_url = "tcp://127.0.0.1:PORT"``.

    Raises:
        TimeoutError: Neither/one bind event arrived within ``startup_timeout``.
        BinaryNotFoundError: Binary discovery failed.
    """
    binary = discover_binary()

    # Use env vars for port overrides (the binary reads --config YAML;
    # CLI has no --http-port / --tcp-port flags — verified in cli.rs).
    # Security (T-03-04-04): pass a minimal copy of the current env rather than
    # constructing a bare minimal dict, so DYLD_LIBRARY_PATH / locale etc. are
    # inherited when the binary needs them.  We explicitly override only the
    # keys we care about.
    # Phase 13.5.1 Plan 05 (Rule 3 blocking-issue auto-fix): pin
    # BEAVA_WAL_DIR + BEAVA_SNAPSHOT_DIR to unique per-spawn tmpdirs so
    # parallel test spawns don't collide on the default ./beava-wal/
    # location (the binary fails on "File exists" if a prior process left
    # behind a 0-byte WAL file). Each spawn gets a fresh dir under
    # ``$TMPDIR/beava-embed-<pid>-<unix-ms>-<unique>/``; teardown_process
    # leaves the dirs in place (small + cheaply gc'd by tmpfs reaper).
    unique = f"{os.getpid()}-{int(time.time() * 1000)}-{os.urandom(4).hex()}"
    spawn_root = Path(tempfile.gettempdir()) / f"beava-embed-{unique}"
    wal_dir = spawn_root / "wal"
    snapshot_dir = spawn_root / "snapshots"
    wal_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    # Phase 13.5.1 Plan 07e (Deviation 2 formalization): register an
    # atexit cleanup so the per-spawn tmpdir doesn't accumulate disk
    # over the life of a long-running test process. ``ignore_errors=True``
    # so a partial-cleanup race (e.g. the binary still holding a file
    # handle at interpreter shutdown) doesn't crash teardown. The OS
    # tmpfs reaper is a backstop for the no-cleanup-ran cases (e.g. SIGKILL
    # of the Python process).
    atexit.register(shutil.rmtree, str(spawn_root), ignore_errors=True)

    env = {
        **os.environ,
        "BEAVA_LISTEN_ADDR": "127.0.0.1:0",
        "BEAVA_TCP_PORT": "0",
        "BEAVA_DEV_ENDPOINTS": "1",
        "BEAVA_WAL_DIR": str(wal_dir),
        "BEAVA_SNAPSHOT_DIR": str(snapshot_dir),
    }
    # Phase 13.5 D-05 (cross-amendment from 13.4 D-03): test_mode kwarg
    # propagates BEAVA_TEST_MODE=1 to the spawned binary, gating OP_RESET
    # and other test-only opcodes server-side.
    if test_mode:
        env["BEAVA_TEST_MODE"] = "1"

    # Pass --config /dev/null so the binary starts with all-defaults regardless of
    # the caller's CWD (the default config path is ./beava.yaml may not exist).
    # All meaningful settings are overridden via env vars above.
    #
    # NOTE: The beava binary writes JSON structured logs to STDOUT (not stderr).
    # The human-readable banner line also goes to stdout.  We must capture stdout
    # to parse the bind-address log lines; stderr is unused and discarded.
    proc: subprocess.Popen[bytes] = subprocess.Popen(
        [str(binary), "--config", "/dev/null"],
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
                # Non-JSON line (e.g. the banner "beava v0.1.0") — log at DEBUG
                # post-startup; skip silently before startup is confirmed.
                if ready.is_set():
                    _log.debug("non-json stdout: %s", line)
                continue

            kind = rec.get("kind", "")
            if kind == "server.http_bound":
                http_addr.append(rec.get("addr", ""))
            elif kind == "server.tcp_bound":
                tcp_addr.append(rec.get("addr", ""))

            if http_addr and tcp_addr:
                if not ready.is_set():
                    ready.set()
                else:
                    # Post-startup structured log line — forward at DEBUG.
                    _log.debug("%s", line)
        # Stdout pipe closed (process exited) — signal readiness in case teardown
        # happened before both bind events arrived (prevents full startup_timeout wait).
        ready.set()

    t = threading.Thread(target=_reader, daemon=True)
    t.start()

    ready.wait(timeout=startup_timeout)

    # Check whether both bind events actually arrived.  ready may have been set
    # by the EOF sentinel (process exited early) rather than by both bind events.
    if not (http_addr and tcp_addr):
        proc.kill()
        proc.wait()
        if proc.stdout:
            proc.stdout.close()  # unblocks the _reader thread
        raise TimeoutError(
            f"embed-mode server did not bind within {startup_timeout}s "
            f"(http_addr={http_addr}, tcp_addr={tcp_addr}). "
            f"Check that the beava binary starts correctly."
        )

    return proc, f"http://{http_addr[0]}", f"tcp://{tcp_addr[0]}", env


def teardown_process(proc: subprocess.Popen[bytes], timeout: float = 5.0) -> None:
    """Send SIGTERM; wait for ``timeout`` seconds; SIGKILL if still running.

    Args:
        proc: The subprocess to terminate.
        timeout: Seconds to wait for graceful shutdown before SIGKILL.
    """
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
