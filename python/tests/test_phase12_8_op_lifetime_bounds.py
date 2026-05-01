"""Phase 12.8 Plan 04 — Python E2E tests for op-lifetime-bound classification.

Per CONTEXT.md D-03 (locked 2026-05-01): every lifetime-mode aggregation op
declares a finite per-entity memory ceiling at register-time. The 4th JSON-
prelude shim ``pre_check_unbounded_op_in_lifetime_mode`` walks the registered
DAG and rejects any windowless op whose lifetime memory bound is ``Unbounded``
(unknown op) OR ``BoundedByRequiredKwarg`` with the kwarg missing/zero.

Plan 01 (Wave 1) shipped the shim with a placeholder helper that returns
``Unbounded`` for every op-string + an env-gate ``BEAVA_MEMORY_GOV_ENFORCE``
defaulting OFF. Plan 04 (Wave 2 — this plan) populates the 53-variant /
54-op-string classification table. **Plan 04 does NOT flip the env-gate;**
Plan 06 (Wave 3) owns that flip alongside the metrics-counter wiring.

These 5 end-to-end tests verify the populated table works through the HTTP
``/register`` path. Each test spawns a fresh ``beava`` subprocess with
``BEAVA_MEMORY_GOV_ENFORCE=1`` baked into the env at spawn-time so the shim
fires (the helper does a per-call ``std::env::var`` read, but the subprocess
inherits its env from spawn — so we control which case each test exercises by
setting the env before ``Popen``).

The 6th test (``test_env_var_zero_disables_enforcement``) is owned by Plan 06
and lands alongside the env-gate flip.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
from pathlib import Path
from typing import Any, Generator

import httpx
import pytest

# ─── Helpers: spawn beava server with custom env ─────────────────────────────


def _spawn_beava_with_enforce(
    binary: Path, wal_dir: Path, snap_dir: Path
) -> tuple[subprocess.Popen[bytes], str]:
    """Spawn a beava server with BEAVA_MEMORY_GOV_ENFORCE=1 baked into env.

    Returns (process, http_url). Caller is responsible for terminating the
    process. Uses the same port-discovery + JSON-log-line pattern as the
    standard ``beava_server`` fixture in conftest.py — but with the extra env
    var so the 4th JSON-prelude shim fires for every register call. Each test
    spawns a fresh subprocess with its own temp WAL+snapshot dirs; the binary
    ``serve_with_dirs`` would otherwise reuse `./beava-wal/` from the test
    runner's CWD and conflict across tests.
    """
    env = {
        **os.environ,
        "BEAVA_LISTEN_ADDR": "127.0.0.1:0",
        "BEAVA_TCP_PORT": "0",
        "BEAVA_DEV_ENDPOINTS": "1",
        "BEAVA_MEMORY_GOV_ENFORCE": "1",
        "BEAVA_WAL_DIR": str(wal_dir),
        "BEAVA_SNAPSHOT_DIR": str(snap_dir),
    }
    proc = subprocess.Popen(
        [str(binary), "--config", "/dev/null"],
        stdout=subprocess.PIPE,
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

    if not ready.wait(timeout=5.0):
        proc.kill()
        proc.wait()
        if proc.stdout:
            proc.stdout.close()
        pytest.fail(
            f"beava server (enforce=1) did not emit bind log lines within 5s; "
            f"http_addr={http_addr}, tcp_addr={tcp_addr}"
        )

    http_url = f"http://{http_addr[0]}"
    return proc, http_url


@pytest.fixture
def beava_server_enforce(
    beava_binary: Path, tmp_path: Path
) -> Generator[str, None, None]:
    """Per-test fixture: beava server with BEAVA_MEMORY_GOV_ENFORCE=1.

    Yields the HTTP URL. Each invocation gets fresh per-test WAL + snapshot
    directories under ``tmp_path`` so subprocesses never collide. Cleans up
    the subprocess on teardown (SIGTERM with 5 s grace, then SIGKILL).
    """
    wal_dir = tmp_path / "wal"
    snap_dir = tmp_path / "snap"
    proc, http_url = _spawn_beava_with_enforce(beava_binary, wal_dir, snap_dir)
    try:
        yield http_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


# ─── Helper: build a register payload with a single derivation + agg ─────────


def _payload_with_agg(
    deriv_name: str, feature_name: str, op: str, params: dict[str, Any]
) -> dict[str, Any]:
    """Build a register JSON payload with a single derivation that group_by's
    user_id and runs one aggregation op. Mirrors the layout used by the Plan 01
    integration test ``crates/beava-server/tests/phase12_8_unbounded_op_in_lifetime_mode.rs``.
    """
    return {
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": [],
                },
            },
            {
                "kind": "derivation",
                "name": deriv_name,
                "output_kind": "event",
                "upstreams": ["Tx"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {feature_name: {"op": op, "params": params}},
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", feature_name: "f64"},
                    "optional_fields": [],
                },
            },
        ]
    }


def _post_register(http_url: str, payload: dict[str, Any]) -> httpx.Response:
    return httpx.post(
        f"{http_url}/register",
        content=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        timeout=10.0,
    )


# ─── Test 16: count lifetime succeeds (O1 → accept) ──────────────────────────


def test_register_count_lifetime_succeeds(beava_server_enforce: str) -> None:
    """Windowless ``count`` is O(1) lifetime — must succeed under enforce=1.

    Plan 01's stub classified count as Unbounded (rejected). Plan 04's table
    classifies count as O1 (accepted).
    """
    payload = _payload_with_agg("ByUser", "cnt", "count", {})
    resp = _post_register(beava_server_enforce, payload)
    assert resp.status_code == 200, (
        f"windowless count (O1) must succeed in lifetime mode under enforce=1; "
        f"got status={resp.status_code}, body={resp.text!r}"
    )


# ─── Test 17: histogram lifetime no buckets → reject ────────────────────────


def test_register_histogram_lifetime_no_buckets_rejected(
    beava_server_enforce: str,
) -> None:
    """Windowless ``histogram`` without ``buckets`` array → 400 +
    ``unbounded_op_in_lifetime_mode`` error code.

    Plan 04 elevates histogram from "soft default" to BoundedByRequiredKwarg.
    The existing wire convention is ``params.buckets: Vec<f64>``; an empty or
    missing buckets array is rejected at register-time.
    """
    payload = _payload_with_agg(
        "ByUser", "h", "histogram", {"field": "amount"}
    )  # buckets missing
    resp = _post_register(beava_server_enforce, payload)
    assert resp.status_code == 400, (
        f"histogram without buckets must be rejected; got status={resp.status_code}, "
        f"body={resp.text!r}"
    )
    body = resp.json()
    assert body["error"]["code"] == "unbounded_op_in_lifetime_mode", (
        f"expected unbounded_op_in_lifetime_mode, got body={body!r}"
    )


# ─── Test 18: histogram lifetime with buckets → succeed ──────────────────────


def test_register_histogram_lifetime_with_buckets_succeeds(
    beava_server_enforce: str,
) -> None:
    """Windowless ``histogram`` with non-empty ``buckets`` array → 200.

    The user supplies the explicit memory bound (the bucket boundaries), so
    register-time validation accepts.
    """
    payload = _payload_with_agg(
        "ByUser",
        "h",
        "histogram",
        {"field": "amount", "buckets": [10.0, 50.0, 100.0, 500.0]},
    )
    resp = _post_register(beava_server_enforce, payload)
    assert resp.status_code == 200, (
        f"histogram with explicit buckets array must succeed; "
        f"got status={resp.status_code}, body={resp.text!r}"
    )


# ─── Test 19: first_n lifetime no n → reject ─────────────────────────────────


def test_register_first_n_lifetime_no_n_rejected(beava_server_enforce: str) -> None:
    """Windowless ``first_n`` without ``n`` kwarg → 400 + unbounded_op_in_lifetime_mode.

    Plan 04: first_n is BoundedByRequiredKwarg("n"). Missing n → reject.
    """
    payload = _payload_with_agg("ByUser", "f5", "first_n", {"field": "amount"})
    resp = _post_register(beava_server_enforce, payload)
    assert resp.status_code == 400, (
        f"first_n without n must be rejected; got status={resp.status_code}, "
        f"body={resp.text!r}"
    )
    body = resp.json()
    assert body["error"]["code"] == "unbounded_op_in_lifetime_mode"
    reason = body["error"]["reason"]
    assert "n" in reason, f"reason should mention `n`; got: {reason!r}"


# ─── Test 20: first_n lifetime with n=5 → succeed ────────────────────────────


def test_register_first_n_lifetime_with_n_5_succeeds(beava_server_enforce: str) -> None:
    """Windowless ``first_n`` with ``n=5`` → 200.

    User supplies the explicit memory bound; register-time validation accepts.
    """
    payload = _payload_with_agg(
        "ByUser", "f5", "first_n", {"field": "amount", "n": 5}
    )
    resp = _post_register(beava_server_enforce, payload)
    assert resp.status_code == 200, (
        f"first_n with n=5 must succeed; got status={resp.status_code}, "
        f"body={resp.text!r}"
    )
