"""Phase 5 Python acceptance smokes — ROADMAP SC1..SC6 end-to-end via SDK + HTTP.

SC1: group_by().agg() registers a Table derivation; GET /registry shows output_kind=table.
SC2: push via /dev/apply_events updates the aggregation; GET /get returns the value.
SC3: all 8 core operators pass table-driven correctness tests (SDK register path).
SC4: identical event stream to two fresh servers → byte-identical GET /get responses.
SC5: windowless count and ratio work (window= omitted in SDK call).
SC6: unknown field at register → 400; aggregation on Table source → 400.

Uses:
  - bv.App(http_url) to register via SDK
  - httpx directly for /dev/apply_events (SDK push is Phase 6)
  - httpx for GET /get/{feature}/{key} and POST /get queries
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

import beava as bv

pytestmark = pytest.mark.phase5

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@bv.event
class Transaction:
    # Plan 12.6-08: event_time field removed from class form per the
    # no-event-time pivot. The server stamps wall-clock arrival time.
    user_id: str
    amount: float
    status: str


def _apply(http_url: str, source: str, event_time_ms: int, row: dict[str, Any]) -> None:
    """POST /dev/apply_events and assert 200.

    Plan 12.6-06 D-03 hard rip: the legacy `event_time_ms` request body field
    has been removed; the apply path uses server-side wall-clock at dispatch.
    The function parameter is kept for caller compatibility but no longer sent
    on the wire.
    """
    _ = event_time_ms  # kept for caller compat, no longer sent
    resp = httpx.post(
        f"{http_url}/dev/apply_events",
        json={"source": source, "row": row},
        timeout=10.0,
    )
    assert resp.status_code == 200, f"apply_events failed: {resp.text}"


def _get(http_url: str, feature: str, key: str) -> Any:
    """GET /get/{feature}/{key} and return the 'value' field."""
    resp = httpx.get(f"{http_url}/get/{feature}/{key}", timeout=10.0)
    assert resp.status_code == 200, f"GET /get/{feature}/{key} failed: {resp.text}"
    return resp.json().get("value")


def _post_get(http_url: str, keys: list[str], features: list[str]) -> dict[str, Any]:
    """POST /get batch and return the flat per-entity dict.

    Phase 13.4 Plan 02 / Phase 13.0-15 wire-spec: the multi-feature
    batched read now returns the flat dict `{entity_id: {feature: value}}`
    directly, with no `{"result": ...}` envelope.
    """
    resp = httpx.post(
        f"{http_url}/get",
        json={"keys": keys, "features": features},
        timeout=10.0,
    )
    assert resp.status_code == 200, f"POST /get failed: {resp.text}"
    body = resp.json()
    assert "result" not in body, (
        f"Plan 13.4-02: result envelope must be absent, got {body!r}"
    )
    return body  # type: ignore[no-any-return]


# ---------------------------------------------------------------------------
# SC1: group_by().agg() produces a Table with correct registry metadata
# ---------------------------------------------------------------------------


def test_sc1_groupby_agg_produces_table(beava_server: tuple[str, str]) -> None:
    """SC1: SDK register of group_by().agg() produces a Table derivation visible in /registry."""
    http_url, _tcp_url = beava_server

    TxCount5m = Transaction.group_by("user_id").agg(cnt=bv.count(window="5m"))
    # Assign a stable name for querying
    TxCount5m._name = "TxCount5m"  # type: ignore[attr-defined]

    with bv.App(http_url) as app:
        resp = app.register(Transaction, TxCount5m)
    assert resp.get("status") == "ok", f"register failed: {resp}"

    # Verify /registry shows the derivation as a table.
    registry = httpx.get(f"{http_url}/registry", timeout=5.0).json()
    derivations = registry.get("derivations", {})
    assert "TxCount5m" in derivations, (
        f"TxCount5m not found in registry, got: {list(derivations.keys())}"
    )
    deriv = derivations["TxCount5m"]
    assert deriv["output_kind"] == "table", (
        f"SC1: output_kind must be 'table', got: {deriv['output_kind']!r}"
    )
    pk = deriv.get("table_primary_key", [])
    assert "user_id" in pk, f"SC1: table_primary_key must contain 'user_id': {pk}"
    schema_fields = deriv["schema"]["fields"]
    assert "cnt" in schema_fields, f"SC1: schema must contain 'cnt': {schema_fields}"
    assert "user_id" in schema_fields, f"SC1: schema must contain 'user_id': {schema_fields}"


# ---------------------------------------------------------------------------
# SC2: push via /dev/apply_events → GET /get returns updated count
# ---------------------------------------------------------------------------


def test_sc2_push_then_get_returns_count(beava_server: tuple[str, str]) -> None:
    """SC2: 10 events pushed via /dev/apply_events; GET /get/cnt/alice → 10."""
    http_url, _tcp_url = beava_server

    TxCount5m = Transaction.group_by("user_id").agg(cnt=bv.count(window="5m"))
    TxCount5m._name = "TxCount5m"  # type: ignore[attr-defined]

    with bv.App(http_url) as app:
        app.register(Transaction, TxCount5m)

    for i in range(10):
        _apply(
            http_url,
            "Transaction",
            1_000_000 + i * 1000,
            {"user_id": "alice", "amount": 50.0, "status": "ok"},
        )

    result = _post_get(http_url, ["alice"], ["cnt"])
    assert result["alice"]["cnt"] == 10, (
        f"SC2: expected count=10, got: {result}"
    )


def test_sc2_where_predicate_filters_events(beava_server: tuple[str, str]) -> None:
    """SC2: where= predicate filters events; only matching events are counted."""
    http_url, _tcp_url = beava_server

    TxCountOk = Transaction.group_by("user_id").agg(
        cnt_ok=bv.count(window="5m", where=bv.col("status") == "ok")
    )
    TxCountOk._name = "TxCountOk"  # type: ignore[attr-defined]

    with bv.App(http_url) as app:
        app.register(Transaction, TxCountOk)

    base_time = 1_000_000
    # 7 ok events
    for i in range(7):
        _apply(
            http_url,
            "Transaction",
            base_time + i * 1000,
            {"user_id": "alice", "amount": 10.0, "status": "ok"},
        )
    # 3 failed events
    for i in range(7, 10):
        _apply(
            http_url,
            "Transaction",
            base_time + i * 1000,
            {"user_id": "alice", "amount": 10.0, "status": "failed"},
        )

    result = _post_get(http_url, ["alice"], ["cnt_ok"])
    assert result["alice"]["cnt_ok"] == 7, (
        f"SC2 where: expected 7, got: {result}"
    )


# ---------------------------------------------------------------------------
# SC3: all 8 core operators pass correctness tests via SDK register path
# ---------------------------------------------------------------------------


def test_sc3_all_8_operators_e2e(beava_server: tuple[str, str]) -> None:
    """SC3: All 8 operators registered via SDK; correctness verified after 6 events.

    Events: 5 with status=ok, amounts [10,20,30,40,50]; 1 with status=bad, amount=99.
    Expected:
      cnt=6, sum=249.0, avg=41.5, min=10.0, max=99.0,
      variance=993.5, stddev=sqrt(993.5), ratio_ok=5/6
    """
    http_url, _tcp_url = beava_server

    AggAll8 = Transaction.group_by("user_id").agg(
        cnt=bv.count(window="1h"),
        total=bv.sum("amount", window="1h"),
        avg_amt=bv.avg("amount", window="1h"),
        min_amt=bv.min("amount", window="1h"),
        max_amt=bv.max("amount", window="1h"),
        var_amt=bv.variance("amount", window="1h"),
        std_amt=bv.stddev("amount", window="1h"),
        ratio_ok=bv.ratio(window="1h", where=bv.col("status") == "ok"),
    )
    AggAll8._name = "AggAll8"  # type: ignore[attr-defined]

    with bv.App(http_url) as app:
        app.register(Transaction, AggAll8)

    base_time = 1_000_000
    amounts = [10.0, 20.0, 30.0, 40.0, 50.0]
    for i, amt in enumerate(amounts):
        _apply(
            http_url,
            "Transaction",
            base_time + i * 1000,
            {"user_id": "alice", "amount": amt, "status": "ok"},
        )
    # 1 bad event (amount=99, status=bad)
    _apply(
        http_url,
        "Transaction",
        base_time + 5 * 1000,
        {"user_id": "alice", "amount": 99.0, "status": "bad"},
    )

    features = ["cnt", "total", "avg_amt", "min_amt", "max_amt", "var_amt", "std_amt", "ratio_ok"]
    result = _post_get(http_url, ["alice"], features)
    alice = result["alice"]

    tol = 1e-6

    assert alice["cnt"] == 6, f"SC3 count: expected 6, got {alice['cnt']}"

    assert abs(alice["total"] - 249.0) < tol, (
        f"SC3 sum: expected 249.0, got {alice['total']}"
    )
    assert abs(alice["avg_amt"] - 41.5) < tol, (
        f"SC3 avg: expected 41.5, got {alice['avg_amt']}"
    )
    assert abs(alice["min_amt"] - 10.0) < tol, (
        f"SC3 min: expected 10.0, got {alice['min_amt']}"
    )
    assert abs(alice["max_amt"] - 99.0) < tol, (
        f"SC3 max: expected 99.0, got {alice['max_amt']}"
    )
    # Sample variance of [10,20,30,40,50,99]: mean=41.5, sum_sq_dev=4967.5, var=4967.5/5=993.5
    assert abs(alice["var_amt"] - 993.5) < 1e-4, (
        f"SC3 variance: expected 993.5, got {alice['var_amt']}"
    )
    expected_std = 993.5**0.5
    assert abs(alice["std_amt"] - expected_std) < 1e-4, (
        f"SC3 stddev: expected {expected_std}, got {alice['std_amt']}"
    )
    expected_ratio = 5.0 / 6.0
    assert abs(alice["ratio_ok"] - expected_ratio) < 1e-9, (
        f"SC3 ratio: expected {expected_ratio:.6f}, got {alice['ratio_ok']}"
    )


# ---------------------------------------------------------------------------
# SC4: replay determinism — same events → byte-identical GET responses
# ---------------------------------------------------------------------------


def test_sc4_replay_determinism(
    beava_binary: Any, tmp_path: Any, pytestconfig: Any
) -> None:
    """SC4 (integration-layer gate): two fresh server instances, same 100-event stream,
    byte-identical GET /get responses.

    SC4 layered coverage:
      - Plan 05-01's windowed_replay_determinism proves byte-identical INTERNAL state
        at the WindowedOp struct level.
      - This test proves byte-identical OBSERVABLE output through the full apply-loop
        + registry + GET wire path after the same 100 events on two fresh servers.
    Together they form the complete SC4 proof per Plan 05-08 design.
    """
    import os
    import subprocess
    import threading

    def _spawn_server() -> tuple[str, subprocess.Popen[bytes]]:
        env = {
            **os.environ,
            "BEAVA_LISTEN_ADDR": "127.0.0.1:0",
            "BEAVA_TCP_PORT": "0",
            "BEAVA_DEV_ENDPOINTS": "1",
        }
        proc = subprocess.Popen(
            [str(beava_binary), "--config", "/dev/null"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        http_addr: list[str] = []
        ready = threading.Event()

        def _reader() -> None:
            assert proc.stdout is not None
            for raw in proc.stdout:
                line = raw.decode("utf-8", errors="replace").rstrip()
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("kind") == "server.http_bound":
                    http_addr.append(rec["addr"])
                    ready.set()

        t = threading.Thread(target=_reader, daemon=True)
        t.start()
        if not ready.wait(timeout=5.0):
            proc.kill()
            proc.wait()
            pytest.fail("server did not bind within 5s")
        return f"http://{http_addr[0]}", proc

    def _run_instance(http_url: str) -> list[str]:
        """Register, push 100 events, collect GET responses for u0/u1/u2."""
        TxCount5m = Transaction.group_by("user_id").agg(cnt=bv.count(window="5m"))
        TxCount5m._name = "TxCount5mReplay"  # type: ignore[attr-defined]
        with bv.App(http_url) as app:
            app.register(Transaction, TxCount5m)

        for i in range(100):
            _apply(
                http_url,
                "Transaction",
                i * 1000,
                {"user_id": f"u{i % 3}", "amount": float(i), "status": "ok"},
            )

        bodies = []
        for key in ["u0", "u1", "u2"]:
            resp = httpx.get(f"{http_url}/get/cnt/{key}", timeout=10.0)
            bodies.append(resp.text)
        return bodies

    url_a, proc_a = _spawn_server()
    try:
        bodies_a = _run_instance(url_a)
    finally:
        proc_a.terminate()
        proc_a.wait(timeout=5)

    url_b, proc_b = _spawn_server()
    try:
        bodies_b = _run_instance(url_b)
    finally:
        proc_b.terminate()
        proc_b.wait(timeout=5)

    for i, key in enumerate(["u0", "u1", "u2"]):
        assert bodies_a[i] == bodies_b[i], (
            f"SC4 replay-determinism FAILED for key={key}:\n"
            f"  run A: {bodies_a[i]!r}\n"
            f"  run B: {bodies_b[i]!r}"
        )


# ---------------------------------------------------------------------------
# SC5: windowless (lifetime) operators
# ---------------------------------------------------------------------------


def test_sc5_lifetime_count(beava_server: tuple[str, str]) -> None:
    """SC5: bv.count() without window= counts all events (lifetime)."""
    http_url, _tcp_url = beava_server

    TxLifetime = Transaction.group_by("user_id").agg(cnt_lifetime=bv.count())
    TxLifetime._name = "TxLifetime"  # type: ignore[attr-defined]

    with bv.App(http_url) as app:
        app.register(Transaction, TxLifetime)

    # Push 50 events spread over many days (no window expiry possible).
    for i in range(50):
        _apply(
            http_url,
            "Transaction",
            i * 86_400_000,  # 1 day apart
            {"user_id": "alice", "amount": 1.0, "status": "ok"},
        )

    result = _post_get(http_url, ["alice"], ["cnt_lifetime"])
    assert result["alice"]["cnt_lifetime"] == 50, (
        f"SC5 lifetime count: expected 50, got: {result}"
    )


def test_sc5_lifetime_ratio(beava_server: tuple[str, str]) -> None:
    """SC5: bv.ratio(where=...) without window= gives lifetime ratio."""
    http_url, _tcp_url = beava_server

    TxRatio = Transaction.group_by("user_id").agg(
        ratio_ok=bv.ratio(where=bv.col("status") == "ok")
    )
    TxRatio._name = "TxRatioLifetime"  # type: ignore[attr-defined]

    with bv.App(http_url) as app:
        app.register(Transaction, TxRatio)

    # 3 ok + 7 bad = ratio 0.3
    for i in range(3):
        _apply(
            http_url, "Transaction", 1_000_000 + i * 1000,
            {"user_id": "alice", "amount": 10.0, "status": "ok"},
        )
    for i in range(3, 10):
        _apply(
            http_url, "Transaction", 1_000_000 + i * 1000,
            {"user_id": "alice", "amount": 10.0, "status": "bad"},
        )

    result = _post_get(http_url, ["alice"], ["ratio_ok"])
    ratio = result["alice"]["ratio_ok"]
    assert abs(ratio - 0.3) < 1e-9, f"SC5 lifetime ratio: expected 0.3, got {ratio}"


# ---------------------------------------------------------------------------
# SC6: validation errors at registration
# ---------------------------------------------------------------------------


def test_sc6_unknown_field_rejected(beava_server: tuple[str, str]) -> None:
    """SC6: bv.sum(field='nonexistent') at register → 400 aggregation_unknown_field."""
    http_url, _tcp_url = beava_server

    # Register Transaction first so the server knows its schema.
    with bv.App(http_url) as app:
        app.register(Transaction)

    # Bypass SDK validation to send a raw payload with a bad field name.
    payload = {
        "nodes": [
            {
                "kind": "derivation",
                "name": "BadAgg",
                "output_kind": "table",
                "upstreams": ["Transaction"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {
                            "bad_sum": {
                                "op": "sum",
                                "params": {"field": "nonexistent", "window": "5m"},
                            }
                        },
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "bad_sum": "f64"},
                    "optional_fields": [],
                },
                "table_primary_key": ["user_id"],
            }
        ]
    }
    resp = httpx.post(
        f"{http_url}/register",
        content=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        timeout=10.0,
    )
    assert resp.status_code == 400, f"SC6: expected 400, got {resp.status_code}: {resp.text}"
    body = resp.json()
    assert body["error"]["code"] == "aggregation_unknown_field", (
        f"SC6: expected aggregation_unknown_field, got: {body['error']['code']!r}"
    )
    reason = body["error"].get("reason", "")
    assert "nonexistent" in reason, (
        f"SC6: error reason must mention 'nonexistent': {reason!r}"
    )


def test_sc6_aggregation_on_table_rejected(beava_server: tuple[str, str]) -> None:
    """SC6: aggregation on a Table source → 400 aggregation_on_table_not_supported."""
    http_url, _tcp_url = beava_server

    # Register Transaction + a table derivation from it.
    TxTable = Transaction.group_by("user_id").agg(cnt=bv.count(window="5m"))
    TxTable._name = "TxTable"  # type: ignore[attr-defined]

    with bv.App(http_url) as app:
        app.register(Transaction, TxTable)

    # Now attempt to aggregate on the Table derivation (should fail).
    payload = {
        "nodes": [
            {
                "kind": "derivation",
                "name": "BadNestedAgg",
                "output_kind": "table",
                "upstreams": ["TxTable"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {"cnt2": {"op": "count", "params": {"window": "1h"}}},
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "cnt2": "i64"},
                    "optional_fields": [],
                },
                "table_primary_key": ["user_id"],
            }
        ]
    }
    resp = httpx.post(
        f"{http_url}/register",
        content=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        timeout=10.0,
    )
    assert resp.status_code == 400, (
        f"SC6: aggregation on Table must return 400, got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert body["error"]["code"] == "aggregation_on_table_not_supported", (
        f"SC6: expected aggregation_on_table_not_supported, got: {body['error']['code']!r}"
    )
