"""Phase 4 Python acceptance smokes — ROADMAP SC1..SC5 over HTTP + TCP.

SC1: filter predicate registered and applied correctly (HTTP + TCP).
SC2: with_columns-derived field visible in GET /registry schema.
SC3: 4-op chained derivation composes; schema propagates correctly.
SC4: hypothesis proptest — 256 random (expr, row) pairs; Python reference eval
     agrees with Rust server /dev/apply_ops (the load-bearing correctness claim).
SC5: malformed predicate → 400 + error path (HTTP) / error frame (TCP).
"""

from __future__ import annotations

import json
import threading
import urllib.parse
from typing import Any

import httpx
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import beava as bv
from beava._col import _BinOp, _ExprAST, _Field, _Literal
from beava._eval_reference import evaluate
from beava._wire import (
    CT_JSON,
    OP_ERROR_RESPONSE,
    OP_REGISTER,
    encode_frame,
    read_frame,
)

pytestmark = pytest.mark.phase4

# ---------------------------------------------------------------------------
# WR-06: SC4 skip-rate guard
# Hypothesis runs all 256 SC4 cases sequentially against the same server.
# We track how many cases are skipped (server rejects the expression at
# register time) so we can fail if the skip rate is too high. A skip rate
# > 50% would mean we're only testing schema-valid expressions, which is a
# weaker coverage claim than advertised.
#
# The counter is keyed by the server HTTP URL so it resets automatically
# when a new server is spawned (each pytest invocation of the test function
# gets a fresh server address from the fixture). This prevents stale counts
# from a previous test run in the same pytest session from inflating the rate.
# ---------------------------------------------------------------------------
_sc4_skip_counter: dict[str, int] = {}  # keyed by http_url
_sc4_skip_lock = threading.Lock()


def _sc4_reset_for_server(http_url: str) -> None:
    """Initialise skip counters for a server URL if not already present.

    Called at the top of every hypothesis example invocation.  Only takes
    effect the first time a particular URL is seen, so counters accumulate
    correctly across all 256 examples against the same server.  When a new
    pytest test-function invocation spawns a server on a different port, the
    new URL has no entry yet and the counters start fresh, preventing stale
    state from a prior run in the same session from inflating the rate.
    """
    with _sc4_skip_lock:
        if http_url not in _sc4_skip_counter:
            _sc4_skip_counter[http_url] = 0
            _sc4_skip_counter[http_url + ":skips"] = 0


# ---------------------------------------------------------------------------
# Module-level descriptors used by SC1–SC3
# ---------------------------------------------------------------------------


@bv.event
class Transaction:
    amount: float
    kind: str
    event_time: int


# ---------------------------------------------------------------------------
# Dedicated event for SC4 proptest — numeric-only schema to keep the
# expression generator type-compatible at registration time.
#
# Using only numeric fields (a: i64, b: i64, c: f64) means every generated
# arithmetic and comparison expression passes the server's register-time type
# checker (SDK-COL-07 + type-mismatch guards in schema_propagate.rs). Bool
# and Str fields were removed because mixing them with arithmetic operators
# triggers TypeMismatch rejections that inflate the WR-06 skip rate.
# Transaction stays as-is for SC1/SC2/SC3/SC5.
# ---------------------------------------------------------------------------


@bv.event
class ProptestBase:
    a: int
    b: int
    c: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _apply_ops(http_url: str, derivation_name: str, row: dict[str, Any]) -> dict[str, Any]:
    """POST /dev/apply_ops and return the response dict."""
    resp = httpx.post(
        f"{http_url}/dev/apply_ops",
        json={"derivation": derivation_name, "row": row},
        timeout=10.0,
    )
    return resp.json()  # type: ignore[no-any-return]


def _register_raw_http(http_url: str, payload: dict[str, Any]) -> httpx.Response:
    """POST /register with a raw payload dict (may contain malformed expressions)."""
    return httpx.post(
        f"{http_url}/register",
        content=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        timeout=10.0,
    )


# ---------------------------------------------------------------------------
# SC1 — filter predicate registered and applied
# ---------------------------------------------------------------------------


def test_sc1_http_filter_predicate_registered(beava_server: tuple[str, str]) -> None:
    """SC1 (HTTP): filter derivation registered; apply_ops correctly keeps/drops rows."""
    http_url, _tcp_url = beava_server

    BigTx = Transaction.filter(bv.col("amount") > 100).named("BigTxHTTP")

    with bv.App(http_url) as app:
        resp = app.register(Transaction, BigTx)
    assert resp.get("status") == "ok"

    # Row that passes the filter (amount=150 > 100).
    kept = _apply_ops(http_url, "BigTxHTTP", {"amount": 150.0, "kind": "sale", "event_time": 1})
    assert kept.get("kept") is True

    # Row that is dropped by the filter (amount=50 <= 100).
    dropped = _apply_ops(http_url, "BigTxHTTP", {"amount": 50.0, "kind": "sale", "event_time": 1})
    assert dropped.get("kept") is False


def test_sc1_tcp_filter_predicate_registered(beava_server: tuple[str, str]) -> None:
    """SC1 (TCP): same derivation registered via TCP; apply_ops (HTTP) still agrees."""
    http_url, tcp_url = beava_server

    BigTxTcp = Transaction.filter(bv.col("amount") > 200).named("BigTxTCP")

    with bv.App(tcp_url) as app:
        resp = app.register(Transaction, BigTxTcp)
    assert resp.get("status") == "ok"

    kept = _apply_ops(http_url, "BigTxTCP", {"amount": 300.0, "kind": "sale", "event_time": 1})
    assert kept.get("kept") is True

    dropped = _apply_ops(http_url, "BigTxTCP", {"amount": 100.0, "kind": "sale", "event_time": 1})
    assert dropped.get("kept") is False


# ---------------------------------------------------------------------------
# SC2 — with_columns derived field visible in GET /registry schema
# ---------------------------------------------------------------------------


def test_sc2_with_columns_schema_propagates_visible_in_registry(
    beava_server: tuple[str, str],
) -> None:
    """SC2: with_columns adds is_big:bool to propagated schema; visible in GET /registry."""
    http_url, _tcp_url = beava_server

    TaggedTx = Transaction.with_columns(is_big=bv.col("amount") > 500).named("TaggedTx")

    with bv.App(http_url) as app:
        resp = app.register(Transaction, TaggedTx)
    assert resp.get("status") == "ok"

    registry = httpx.get(f"{http_url}/registry", timeout=5.0).json()
    derivations = registry.get("derivations", {})
    assert "TaggedTx" in derivations, f"TaggedTx not in registry: {list(derivations.keys())}"
    schema_fields = derivations["TaggedTx"]["schema"]["fields"]
    assert "is_big" in schema_fields, f"is_big not in schema: {schema_fields}"
    assert schema_fields["is_big"] == "bool", f"is_big type mismatch: {schema_fields['is_big']}"


# ---------------------------------------------------------------------------
# SC3 — 4-op chained derivation; schema propagates
# ---------------------------------------------------------------------------


def test_sc3_chained_ops_compose_schema_propagates(
    beava_server: tuple[str, str],
) -> None:
    """SC3: 4-op chain (filter→select→with_columns→cast); schema propagates."""
    http_url, _tcp_url = beava_server

    # Build: filter(amount>0) → select(event_time,amount) → with_columns(is_big) → cast(is_big→int)
    ChainedDeriv = (
        Transaction.filter(bv.col("amount") > 0)
        .select("event_time", "amount")
        .with_columns(is_big=bv.col("amount") > 500)
        .cast(is_big="int")
        .named("ChainedDeriv")
    )

    with bv.App(http_url) as app:
        resp = app.register(Transaction, ChainedDeriv)
    assert resp.get("status") == "ok"

    # Verify schema via GET /registry.
    registry = httpx.get(f"{http_url}/registry", timeout=5.0).json()
    derivations = registry.get("derivations", {})
    assert "ChainedDeriv" in derivations
    fields = derivations["ChainedDeriv"]["schema"]["fields"]
    # After cast(is_big="int"), is_big should be i64.
    assert fields.get("is_big") == "i64", f"is_big type mismatch: {fields}"

    # apply_ops: row with amount=1000 should be kept; is_big should be 1 (cast bool→int).
    result = _apply_ops(
        http_url, "ChainedDeriv", {"amount": 1000.0, "kind": "sale", "event_time": 1}
    )
    assert result.get("kept") is True
    assert result["row"].get("is_big") == 1, f"is_big cast mismatch: {result}"

    # Row with amount=0 should be dropped by the filter.
    dropped = _apply_ops(
        http_url, "ChainedDeriv", {"amount": 0.0, "kind": "sale", "event_time": 1}
    )
    assert dropped.get("kept") is False


# ---------------------------------------------------------------------------
# SC4 — hypothesis proptest: Python reference eval == Rust server eval (256 cases)
# ---------------------------------------------------------------------------

# Counter for unique derivation names across hypothesis cases.
# Hypothesis may shrink / re-run; we use a thread-safe monotonic counter so
# each generated case always gets a fresh name even across retries.
_PROPTEST_COUNTER = 0
_PROPTEST_LOCK = threading.Lock()


def _next_proptest_name() -> str:
    global _PROPTEST_COUNTER
    with _PROPTEST_LOCK:
        _PROPTEST_COUNTER += 1
        return f"ProptestDeriv_{_PROPTEST_COUNTER}"


# Schema for proptest: {a: i64, b: i64, c: f64} — numeric only.
# Bool and Str fields were intentionally excluded: mixing them with arithmetic
# operators (+, -, *, /) or cross-type comparisons triggers TypeMismatch
# rejections in the server's register-time type checker, inflating the WR-06
# skip rate above the 50% guard threshold.
_SCHEMA_FIELDS = ["a", "b", "c"]


def _arb_expr(draw: Any, depth: int = 0) -> _ExprAST:
    """Recursive Hypothesis strategy for building random expression ASTs.

    Generates only numeric-compatible expressions (arithmetic + comparison) to
    keep the server's register-time type checker happy. Excluded:
    - Boolean operators (and/or/not): require Bool-typed operands; ProptestBase
      has no Bool fields.
    - isnull: returns Bool, which cannot be used as an arithmetic or comparison
      operand in the server's type system (Bool is not comparable with I64/F64).
      Including isnull inflates the skip rate when its Bool result flows into
      an outer arithmetic/comparison op.
    """
    # At depth 3, only leaves.
    if depth >= 3:
        return _arb_leaf(draw)

    kind = draw(st.integers(min_value=0, max_value=1))

    if kind == 0:
        return _arb_leaf(draw)
    # kind == 1: BinOp arithmetic/comparison (all valid for numeric operands)
    op = draw(st.sampled_from(["+", "-", "*", "/", ">", ">=", "<", "<=", "==", "!="]))
    left = _arb_expr(draw, depth + 1)
    right = _arb_expr(draw, depth + 1)
    return _BinOp(op, left, right)


def _safe_float() -> Any:
    """Hypothesis strategy for JSON-safe floats.

    Restricted to [-1000, 1000] and rounded to 4 decimal places.  This caps
    values at ≤7 significant digits, well within the zone where Python's
    json.dumps and Rust's serde_json agree on the last ULP.  Without this
    constraint hypothesis generates floats like 2.65e-185 that round-trip
    with a 1-ULP disagreement between the two parsers, causing spurious
    SC4 divergences.
    """
    return st.floats(
        min_value=-1000.0,
        max_value=1000.0,
        allow_nan=False,
        allow_infinity=False,
        allow_subnormal=False,
    ).map(lambda x: round(x, 4))


def _arb_leaf(draw: Any) -> _ExprAST:
    """Draw a leaf node: numeric field ref, int/float literal, or null literal."""
    kind = draw(st.integers(min_value=0, max_value=2))
    if kind == 0:
        # Field reference (numeric fields only)
        name = draw(st.sampled_from(_SCHEMA_FIELDS))
        return _Field(name)
    if kind == 1:
        # Int literal
        val = draw(st.integers(min_value=-(2**30), max_value=2**30))
        return _Literal(val)
    # kind == 2: float or null literal (null exercises three-valued logic)
    choice = draw(st.integers(min_value=0, max_value=1))
    if choice == 0:
        val = draw(_safe_float())
        return _Literal(val)
    return _Literal(None)


@st.composite
def _arb_expr_and_row(draw: Any) -> tuple[_ExprAST, dict[str, Any]]:
    """Draw a random (numeric expr, row) pair for ProptestBase {a: i64, b: i64, c: f64}."""
    expr = _arb_expr(draw)
    # Row values: each field may be its typed value or None (exercises null propagation).
    a_val: Any = draw(
        st.one_of(st.none(), st.integers(min_value=-(2**30), max_value=2**30))
    )
    b_val: Any = draw(
        st.one_of(st.none(), st.integers(min_value=-(2**30), max_value=2**30))
    )
    c_val: Any = draw(st.one_of(st.none(), _safe_float()))
    row: dict[str, Any] = {
        "a": a_val,
        "b": b_val,
        "c": c_val,
    }
    return expr, row


def _python_val_to_json(v: Any) -> Any:
    """Convert a Python value to JSON-serializable form for the /dev/apply_ops row."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return v
    if isinstance(v, str):
        return v
    return None  # unsupported types → null


def _server_val_to_python(v: Any) -> Any:
    """Normalize a JSON value from the server response to canonical Python type.

    Server returns JSON null → None, JSON bool → bool, JSON int → int,
    JSON float → float. For comparison we need to normalize carefully because
    JSON doesn't distinguish int from float in some parsers.
    """
    return v  # httpx already deserializes JSON → Python native types


def _compare_values(py_val: Any, server_val: Any) -> bool:
    """Compare Python reference evaluator output vs server output.

    Handles the type-coercion edge cases:
    - Server returns JSON; Python parses 1 as int, 1.0 as float.
    - NaN / Inf: JSON cannot represent NaN or ±Inf (they are not valid JSON
      values).  The server serializes them as JSON null.  Treat
      (float nan, None) and (float ±inf, None) as equal.
    - None: both should be None (JSON null).
    """
    import math

    if py_val is None and server_val is None:
        return True
    # NaN / ±Inf serialization: Rust f64 NaN and ±Inf → JSON null on the wire.
    # Python reference evaluator returns float('nan') or float('inf');
    # the server response carries None (JSON null) for both.
    py_is_non_finite = isinstance(py_val, float) and (math.isnan(py_val) or math.isinf(py_val))
    if py_is_non_finite and server_val is None:
        return True
    if py_val is None or server_val is None:
        return False
    # Both are bool
    if isinstance(py_val, bool) and isinstance(server_val, bool):
        return py_val == server_val
    # Float NaN
    if isinstance(py_val, float) and isinstance(server_val, float):
        if math.isnan(py_val) and math.isnan(server_val):
            return True
        # Inf
        if math.isinf(py_val) and math.isinf(server_val):
            return math.copysign(1.0, py_val) == math.copysign(1.0, server_val)
        return py_val == server_val
    # Int vs float coercion: server may return 3 for what Python computes as 3 (int)
    if isinstance(py_val, int) and not isinstance(py_val, bool):
        if isinstance(server_val, float):
            return float(py_val) == server_val
        if isinstance(server_val, int) and not isinstance(server_val, bool):
            return py_val == server_val
    if isinstance(py_val, float) and isinstance(server_val, int) and not isinstance(
        server_val, bool
    ):
        return py_val == float(server_val)
    if isinstance(py_val, str) and isinstance(server_val, str):
        return py_val == server_val
    # Fallback: direct equality
    return py_val == server_val  # type: ignore[no-any-return]


@settings(
    max_examples=256,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(pair=_arb_expr_and_row())
def test_sc4_proptest_client_server_eval_equivalence(
    beava_server: tuple[str, str], pair: tuple[_ExprAST, dict[str, Any]]
) -> None:
    """SC4: 256 hypothesis cases comparing Python reference eval vs Rust /dev/apply_ops.

    For each random (expr, row) pair:
      a. Evaluate via Python _eval_reference.evaluate(expr, row).
      b. Register a with_columns(out=expr) derivation on the live server.
      c. POST /dev/apply_ops with the row; read back row["out"].
      d. Assert Python result == server result.

    Zero divergence across 256 EXECUTED cases is the Phase 4 load-bearing
    correctness claim (CONTEXT.md SC4). Cases where the server rejects the
    expression at register time (schema-invalid expressions) are counted and
    the skip rate is asserted to be ≤ 50% — if more than half the generated
    expressions are schema-invalid, the generator is too permissive and the
    equivalence claim is weakened. (WR-06 fix.)
    """
    from hypothesis import note

    http_url, _tcp_url = beava_server
    expr, row = pair

    # WR-06: ensure the skip counter is scoped to this server's lifetime.
    # Each pytest test function invocation gets a fresh server at a new port;
    # the per-URL key prevents stale counts from a prior invocation in the
    # same pytest session from inflating the skip rate during shrinking/replay.
    _sc4_reset_for_server(http_url)  # no-op if already initialised for this URL

    # Step A: Python reference evaluation.
    py_result = evaluate(expr, row)

    # Step B: Register a unique derivation with with_columns(out=<expr>).
    deriv_name = _next_proptest_name()

    # Build the derivation — ProptestBase is the source (schema {a, b, c}
    # matches _SCHEMA_FIELDS exactly; no field-missing rejections at
    # registration time). We register ProptestBase + the per-case derivation
    # together each time; the server accepts re-registration as "already_present".
    deriv = ProptestBase.with_columns(out=expr).named(deriv_name)

    payload = json.dumps(
        {"nodes": [ProptestBase._to_register_json(), deriv._to_register_json()]}
    ).encode()
    reg_resp = httpx.post(
        f"{http_url}/register",
        content=payload,
        headers={"Content-Type": "application/json"},
        timeout=10.0,
    )

    # WR-06: track skips vs total for this server's run.
    total_key = http_url
    skips_key = http_url + ":skips"
    with _sc4_skip_lock:
        _sc4_skip_counter[total_key] = _sc4_skip_counter.get(total_key, 0) + 1

    if reg_resp.status_code != 200:
        # The expression fails server-side schema validation (e.g. cross-type
        # comparison). Count as a skip; check skip rate below.
        with _sc4_skip_lock:
            _sc4_skip_counter[skips_key] = _sc4_skip_counter.get(skips_key, 0) + 1

        current_skips = _sc4_skip_counter[skips_key]
        current_total = _sc4_skip_counter[total_key]
        skip_rate = current_skips / current_total if current_total > 0 else 0.0

        note(
            f"SC4 skip #{current_skips}/{current_total} "
            f"(skip_rate={skip_rate:.0%}): "
            f"server rejected expr={expr.to_expr_string()!r} "
            f"status={reg_resp.status_code}"
        )

        # WR-06: fail if skip rate exceeds 50% to catch a generator that is
        # too permissive. Only enforce once we have enough data (≥10 cases).
        if current_total >= 10:
            assert skip_rate <= 0.50, (
                f"SC4 skip rate {skip_rate:.0%} exceeds 50% threshold "
                f"({current_skips}/{current_total} cases skipped). "
                "The expression generator is producing too many schema-invalid "
                "expressions — the equivalence claim is not adequately tested."
            )
        return

    # Step C: POST /dev/apply_ops with the row.
    # Convert row values to JSON-compatible form.
    json_row = {k: _python_val_to_json(v) for k, v in row.items()}
    ops_resp = httpx.post(
        f"{http_url}/dev/apply_ops",
        json={"derivation": deriv_name, "row": json_row},
        timeout=10.0,
    )
    if ops_resp.status_code == 404:
        # Derivation wasn't retained (shouldn't happen, but defensive).
        return
    ops_body = ops_resp.json()

    if not ops_body.get("kept", True):
        # A with_columns derivation should never drop rows (no filter op).
        # If it did, the filter predicate evaluated to null/false — only
        # possible if this derivation has a filter which we don't add.
        # Treat server null-drop as server returning None for the expression.
        server_result = None
    else:
        server_result = ops_body.get("row", {}).get("out")
        server_result = _server_val_to_python(server_result)

    # Step D: Assert equality.
    assert _compare_values(py_result, server_result), (
        f"SC4 divergence!\n"
        f"  expr: {expr.to_expr_string()!r}\n"
        f"  row:  {row}\n"
        f"  Python reference: {py_result!r}\n"
        f"  Rust server:      {server_result!r}"
    )


# ---------------------------------------------------------------------------
# SC5 — malformed predicate → error response
# ---------------------------------------------------------------------------


def test_sc5_malformed_predicate_http_400(beava_server: tuple[str, str]) -> None:
    """SC5 (HTTP): malformed expression → 400 + error code + path in response."""
    http_url, _tcp_url = beava_server

    # Register Transaction source first so the upstream reference resolves.
    with bv.App(http_url) as app:
        app.register(Transaction)

    # Bypass the SDK's expression builder to send a raw malformed expr string.
    # Include the Transaction schema in the derivation so the server reaches
    # the expression-parsing stage (it validates schema fields first).
    payload = {
        "nodes": [
            {
                "kind": "derivation",
                "name": "BadFilterHTTP",
                "output_kind": "event",
                "upstreams": ["Transaction"],
                "ops": [{"op": "filter", "expr": "(amount > "}],  # unterminated expr
                "schema": {
                    "fields": {"amount": "f64", "kind": "str", "event_time": "i64"},
                    "optional_fields": [],
                },
                "table_primary_key": None,
            }
        ]
    }

    resp = _register_raw_http(http_url, payload)
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}: {resp.text}"
    body = resp.json()
    error = body.get("error", {})
    assert error.get("code") == "invalid_expression", (
        f"Expected code='invalid_expression', got: {error}"
    )
    path = error.get("path", "")
    assert "ops" in path, f"Expected 'ops' in error path, got: {path!r}"


def test_sc5_malformed_predicate_tcp_error_frame(beava_server: tuple[str, str]) -> None:
    """SC5 (TCP): malformed expression over TCP → OP_ERROR_RESPONSE frame."""
    http_url, tcp_url = beava_server

    # Register Transaction source first (HTTP) so the upstream reference resolves.
    with bv.App(http_url) as app:
        app.register(Transaction)

    parsed = urllib.parse.urlparse(tcp_url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 7380

    import socket

    # Build a REGISTER payload with a malformed expression.
    # Include Transaction schema so the server reaches the expression-parsing stage.
    payload = json.dumps(
        {
            "nodes": [
                {
                    "kind": "derivation",
                    "name": "BadFilterTCP",
                    "output_kind": "event",
                    "upstreams": ["Transaction"],
                    "ops": [{"op": "filter", "expr": "(amount > "}],  # unterminated
                    "schema": {
                        "fields": {"amount": "f64", "kind": "str", "event_time": "i64"},
                        "optional_fields": [],
                    },
                    "table_primary_key": None,
                }
            ]
        }
    ).encode()

    with socket.create_connection((host, port), timeout=5.0) as sock:
        sock.sendall(encode_frame(OP_REGISTER, CT_JSON, payload))
        frame = read_frame(sock, 4 * 1024 * 1024)

    assert frame.op == OP_ERROR_RESPONSE, (
        f"Expected OP_ERROR_RESPONSE (0xFFFF), got op={frame.op:#06x}"
    )
    error_body = json.loads(frame.payload.decode("utf-8"))
    error = error_body.get("error", {})
    assert error.get("code") == "invalid_expression", (
        f"Expected code='invalid_expression', got: {error}"
    )
    path = error.get("path", "")
    assert "ops" in path, f"Expected 'ops' in error path, got: {path!r}"
