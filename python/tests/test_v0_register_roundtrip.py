"""Phase 22-01 round-trip contract test.

Asserts that for every AggOp descriptor, the JSON emitted by
``tally._serialize.compile_to_register_json`` contains exactly the fields
that the Rust-side ``V0RegisterPayload`` parser and ``build_operator``
dispatch consume.

**Scope note:** This plan (22-01) lands the parser/dispatch scaffold but
does *not* wire the TCP REGISTER opcode to ``register_v0`` — that's a 22-02
task (alongside operator bodies). A full TCP round-trip (App.register →
PUSH → GET) therefore lives in 22-02's test suite; here we verify the
serialization contract so the Rust parser tests in
``tests/test_register_json_v0.rs`` stay aligned with what the SDK produces.

Every field checked here is consumed by ``src/engine/register.rs``:

  * ``type``         → dispatched via ``build_operator``
  * ``field``        → required for all non-count ops
  * ``window``       → parsed via ``parse_window``
  * ``bucket``       → optional; defaults via ``default_bucket``
  * ``quantile``     → percentile
  * ``k``            → top_k
  * ``n``            → first_n / last_n / lag
  * ``half_life``    → ema
  * hybrid params    → flattened top-level for sketch ops
"""

from __future__ import annotations

import json

import pytest

import tally as tl
from tally._agg_ops import ALL_AGG_OPS
from tally._serialize import compile_to_register_json, collect_registrations


# ---------------------------------------------------------------------------
# Every AggOp round-trips through the serializer with a Rust-consumable shape
# ---------------------------------------------------------------------------


def _build_op(cls):
    """Construct an AggOp instance with reasonable defaults for the class."""
    name = cls.__name__
    # count: no field
    if name == "_Count":
        return cls(window="1h"), "n"
    if name in ("_Sum", "_Avg", "_Min", "_Max", "_Variance", "_Stddev"):
        return cls("amount", window="1h"), "agg"
    if name == "_Percentile":
        return cls("latency", 0.95, window="1h"), "p95"
    if name == "_CountDistinct":
        return cls("session_id", window="1h"), "uniq"
    if name == "_TopK":
        return cls("merchant_id", 5, window="1h"), "top"
    if name in ("_First", "_Last"):
        return cls("country"), "fc"
    if name in ("_FirstN", "_LastN"):
        return cls("country", 5), "fn5"
    if name == "_EMA":
        return cls("amount", "30m"), "smooth"
    if name == "_Lag":
        return cls("amount", 3), "prev3"
    raise AssertionError(f"missing builder for {name}")


@pytest.mark.parametrize(
    "cls",
    ALL_AGG_OPS,
    ids=lambda c: c.__name__,
)
def test_every_aggop_serializes_to_rust_parseable_shape(cls):
    op, name = _build_op(cls)
    d = op.to_json(name)

    # Core contract: name + type always present.
    assert d["name"] == name
    assert d["type"] == op.op_type
    assert isinstance(d["supports_retraction"], bool)

    # Field required for all non-count ops.
    if op.op_type != "count":
        assert d["field"], f"{op.op_type} must emit non-empty 'field'"

    # Window required for windowed ops.
    if cls.requires_window:
        assert "window" in d

    # Operator-specific required keys.
    if op.op_type == "percentile":
        assert isinstance(d["quantile"], float)
        assert d["exact_threshold"] >= 1
        assert 0.0 < d["hybrid_alpha"] < 1.0
    if op.op_type == "count_distinct":
        assert d["exact_threshold"] >= 1
        assert 4 <= d["hybrid_precision"] <= 16
    if op.op_type == "top_k":
        assert d["k"] >= 1
        assert d["exact_threshold"] >= 1
        assert d["hybrid_width"] >= 1
        assert d["hybrid_depth"] >= 1
    if op.op_type in ("first_n", "last_n", "lag"):
        assert d["n"] >= 1
    if op.op_type == "ema":
        assert d["half_life"]


# ---------------------------------------------------------------------------
# A canonical pipeline compiles to a payload that passes every Rust assertion
# ---------------------------------------------------------------------------


def test_canonical_pipeline_emits_valid_aggregation_payload():
    """CLAUDE.md canonical example: Clicks → UserSpend.

    This is the payload that the Rust ``V0RegisterPayload::parse`` test suite
    expects; if the shape drifts, the cross-language contract breaks.
    """

    @tl.stream
    class Clicks:
        user_id: str
        amount: float

    @tl.table(key="user_id")
    def UserSpend(clicks: Clicks) -> tl.Table:
        return clicks.group_by("user_id").agg(
            n=tl.count(window="1h"),
            total=tl.sum("amount", window="1h"),
        )

    frames = collect_registrations(UserSpend)
    names = [f["name"] for f in frames]
    assert names == ["Clicks", "UserSpend"]

    spend = frames[-1]
    assert spend["kind"] == "table"
    assert spend["key_field"] == "user_id"
    assert "aggregation" in spend
    agg = spend["aggregation"]
    assert agg["source"] == "Clicks"
    assert agg["keys"] == ["user_id"]

    # Two features, both already covered by existing v2.0 operator impls.
    feats = agg["features"]
    assert len(feats) == 2
    by_type = {f["type"]: f for f in feats}
    assert set(by_type) == {"count", "sum"}
    assert by_type["count"]["window"] == "1h"
    assert by_type["sum"]["field"] == "amount"
    assert by_type["sum"]["window"] == "1h"

    # JSON-serializable (matches the TCP wire format).
    blob = json.dumps(spend)
    assert b"Clicks" in blob.encode()


# ---------------------------------------------------------------------------
# A 16-op pipeline produces a payload the Rust parser will accept
# ---------------------------------------------------------------------------


def test_pipeline_with_all_sixteen_aggops_serializes():
    @tl.stream
    class Events:
        user_id: str
        amount: float
        session_id: str
        merchant_id: str
        country: str
        latency: float

    @tl.table(key="user_id")
    def UserMetrics(events: Events) -> tl.Table:
        return events.group_by("user_id").agg(
            a=tl.count(window="1h"),
            b=tl.sum("amount", window="1h"),
            c=tl.avg("amount", window="1h"),
            d=tl.min("amount", window="1h"),
            e=tl.max("amount", window="1h"),
            f=tl.variance("amount", window="1h"),
            g=tl.stddev("amount", window="1h"),
            h=tl.percentile("latency", 0.95, window="1h"),
            i=tl.count_distinct("session_id", window="1h"),
            j=tl.top_k("merchant_id", 5, window="1h"),
            k=tl.first("country"),
            last_c=tl.last("country"),
            m=tl.first_n("country", 3),
            n=tl.last_n("country", 3),
            o=tl.ema("amount", "30m"),
            p=tl.lag("amount", 2),
        )

    frames = collect_registrations(UserMetrics)
    metrics = frames[-1]
    feats = metrics["aggregation"]["features"]
    assert len(feats) == 16
    expected = {
        "count", "sum", "avg", "min", "max", "variance", "stddev",
        "percentile", "count_distinct", "top_k",
        "first", "last", "first_n", "last_n", "ema", "lag",
    }
    assert {f["type"] for f in feats} == expected


# ---------------------------------------------------------------------------
# Full TCP round-trip placeholder — wired in 22-02
# ---------------------------------------------------------------------------


def test_full_tcp_roundtrip_register_push_get(app):
    """Plan 22-04: end-to-end TCP round-trip for a v0 aggregation pipeline.

    REGISTER dispatches to the v0→v2 translator in
    ``src/engine/register.rs::v0_aggregation_to_stream_def`` so the existing
    PipelineEngine cascade drives the new ``group_by(...).agg(...)``
    aggregation. PUSH on the source stream cascades into the target table
    keyed by ``user_id``; GET returns the computed features.
    """
    @tl.stream
    class Transactions:
        user_id: str
        amount: float

    @tl.table(key="user_id")
    def UserSpend(txs: Transactions) -> tl.Table:
        return txs.group_by("user_id").agg(
            n=tl.count(window="1h"),
            total=tl.sum("amount", window="1h"),
        )

    app.register(Transactions, UserSpend)
    app.push_sync(Transactions, {"user_id": "u1", "amount": 50.0})
    app.flush()
    row = app.get("u1")
    # FeatureResult stores the raw mapping; numeric count/sum come back as
    # int/float respectively.
    assert row["n"] == 1, f"expected n=1, got {row!r}"
    assert row["total"] == 50.0, f"expected total=50.0, got {row!r}"
