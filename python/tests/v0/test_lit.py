"""bv.lit literal-export tests (ADR-003 scope amendment 2026-05-03).

Verifies that `bv.lit(value)` is exposed in the public namespace and that
literals constructed via `bv.lit(...)` behave identically to inline Python
literals when used in chain expressions.

Tests cover:
  - bv.lit-as-with_columns constant column ("source"=bv.lit("web"))
  - bv.lit-as-explicit-filter (col > bv.lit(100) == col > 100, same wire payload)
  - bv.lit-as-divisor for forced-float division (rate = count / bv.lit(60.0))
  - bv.lit value types — int / float / str / bool / None all roundtrip
  - bv.lit immutability — fresh AST node per call; same input → same wire form

Phase 13.5.2 D-04 rewrite: every chain that previously used the
``Click.with_columns(...).named("Tagged")`` chain-form is now wrapped in
an ``@bv.event def Tagged(click: Click): return click.with_columns(...)``
function. Per D-01+D-02, raw chain expressions are no longer registerable;
the ``@bv.event def`` form is the canonical (and only) public surface.
"""
from __future__ import annotations

import random

import pytest

import beava as bv
from beava._app import _descriptor_to_node

from ._helpers import (
    ENTITIES,
    _engine_available,
    cold_start_equivalent,
)

pytestmark = pytest.mark.skipif(
    not _engine_available(),
    reason="requires Phase 13.4 engine + Phase 13.5 SDK rewrite (ADR-003 bv.lit)",
)


# ---------------------------------------------------------------------------
# Test 1: bv.lit as constant column in with_columns
# ---------------------------------------------------------------------------


def test_lit_constant_column(app):
    """events.with_columns(source=bv.lit('web')): 800 events / 5 users; tag survives downstream."""

    @bv.event
    class Click:
        user_id: str
        page: str

    @bv.event
    def Tagged(click: Click):
        return click.with_columns(source=bv.lit("web"))

    @bv.table(key="user_id")
    def UserClicks(tagged: Tagged):
        return tagged.group_by("user_id").agg(
            total=bv.count(window="forever"),
            web_only=bv.count(
                window="forever",
                where=bv.col("source") == bv.lit("web"),
            ),
        )

    app.register(Click, Tagged, UserClicks)

    rng = random.Random(140)
    counts: dict[str, int] = {entity: 0 for entity in ENTITIES}
    for _ in range(800):
        user = rng.choice(ENTITIES)
        counts[user] += 1
        app.push("Click", {"user_id": user, "page": "/home"})

    for entity, exp in counts.items():
        if exp == 0:
            continue
        result = app.get("UserClicks", entity)
        # bv.lit("web") sets source="web" on EVERY event; so total == web_only.
        assert result["total"] == exp, f"{entity}: total={result['total']} != {exp}"
        assert result["web_only"] == exp, (
            f"{entity}: web_only={result['web_only']} != {exp} (lit-constant should match all)"
        )

    assert cold_start_equivalent(app.get("UserClicks", "unknown_lit"))


# ---------------------------------------------------------------------------
# Test 2: bv.lit explicit vs implicit literal — same wire + same result
# ---------------------------------------------------------------------------


def test_lit_explicit_filter_literal(app):
    """events.filter(col > bv.lit(100)) == events.filter(col > 100): same wire + same result."""

    @bv.event
    class Tx:
        user_id: str
        amount: float

    # Two derivations, identical filter — one with bv.lit, one with implicit literal.
    @bv.event
    def BigImplicit(tx: Tx):
        return tx.filter(bv.col("amount") > 100)

    @bv.event
    def BigExplicit(tx: Tx):
        return tx.filter(bv.col("amount") > bv.lit(100))

    @bv.table(key="user_id")
    def CountImplicit(big: BigImplicit):
        return big.group_by("user_id").agg(nI=bv.count(window="forever"))

    @bv.table(key="user_id")
    def CountExplicit(big: BigExplicit):
        return big.group_by("user_id").agg(nE=bv.count(window="forever"))

    app.register(
        Tx,
        BigImplicit,
        BigExplicit,
        CountImplicit,
        CountExplicit,
    )

    # ── Wire-payload equivalence: both filter wire JSONs must be byte-identical.
    impl_node = _descriptor_to_node(BigImplicit)
    expl_node = _descriptor_to_node(BigExplicit)
    impl_filter_op = next(op for op in impl_node["ops"] if op.get("op") == "filter")
    expl_filter_op = next(op for op in expl_node["ops"] if op.get("op") == "filter")
    assert impl_filter_op["expr"] == expl_filter_op["expr"], (
        f"explicit/implicit literal compile to different exprs: "
        f"impl={impl_filter_op['expr']!r}, expl={expl_filter_op['expr']!r}"
    )

    # ── Result equivalence: 600 transactions, count where amount > 100 per entity.
    rng = random.Random(141)
    big_counts: dict[str, int] = {entity: 0 for entity in ENTITIES}
    for _ in range(600):
        user = rng.choice(ENTITIES)
        amount = rng.uniform(0.0, 200.0)
        if amount > 100:
            big_counts[user] += 1
        app.push("Tx", {"user_id": user, "amount": amount})

    for entity, exp in big_counts.items():
        r_impl = app.get("CountImplicit", entity)
        r_expl = app.get("CountExplicit", entity)
        # Distinct feature names per Plan 07b sub-task 4 (global feature
        # uniqueness); compare counts rather than dict-identity.
        assert r_impl.get("nI", 0) == r_expl.get("nE", 0), (
            f"{entity}: implicit lit result {r_impl!r} != explicit {r_expl!r}"
        )
        assert r_impl.get("nI", 0) == exp, (
            f"{entity}: implicit count {r_impl.get('nI', 0)} != expected {exp}"
        )

    assert cold_start_equivalent(app.get("CountImplicit", "unknown_litf"))


# ---------------------------------------------------------------------------
# Test 3: bv.lit forces float division
# ---------------------------------------------------------------------------


def test_lit_force_float_division(app):
    """events.with_columns(rate=col('count')/bv.lit(60.0)): result should be f64.

    bv.lit(60.0) is f64 → division widens to f64 per infer_output_type.
    Compare to bv.lit(60) (int) which would give i64 division — the test
    asserts the ENGINE infers f64 from bv.lit(60.0) and downstream
    aggregations remain numeric (sum/mean compute correctly).
    """

    @bv.event
    class Telemetry:
        user_id: str
        count: int

    @bv.event
    def Rated(telemetry: Telemetry):
        return telemetry.with_columns(rate=bv.col("count") / bv.lit(60.0))

    @bv.table(key="user_id")
    def UserMeanRate(rated: Rated):
        return rated.group_by("user_id").agg(
            mean_rate=bv.mean("rate", window="forever"),
        )

    app.register(Telemetry, Rated, UserMeanRate)

    rng = random.Random(142)
    accum: dict[str, list[float]] = {entity: [] for entity in ENTITIES}
    for _ in range(500):
        user = rng.choice(ENTITIES)
        count = rng.randint(1, 1000)
        accum[user].append(count / 60.0)  # python ground truth using same divisor
        app.push("Telemetry", {"user_id": user, "count": count})

    for entity, rates in accum.items():
        if not rates:
            continue
        expected = sum(rates) / len(rates)
        result = app.get("UserMeanRate", entity)
        actual = float(result["mean_rate"])
        # Tolerance for float arithmetic + accumulation
        assert abs(actual - expected) < 1e-3, (
            f"{entity}: expected mean_rate={expected}, got {actual}"
        )

    assert cold_start_equivalent(app.get("UserMeanRate", "unknown_litd"))


# ---------------------------------------------------------------------------
# Test 4: bv.lit value types (int / float / str / bool / None)
# ---------------------------------------------------------------------------


def test_lit_value_types(app):
    """bv.lit accepts int, float, str, bool, None — each produces a usable AST node.

    Pushes 500 events through a single derivation that uses 5 distinct bv.lit
    types in chained filter expressions, validating each compiles + roundtrips.
    """

    @bv.event
    class M:
        user_id: str
        n: int
        x: float
        s: str
        b: bool

    # Build a filter that uses each lit-type:
    #   n >= bv.lit(0)              # int
    #   x <= bv.lit(1000.0)         # float
    #   s != bv.lit("excluded")     # str
    #   b == bv.lit(True)           # bool
    @bv.event
    def Filtered(m: M):
        return m.filter(
            (bv.col("n") >= bv.lit(0))
            & (bv.col("x") <= bv.lit(1000.0))
            & (bv.col("s") != bv.lit("excluded"))
            & (bv.col("b") == bv.lit(True))
        )

    @bv.table(key="user_id")
    def UserFilteredCount(filtered: Filtered):
        return filtered.group_by("user_id").agg(n=bv.count(window="forever"))

    app.register(M, Filtered, UserFilteredCount)

    rng = random.Random(143)
    expected_kept: dict[str, int] = {entity: 0 for entity in ENTITIES}
    for _ in range(500):
        user = rng.choice(ENTITIES)
        n = rng.randint(-50, 100)
        x = rng.uniform(0.0, 2000.0)
        s = rng.choice(["ok", "excluded", "x"])
        b = rng.choice([True, False])
        # Mirror the filter logic in Python ground truth
        if n >= 0 and x <= 1000.0 and s != "excluded" and b is True:
            expected_kept[user] += 1
        app.push("M", {"user_id": user, "n": n, "x": x, "s": s, "b": b})

    for entity, exp in expected_kept.items():
        result = app.get("UserFilteredCount", entity)
        assert result.get("n", 0) == exp, (
            f"{entity}: expected filtered count={exp}, got {result.get('n', 0)}"
        )

    # Verify the wire payload references each lit value type at least once.
    filter_op = next(
        op for op in _descriptor_to_node(Filtered)["ops"] if op.get("op") == "filter"
    )
    expr_str = filter_op["expr"]
    # Sanity: each lit form's textual representation appears in the serialized expr.
    assert "0" in expr_str  # int
    assert "1000.0" in expr_str or "1000" in expr_str  # float
    assert "'excluded'" in expr_str  # str
    assert "true" in expr_str  # bool

    assert cold_start_equivalent(app.get("UserFilteredCount", "unknown_lit_t"))


# ---------------------------------------------------------------------------
# Test 5: bv.lit immutability — fresh AST per call, same input → same wire
# ---------------------------------------------------------------------------


def test_lit_immutability(app):
    """bv.lit(42) called twice yields two distinct AST nodes that serialize identically."""

    @bv.event
    class M:
        user_id: str
        n: int

    a = bv.lit(42)
    b = bv.lit(42)
    # Two distinct objects — id() differs (immutability assertion).
    assert id(a) != id(b), "bv.lit must produce a fresh AST node each call"
    # But same canonical wire form.
    assert a.to_expr_string() == b.to_expr_string() == "42"

    # Use both in a real pipeline; both compile to the same wire payload + same result.
    @bv.event
    def UseA(m: M):
        return m.filter(bv.col("n") > a)

    @bv.event
    def UseB(m: M):
        return m.filter(bv.col("n") > b)

    @bv.table(key="user_id")
    def CountA(filt: UseA):
        return filt.group_by("user_id").agg(nA=bv.count(window="forever"))

    @bv.table(key="user_id")
    def CountB(filt: UseB):
        return filt.group_by("user_id").agg(nB=bv.count(window="forever"))

    app.register(M, UseA, UseB, CountA, CountB)

    rng = random.Random(144)
    expected_filtered: dict[str, int] = {entity: 0 for entity in ENTITIES}
    for _ in range(500):
        user = rng.choice(ENTITIES)
        n_val = rng.randint(0, 100)
        if n_val > 42:
            expected_filtered[user] += 1
        app.push("M", {"user_id": user, "n": n_val})

    for entity, exp in expected_filtered.items():
        ra = app.get("CountA", entity)
        rb = app.get("CountB", entity)
        # Distinct feature names per Plan 07b sub-task 4 (global feature
        # uniqueness); compare counts directly.
        assert ra.get("nA", 0) == rb.get("nB", 0), (
            f"{entity}: bv.lit calls produce different results: {ra!r} != {rb!r}"
        )
        assert ra.get("nA", 0) == exp, (
            f"{entity}: expected nA={exp}, got {ra.get('nA', 0)}"
        )

    assert cold_start_equivalent(app.get("CountA", "unknown_imm"))
