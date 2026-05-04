"""Phase 13.5 Plan 03 red→green: bv.lit factory per ADR-003 Decision A."""
from __future__ import annotations

import beava as bv
from beava._col import _Literal


def test_lit_returns_Literal_node() -> None:
    e = bv.lit(42)
    assert isinstance(e, _Literal)
    assert e.value == 42


def test_lit_string_value() -> None:
    e = bv.lit("web")
    assert e.value == "web"


def test_lit_none_value() -> None:
    e = bv.lit(None)
    assert e.value is None


def test_lit_bool_value() -> None:
    assert bv.lit(True).value is True
    assert bv.lit(False).value is False


def test_lit_explicit_in_filter() -> None:
    """ADR-003 use case: bv.col('amount') > bv.lit(100) — explicit literal in filter."""
    expr = bv.col("amount") > bv.lit(100)
    s = expr.to_expr_string()
    assert "amount" in s
    assert "100" in s
    assert ">" in s


def test_lit_distinct_calls_produce_distinct_nodes() -> None:
    """ADR-003 contract: each bv.lit call produces a fresh AST node (immutability)."""
    a = bv.lit(42)
    b = bv.lit(42)
    assert id(a) != id(b)
    # But same canonical wire form.
    assert a.to_expr_string() == b.to_expr_string() == "42"
