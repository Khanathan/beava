"""SDK-OPS-09 unit tests: op methods on EventSource/EventDerivation + TableSource/TableDerivation.

Every test in this file is a unit test — no server required.

Tests verify:
  - Every op returns a NEW derivation (object identity differs; SDK-OPS-09).
  - No mutation of `self` (strict immutability).
  - Each op appends the correct dict to the ops list.
  - Chaining composes left-to-right.
  - Table-specific: drop rejects key fields; rename cascades key list.
  - Cast client-side validates target type.

These tests are RED until Task 1.b implements the op methods.
"""

from __future__ import annotations

import pytest

import beava as bv
from beava._events import EventDerivation
from beava._tables import TableDerivation

# ---------------------------------------------------------------------------
# Shared descriptors (defined at module scope so they are not re-created per test)
# ---------------------------------------------------------------------------


@bv.event
class Transaction:
    amount: float
    kind: str
    ts: int


@bv.table(key="user_id")
class UserProfile:
    user_id: str
    score: float


# ---------------------------------------------------------------------------
# SDK-OPS-01: filter
# ---------------------------------------------------------------------------


def test_filter_returns_new_derivation() -> None:
    d1 = Transaction.filter(bv.col("amount") > 100)
    assert d1 is not Transaction
    assert isinstance(d1, EventDerivation)
    assert d1.upstream is Transaction
    assert len(d1.ops) == 1
    assert d1.ops[0] == {"op": "filter", "expr": "(amount > 100)"}


# ---------------------------------------------------------------------------
# SDK-OPS-02: select
# ---------------------------------------------------------------------------


def test_select_returns_new_derivation() -> None:
    d1 = Transaction.select("amount", "ts")
    assert d1 is not Transaction
    assert isinstance(d1, EventDerivation)
    assert d1.ops == [{"op": "select", "fields": ["amount", "ts"]}]


# ---------------------------------------------------------------------------
# SDK-OPS-03: drop
# ---------------------------------------------------------------------------


def test_drop_returns_new_derivation() -> None:
    d1 = Transaction.drop("amount")
    assert d1 is not Transaction
    assert isinstance(d1, EventDerivation)
    assert d1.ops == [{"op": "drop", "fields": ["amount"]}]


# ---------------------------------------------------------------------------
# SDK-OPS-04: rename
# ---------------------------------------------------------------------------


def test_rename_returns_new_derivation() -> None:
    d1 = Transaction.rename(amount="amt")
    assert d1 is not Transaction
    assert isinstance(d1, EventDerivation)
    assert d1.ops == [{"op": "rename", "mapping": {"amount": "amt"}}]


# ---------------------------------------------------------------------------
# SDK-OPS-05: with_columns
# ---------------------------------------------------------------------------


def test_with_columns_returns_new_derivation() -> None:
    d1 = Transaction.with_columns(is_big=bv.col("amount") > 500)
    assert d1 is not Transaction
    assert isinstance(d1, EventDerivation)
    assert d1.ops == [{"op": "with_columns", "exprs": {"is_big": "(amount > 500)"}}]


# ---------------------------------------------------------------------------
# SDK-OPS-06: map (alias; distinct op name on wire)
# ---------------------------------------------------------------------------


def test_map_is_alias_of_with_columns() -> None:
    d1 = Transaction.map(is_big=bv.col("amount") > 500)
    assert d1 is not Transaction
    assert isinstance(d1, EventDerivation)
    assert d1.ops[0]["op"] == "map"
    assert d1.ops[0]["exprs"] == {"is_big": "(amount > 500)"}


# ---------------------------------------------------------------------------
# SDK-OPS-07: cast
# ---------------------------------------------------------------------------


def test_cast_returns_new_derivation() -> None:
    d1 = Transaction.cast(amount="int")
    assert d1 is not Transaction
    assert isinstance(d1, EventDerivation)
    assert d1.ops == [{"op": "cast", "type_map": {"amount": "int"}}]


# ---------------------------------------------------------------------------
# SDK-OPS-08: fillna
# ---------------------------------------------------------------------------


def test_fillna_returns_new_derivation() -> None:
    d1 = Transaction.fillna(amount=0)
    assert d1 is not Transaction
    assert isinstance(d1, EventDerivation)
    assert d1.ops == [{"op": "fillna", "defaults": {"amount": 0}}]


# ---------------------------------------------------------------------------
# SDK-OPS-09/10: chaining + immutability
# ---------------------------------------------------------------------------


def test_chained_ops_append_to_ops_list() -> None:
    # Capture intermediate derivation BEFORE chaining further.
    filter_only = Transaction.filter(bv.col("amount") > 0)
    assert len(filter_only.ops) == 1  # sanity pre-condition

    d3 = filter_only.select("amount", "ts").with_columns(
        is_big=bv.col("amount") > 500
    )

    assert len(d3.ops) == 3
    assert d3.ops[0]["op"] == "filter"
    assert d3.ops[1]["op"] == "select"
    assert d3.ops[2]["op"] == "with_columns"

    # SDK-OPS-09: filter_only must NOT be mutated by further chaining.
    assert len(filter_only.ops) == 1, (
        "filter_only.ops was mutated by downstream chaining — SDK-OPS-09 violated"
    )


def test_source_reference_preserved_through_chain() -> None:
    d1 = Transaction.filter(bv.col("amount") > 0)
    d2 = d1.select("amount", "ts")
    d3 = d2.with_columns(is_big=bv.col("amount") > 500)
    # Can trace back to the original source through the upstream chain.
    assert d3.upstream is d2
    assert d2.upstream is d1
    assert d1.upstream is Transaction


# ---------------------------------------------------------------------------
# Table-specific: SDK-OPS-03 (drop rejects key) + SDK-OPS-04 (rename cascades)
# ---------------------------------------------------------------------------


def test_table_drop_rejects_key_fields() -> None:
    with pytest.raises(ValueError, match="cannot drop key"):
        UserProfile.drop("user_id")
    # Dropping a non-key field works fine.
    d1 = UserProfile.drop("score")
    assert isinstance(d1, TableDerivation)
    assert d1.ops == [{"op": "drop", "fields": ["score"]}]


def test_table_rename_cascades_into_key_list() -> None:
    d1 = UserProfile.rename(user_id="uid")
    assert isinstance(d1, TableDerivation)
    # The key list in the resulting derivation must reflect the renamed field.
    assert d1.key == ["uid"]
    # ops list carries the rename op.
    assert d1.ops == [{"op": "rename", "mapping": {"user_id": "uid"}}]


# ---------------------------------------------------------------------------
# Return-type alignment
# ---------------------------------------------------------------------------


def test_event_op_methods_available_on_event_derivation() -> None:
    d1 = Transaction.filter(bv.col("amount") > 0)
    assert isinstance(d1, EventDerivation)
    # d1 itself must expose the op methods (fluent chaining).
    d2 = d1.filter(bv.col("amount") < 1000)
    assert isinstance(d2, EventDerivation)
    assert len(d2.ops) == 2


def test_op_method_returns_type_matches_source() -> None:
    # EventSource -> EventDerivation
    ev_d = Transaction.filter(bv.col("amount") > 0)
    assert isinstance(ev_d, EventDerivation)
    # TableSource -> TableDerivation
    tbl_d = UserProfile.filter(bv.col("score") > 0)
    assert isinstance(tbl_d, TableDerivation)


# ---------------------------------------------------------------------------
# Expression serialization
# ---------------------------------------------------------------------------


def test_filter_expression_serializes_to_canonical_form() -> None:
    d1 = Transaction.filter(
        (bv.col("amount") > 100) & (bv.col("kind") == "fraud")
    )
    assert d1.ops[0]["expr"] == "((amount > 100) and (kind == 'fraud'))"


def test_with_columns_expressions_serialize_to_canonical_form() -> None:
    d1 = Transaction.with_columns(
        is_big=bv.col("amount") > 500,
        is_small=bv.col("amount") < 10,
    )
    assert d1.ops[0]["exprs"] == {
        "is_big": "(amount > 500)",
        "is_small": "(amount < 10)",
    }


def test_fillna_default_serialization() -> None:
    d1 = Transaction.fillna(amount=0, kind="unknown")
    assert d1.ops[0]["defaults"] == {"amount": 0, "kind": "unknown"}


# ---------------------------------------------------------------------------
# SDK-OPS-07: cast client-side target validation
# ---------------------------------------------------------------------------


def test_cast_type_map_rejects_invalid_target_client_side() -> None:
    with pytest.raises(ValueError, match="blob"):
        Transaction.cast(amount="blob")
    # Valid targets do NOT raise.
    for valid in ("str", "int", "float", "bool"):
        Transaction.cast(amount=valid)  # must not raise
