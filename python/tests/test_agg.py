"""Tests for _agg.py: AggDescriptor, GroupBy, and 8 module-level helpers.

Requirements: SDK-AGG-01, SDK-AGG-02, SDK-AGG-03, SDK-AGG-04, SDK-AGG-05, SDK-AGG-06
"""

from __future__ import annotations

import pytest

import beava as bv
from beava._agg import AggDescriptor, GroupBy
from beava._tables import TableDerivation


# ---------------------------------------------------------------------------
# Fixtures — a simple event source for group_by tests
# ---------------------------------------------------------------------------


@bv.event
class Tx:
    user_id: str
    amount: float
    status: str


@bv.table(key="user_id")
class Users:
    user_id: str
    name: str


# ===========================================================================
# Re-export tests
# ===========================================================================


def test_module_exports_all_helpers() -> None:
    """All 8 aggregation helpers are importable from beava."""
    from beava import avg, count, max, min, ratio, stddev, sum, variance  # noqa: F401


def test_module_exports_groupby() -> None:
    """GroupBy and AggDescriptor are importable from beava."""
    from beava import AggDescriptor, GroupBy  # noqa: F401

    assert AggDescriptor is not None
    assert GroupBy is not None


# ===========================================================================
# bv.count — SDK-AGG-06 window validation
# ===========================================================================


def test_count_minimal_returns_descriptor() -> None:
    """bv.count() with no args returns AggDescriptor(op='count', ...)."""
    d = bv.count()
    assert isinstance(d, AggDescriptor)
    assert d.op == "count"
    assert d.field is None
    assert d.window is None
    assert d.where is None


def test_count_with_window_5m() -> None:
    """bv.count(window='5m') returns descriptor with window='5m'."""
    d = bv.count(window="5m")
    assert isinstance(d, AggDescriptor)
    assert d.op == "count"
    assert d.window == "5m"


def test_count_rejects_malformed_window() -> None:
    """bv.count(window='5seconds') raises ValueError matching /window .*must match/."""
    with pytest.raises(ValueError, match=r"window.*must match"):
        bv.count(window="5seconds")


def test_count_rejects_bare_number_window() -> None:
    """bv.count(window='5') raises ValueError — no unit suffix."""
    with pytest.raises(ValueError, match=r"window.*must match"):
        bv.count(window="5")


def test_count_no_window_ok() -> None:
    """bv.count() without window = lifetime mode (AGG-CORE-09)."""
    d = bv.count()
    assert d.window is None


def test_count_forever_window_ok() -> None:
    """bv.count(window='forever') is valid."""
    d = bv.count(window="forever")
    assert d.window == "forever"


# ===========================================================================
# bv.sum
# ===========================================================================


def test_sum_with_field_and_window() -> None:
    """bv.sum('amount', window='1h') returns AggDescriptor(op='sum', field='amount', window='1h')."""
    d = bv.sum("amount", window="1h")
    assert isinstance(d, AggDescriptor)
    assert d.op == "sum"
    assert d.field == "amount"
    assert d.window == "1h"


def test_sum_requires_window() -> None:
    """bv.sum('amount') without window= raises ValueError /window is required for/."""
    with pytest.raises((ValueError, TypeError), match=r"window"):
        bv.sum("amount")  # type: ignore[call-arg]


def test_sum_rejects_malformed_window() -> None:
    """bv.sum('amount', window='1hour') raises ValueError."""
    with pytest.raises(ValueError, match=r"window.*must match"):
        bv.sum("amount", window="1hour")


# ===========================================================================
# bv.avg
# ===========================================================================


def test_avg_requires_window_rejected_without() -> None:
    """bv.avg('amount') without window= raises ValueError."""
    with pytest.raises((ValueError, TypeError), match=r"window"):
        bv.avg("amount")  # type: ignore[call-arg]


def test_avg_with_field_and_window() -> None:
    """bv.avg('amount', window='30m') returns AggDescriptor."""
    d = bv.avg("amount", window="30m")
    assert d.op == "avg"
    assert d.field == "amount"
    assert d.window == "30m"


# ===========================================================================
# bv.min
# ===========================================================================


def test_min_requires_window() -> None:
    """bv.min('amount') without window= raises ValueError."""
    with pytest.raises((ValueError, TypeError), match=r"window"):
        bv.min("amount")  # type: ignore[call-arg]


def test_min_with_field_and_window() -> None:
    """bv.min('amount', window='10s') returns AggDescriptor."""
    d = bv.min("amount", window="10s")
    assert d.op == "min"
    assert d.field == "amount"


# ===========================================================================
# bv.max
# ===========================================================================


def test_max_requires_window() -> None:
    """bv.max('amount') without window= raises ValueError."""
    with pytest.raises((ValueError, TypeError), match=r"window"):
        bv.max("amount")  # type: ignore[call-arg]


def test_max_with_field_and_window() -> None:
    """bv.max('amount', window='1d') returns AggDescriptor."""
    d = bv.max("amount", window="1d")
    assert d.op == "max"
    assert d.field == "amount"
    assert d.window == "1d"


# ===========================================================================
# bv.variance
# ===========================================================================


def test_variance_requires_window() -> None:
    """bv.variance('amount') without window= raises ValueError."""
    with pytest.raises((ValueError, TypeError), match=r"window"):
        bv.variance("amount")  # type: ignore[call-arg]


def test_variance_with_field_and_window() -> None:
    """bv.variance('amount', window='5m') returns AggDescriptor."""
    d = bv.variance("amount", window="5m")
    assert d.op == "variance"
    assert d.field == "amount"


# ===========================================================================
# bv.stddev
# ===========================================================================


def test_stddev_requires_window() -> None:
    """bv.stddev('amount') without window= raises ValueError."""
    with pytest.raises((ValueError, TypeError), match=r"window"):
        bv.stddev("amount")  # type: ignore[call-arg]


def test_stddev_with_field_and_window() -> None:
    """bv.stddev('amount', window='100ms') returns AggDescriptor."""
    d = bv.stddev("amount", window="100ms")
    assert d.op == "stddev"
    assert d.field == "amount"
    assert d.window == "100ms"


# ===========================================================================
# bv.ratio — windowless ok (AGG-CORE-09)
# ===========================================================================


def test_ratio_no_field_no_window_ok() -> None:
    """bv.ratio() with no args returns AggDescriptor(op='ratio', window=None)."""
    d = bv.ratio()
    assert isinstance(d, AggDescriptor)
    assert d.op == "ratio"
    assert d.field is None
    assert d.window is None


def test_ratio_with_window() -> None:
    """bv.ratio(window='1h') is valid."""
    d = bv.ratio(window="1h")
    assert d.window == "1h"


def test_ratio_rejects_malformed_window() -> None:
    """bv.ratio(window='1hour') raises ValueError."""
    with pytest.raises(ValueError, match=r"window.*must match"):
        bv.ratio(window="1hour")


# ===========================================================================
# where= serialization via _ExprAST.to_expr_string() (SDK-AGG-04)
# ===========================================================================


def test_where_expr_serialises_via_to_expr_string() -> None:
    """bv.count(where=bv.col('amount') > 100) serialises the predicate."""
    d = bv.count(where=bv.col("amount") > 100)
    assert isinstance(d, AggDescriptor)
    assert d.where is not None
    assert "amount" in d.where
    assert "100" in d.where


def test_where_string_comparison_serialises() -> None:
    """bv.count(where=bv.col('status') == 'ok') includes the string literal."""
    d = bv.count(where=bv.col("status") == "ok")
    assert d.where is not None
    assert "status" in d.where
    assert "ok" in d.where


def test_where_non_expr_raises_typeerror() -> None:
    """where= with a raw string (not a bv.col expr) raises TypeError."""
    with pytest.raises(TypeError):
        bv.count(where="status == 'ok'")


# ===========================================================================
# AggDescriptor.to_agg_spec() — wire JSON shape
# ===========================================================================


def test_to_agg_spec_count_with_window() -> None:
    """count(window='5m').to_agg_spec() returns {'op': 'count', 'params': {'window': '5m'}}."""
    d = bv.count(window="5m")
    spec = d.to_agg_spec()
    assert spec["op"] == "count"
    assert spec["params"]["window"] == "5m"
    assert "field" not in spec["params"]


def test_to_agg_spec_sum_with_field_and_window() -> None:
    """sum('amount', window='1h').to_agg_spec() includes field and window in params."""
    d = bv.sum("amount", window="1h")
    spec = d.to_agg_spec()
    assert spec["op"] == "sum"
    assert spec["params"]["field"] == "amount"
    assert spec["params"]["window"] == "1h"


def test_to_agg_spec_count_minimal_omits_empty_params() -> None:
    """count().to_agg_spec() params dict has no None entries."""
    spec = bv.count().to_agg_spec()
    for v in spec["params"].values():
        assert v is not None


def test_to_agg_spec_with_where() -> None:
    """Descriptor with where= includes where in params."""
    d = bv.count(window="5m", where=bv.col("status") == "ok")
    spec = d.to_agg_spec()
    assert "where" in spec["params"]
    assert spec["params"]["where"] is not None


# ===========================================================================
# GroupBy — creation
# ===========================================================================


def test_group_by_single_key_creates_groupby() -> None:
    """Tx.group_by('user_id') returns a GroupBy with keys=['user_id']."""
    gb = Tx.group_by("user_id")
    assert isinstance(gb, GroupBy)
    assert gb._keys == ["user_id"]


def test_group_by_multiple_keys() -> None:
    """Tx.group_by('user_id', 'status') sets both keys."""
    gb = Tx.group_by("user_id", "status")
    assert gb._keys == ["user_id", "status"]


def test_group_by_rejects_non_string_keys() -> None:
    """Tx.group_by(123) raises TypeError."""
    with pytest.raises(TypeError):
        Tx.group_by(123)  # type: ignore[arg-type]


def test_group_by_rejects_missing_key_in_schema() -> None:
    """Tx.group_by('nonexistent') raises ValueError /key .* not in schema/."""
    with pytest.raises(ValueError, match=r"key.*not in schema|not in schema|nonexistent"):
        Tx.group_by("nonexistent")


def test_group_by_rejects_empty_keys() -> None:
    """Tx.group_by() with no args raises ValueError."""
    with pytest.raises(ValueError):
        Tx.group_by()


# ===========================================================================
# GroupBy.agg() — returns TableDerivation + correct OpNode
# ===========================================================================


def test_group_by_agg_returns_table_derivation() -> None:
    """Tx.group_by('user_id').agg(cnt=bv.count(window='5m')) returns TableDerivation."""
    td = Tx.group_by("user_id").agg(cnt=bv.count(window="5m"))
    assert isinstance(td, TableDerivation)
    assert td._output_kind == "table"
    assert td._table_primary_key == ["user_id"]


def test_group_by_agg_adds_groupby_opnode() -> None:
    """The last op node in the TableDerivation matches the expected GroupBy wire shape."""
    td = Tx.group_by("user_id").agg(cnt=bv.count(window="5m"))
    last_op = td._ops[-1]
    assert last_op["op"] == "group_by"
    assert last_op["keys"] == ["user_id"]
    agg = last_op["agg"]
    assert "cnt" in agg
    assert agg["cnt"]["op"] == "count"
    assert agg["cnt"]["params"]["window"] == "5m"


def test_group_by_agg_with_where_serialises_predicate() -> None:
    """where= in a feature's agg spec is serialised into ops[-1]['agg'][name]['params']['where']."""
    td = Tx.group_by("user_id").agg(
        ok_cnt=bv.count(window="5m", where=bv.col("status") == "ok")
    )
    last_op = td._ops[-1]
    params = last_op["agg"]["ok_cnt"]["params"]
    assert "where" in params
    where_str = params["where"]
    assert "status" in where_str
    assert "ok" in where_str


def test_group_by_agg_with_multiple_features_preserves_order() -> None:
    """3 features → ops[-1]['agg'] has exactly 3 entries."""
    td = Tx.group_by("user_id").agg(
        cnt=bv.count(window="5m"),
        total=bv.sum("amount", window="1h"),
        avg_amt=bv.avg("amount", window="30m"),
    )
    last_op = td._ops[-1]
    assert len(last_op["agg"]) == 3
    assert list(last_op["agg"].keys()) == ["cnt", "total", "avg_amt"]


def test_group_by_agg_rejects_non_agg_descriptor() -> None:
    """GroupBy.agg(x=42) raises TypeError for non-AggDescriptor values."""
    with pytest.raises(TypeError):
        Tx.group_by("user_id").agg(x=42)  # type: ignore[arg-type]


# ===========================================================================
# Table rejection (SDK-AGG-05)
# ===========================================================================


def test_table_group_by_raises() -> None:
    """Users.group_by('user_id') raises TypeError citing SDK-AGG-05 or v0."""
    with pytest.raises(TypeError, match=r"SDK-AGG-05|not supported in v0"):
        Users.group_by("user_id")


# ===========================================================================
# All window unit suffixes accepted
# ===========================================================================


@pytest.mark.parametrize("window", ["1ms", "1s", "1m", "1h", "1d", "forever", "100ms", "24h"])
def test_count_accepts_all_valid_window_units(window: str) -> None:
    """bv.count(window=X) accepts all valid unit suffixes per SDK-AGG-06."""
    d = bv.count(window=window)
    assert d.window == window


@pytest.mark.parametrize("window", ["1hour", "1min", "5sec", "2weeks", "abc", "5", ""])
def test_count_rejects_invalid_window_strings(window: str) -> None:
    """bv.count(window=X) rejects all malformed window strings per SDK-AGG-06."""
    with pytest.raises(ValueError, match=r"window.*must match"):
        bv.count(window=window)
