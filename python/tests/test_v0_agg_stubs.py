"""Plan 21-03 / Task 1: aggregation operator descriptors + GroupBy.agg stub."""

from __future__ import annotations

import pytest

import tally as tl
from tally._agg_ops import ALL_AGG_OPS, AggOp
from tally._aggregation import AggregationSpec


# ---------------------------------------------------------------------------
# Registry sanity
# ---------------------------------------------------------------------------


def test_sixteen_operators_registered():
    assert len(ALL_AGG_OPS) == 16
    names = {op.__name__ for op in ALL_AGG_OPS}
    expected = {
        "_Count", "_Sum", "_Avg", "_Min", "_Max",
        "_Variance", "_Stddev",
        "_Percentile", "_CountDistinct", "_TopK",
        "_First", "_Last", "_FirstN", "_LastN",
        "_EMA", "_Lag",
    }
    assert names == expected


# ---------------------------------------------------------------------------
# Happy path construction + to_json per op
# ---------------------------------------------------------------------------


def test_count_basic():
    op = tl.count(window="30m")
    assert isinstance(op, AggOp)
    assert op.supports_retraction is True
    assert op.requires_window is True
    j = op.to_json("f")
    assert j["name"] == "f"
    assert j["type"] == "count"
    assert j["window"] == "30m"
    assert j["supports_retraction"] is True
    assert "field" not in j


def test_count_with_where_and_bucket():
    op = tl.count(window="1h", where="status == 'failed'", bucket="1m")
    j = op.to_json("fails")
    assert j["where"] == "status == 'failed'"
    assert j["bucket"] == "1m"


def test_sum_sets_field():
    op = tl.sum("amount", window="1h")
    assert op.supports_retraction is True
    j = op.to_json("total")
    assert j["type"] == "sum"
    assert j["field"] == "amount"
    assert j["window"] == "1h"


def test_avg_and_variance_and_stddev():
    for ctor, label, retr in (
        (tl.avg, "avg", True),
        (tl.variance, "variance", True),
        (tl.stddev, "stddev", True),
    ):
        op = ctor("amount", window="1h")
        assert op.supports_retraction is retr
        j = op.to_json("x")
        assert j["type"] == label


def test_min_max_not_retractable():
    for ctor, label in ((tl.min, "min"), (tl.max, "max")):
        op = ctor("amount", window="1h")
        assert op.supports_retraction is False
        j = op.to_json("x")
        assert j["type"] == label
        assert j["supports_retraction"] is False


def test_percentile_defaults_and_params():
    op = tl.percentile("latency_ms", 0.95, window="5m")
    assert op.supports_retraction is False
    assert op.quantile == 0.95
    assert op.hybrid_params == {"exact_threshold": 256, "hybrid_alpha": 0.01}
    j = op.to_json("p95")
    assert j["type"] == "percentile"
    assert j["quantile"] == 0.95
    assert j["exact_threshold"] == 256
    assert j["hybrid_alpha"] == 0.01


def test_count_distinct_defaults():
    op = tl.count_distinct("merchant_id", window="24h")
    assert op.supports_retraction is False
    assert op.hybrid_params == {"exact_threshold": 1024, "hybrid_precision": 14}
    j = op.to_json("uniq")
    assert j["type"] == "count_distinct"
    assert j["exact_threshold"] == 1024
    assert j["hybrid_precision"] == 14


def test_top_k_defaults():
    op = tl.top_k("merchant_id", 10, window="1h")
    assert op.supports_retraction is False
    assert op.k == 10
    assert op.hybrid_params == {
        "exact_threshold": 1024,
        "hybrid_width": 2048,
        "hybrid_depth": 4,
    }
    j = op.to_json("top")
    assert j["type"] == "top_k"
    assert j["k"] == 10


def test_first_last_no_window():
    for ctor, label in ((tl.first, "first"), (tl.last, "last")):
        op = ctor("country")
        assert op.requires_window is False
        assert op.supports_retraction is False
        j = op.to_json("x")
        assert j["type"] == label
        assert j["field"] == "country"
        assert "window" not in j


def test_first_n_last_n():
    for ctor, label in ((tl.first_n, "first_n"), (tl.last_n, "last_n")):
        op = ctor("amount", 5)
        assert op.requires_window is False
        assert op.n == 5
        j = op.to_json("x")
        assert j["type"] == label
        assert j["n"] == 5


def test_ema():
    op = tl.ema("amount", half_life="1h")
    assert op.requires_window is False
    assert op.half_life == "1h"
    j = op.to_json("x")
    assert j["type"] == "ema"
    assert j["half_life"] == "1h"


def test_lag():
    op = tl.lag("amount", 3)
    assert op.requires_window is False
    assert op.n == 3
    j = op.to_json("x")
    assert j["type"] == "lag"
    assert j["n"] == 3


# ---------------------------------------------------------------------------
# Invalid construction
# ---------------------------------------------------------------------------


def test_count_missing_window_raises():
    with pytest.raises(TypeError):
        tl.count()  # window= required kw-only


def test_sum_requires_field():
    with pytest.raises(TypeError):
        tl.sum("", window="1h")


def test_percentile_quantile_out_of_range():
    with pytest.raises(ValueError, match=r"quantile must be in"):
        tl.percentile("x", 1.5, window="1h")
    with pytest.raises(ValueError, match=r"quantile must be in"):
        tl.percentile("x", -0.1, window="1h")


def test_first_n_negative_n():
    with pytest.raises(ValueError, match=r"n must be a positive int"):
        tl.first_n("amount", 0)
    with pytest.raises(ValueError, match=r"n must be a positive int"):
        tl.last_n("amount", -1)
    with pytest.raises(ValueError, match=r"n must be a positive int"):
        tl.lag("amount", 0)


def test_top_k_bad_k():
    with pytest.raises(ValueError, match=r"k must be a positive int"):
        tl.top_k("merchant_id", 0, window="1h")


def test_ema_bad_half_life():
    with pytest.raises(ValueError, match=r"half_life must be a duration"):
        tl.ema("amount", "not a window")


def test_count_bad_window_format():
    with pytest.raises(ValueError, match=r"window must be a duration"):
        tl.count(window="forever")


# ---------------------------------------------------------------------------
# GroupBy.agg schema inference
# ---------------------------------------------------------------------------


def _build_stream():
    @tl.stream
    class Transactions:
        user_id: str
        amount: float
        merchant_id: str
        status: str
    return Transactions


def test_groupby_agg_builds_table_with_inferred_schema():
    Transactions = _build_stream()
    t = Transactions.group_by("user_id").agg(
        n=tl.count(window="1h"),
        total=tl.sum("amount", window="1h"),
    )
    # Result is a TableDerivation
    assert isinstance(t, tl.TableDerivation)
    assert t._key == ["user_id"]
    # describe() exposes the inferred schema
    d = t.describe()
    assert d["kind"] == "table"
    assert d["key"] == ["user_id"]
    # Fields: group key first, then features in kwarg order
    names = list(d["fields"].keys())
    assert names == ["user_id", "n", "total"]
    # Types: user_id preserves str; count=int; sum=float
    assert t._schema["user_id"].py_type is str
    assert t._schema["n"].py_type is int
    assert t._schema["total"].py_type is float


def test_groupby_agg_min_max_preserve_input_type():
    @tl.stream
    class S:
        k: str
        v: int
    t = S.group_by("k").agg(
        lo=tl.min("v", window="1h"),
        hi=tl.max("v", window="1h"),
    )
    assert t._schema["lo"].py_type is int
    assert t._schema["hi"].py_type is int


def test_groupby_unknown_key_raises():
    Transactions = _build_stream()
    with pytest.raises(TypeError, match=r"not in Transactions"):
        Transactions.group_by("not_a_field")


def test_groupby_requires_at_least_one_key():
    Transactions = _build_stream()
    with pytest.raises(TypeError, match=r"requires at least one key"):
        Transactions.group_by()


def test_agg_requires_aggop_values():
    Transactions = _build_stream()
    with pytest.raises(TypeError, match=r"requires a tally aggregation operator"):
        Transactions.group_by("user_id").agg(total=42)


def test_agg_field_not_in_schema():
    Transactions = _build_stream()
    with pytest.raises(TypeError, match=r"not in Transactions"):
        Transactions.group_by("user_id").agg(
            total=tl.sum("not_a_field", window="1h"),
        )


def test_agg_feature_name_collides_with_key():
    Transactions = _build_stream()
    with pytest.raises(TypeError, match=r"collides with group key"):
        Transactions.group_by("user_id").agg(
            user_id=tl.count(window="1h"),
        )


def test_agg_empty_features_raises():
    Transactions = _build_stream()
    with pytest.raises(TypeError, match=r"at least one feature"):
        Transactions.group_by("user_id").agg()


# ---------------------------------------------------------------------------
# AggregationSpec compile-for-server stub
# ---------------------------------------------------------------------------


def test_agg_spec_compile_raises_phase_22():
    Transactions = _build_stream()
    t = Transactions.group_by("user_id").agg(
        n=tl.count(window="1h"),
    )
    spec = t._agg_spec
    assert isinstance(spec, AggregationSpec)
    with pytest.raises(NotImplementedError, match=r"ships in Phase 22"):
        spec._compile_for_server()


def test_agg_spec_to_feature_list_shape():
    Transactions = _build_stream()
    t = Transactions.group_by("user_id").agg(
        n=tl.count(window="1h"),
        total=tl.sum("amount", window="1h"),
    )
    feats = t._agg_spec._to_feature_list()
    assert len(feats) == 2
    assert feats[0]["name"] == "n" and feats[0]["type"] == "count"
    assert feats[1]["name"] == "total" and feats[1]["type"] == "sum"
    assert feats[1]["field"] == "amount"
