"""Tests for operator descriptor classes and their JSON serialization.

Each operator's to_json() output must match the Rust FeatureDefRequest schema:
  - name: str (feature name, passed to to_json)
  - type: str (operator type)
  - field: Optional[str] (for sum, avg, min, max, distinct_count, last)
  - window: Optional[str] (for count, sum, avg, min, max, distinct_count)
  - bucket: Optional[str] (optional bucket granularity)
  - expr: Optional[str] (for derive, lookup)
  - optional: Optional[bool] (for sum, avg)
"""

from __future__ import annotations

import pytest

from tally._operators import (
    Avg,
    Count,
    Derive,
    DistinctCount,
    Last,
    Lookup,
    Max,
    Min,
    OperatorBase,
    Sum,
)


# -----------------------------------------------------------------------
# OperatorBase: all operators are instances
# -----------------------------------------------------------------------


class TestOperatorBaseInheritance:
    """All operator classes must inherit from OperatorBase."""

    def test_count_is_operator_base(self) -> None:
        assert isinstance(Count(window="30m"), OperatorBase)

    def test_sum_is_operator_base(self) -> None:
        assert isinstance(Sum("amount", window="1h"), OperatorBase)

    def test_avg_is_operator_base(self) -> None:
        assert isinstance(Avg("amount", window="1h"), OperatorBase)

    def test_min_is_operator_base(self) -> None:
        assert isinstance(Min("amount", window="1h"), OperatorBase)

    def test_max_is_operator_base(self) -> None:
        assert isinstance(Max("amount", window="24h"), OperatorBase)

    def test_distinct_count_is_operator_base(self) -> None:
        assert isinstance(DistinctCount("merchant_id", window="24h"), OperatorBase)

    def test_last_is_operator_base(self) -> None:
        assert isinstance(Last("country"), OperatorBase)

    def test_derive_is_operator_base(self) -> None:
        assert isinstance(Derive("failed / total"), OperatorBase)

    def test_lookup_is_operator_base(self) -> None:
        assert isinstance(Lookup("MerchantActivity.chargeback_count_24h", on="merchant_id"), OperatorBase)


# -----------------------------------------------------------------------
# Count
# -----------------------------------------------------------------------


class TestCount:
    def test_basic_json(self) -> None:
        op = Count(window="30m")
        result = op.to_json("tx_count_30m")
        assert result == {"name": "tx_count_30m", "type": "count", "window": "30m"}

    def test_with_where_clause(self) -> None:
        op = Count(window="30m", where="status == 'failed'")
        result = op.to_json("failed")
        assert result == {
            "name": "failed",
            "type": "count",
            "window": "30m",
            "where": "status == 'failed'",
        }

    def test_with_bucket(self) -> None:
        op = Count(window="1h", bucket="1m")
        result = op.to_json("tx_count_1h")
        assert result == {
            "name": "tx_count_1h",
            "type": "count",
            "window": "1h",
            "bucket": "1m",
        }

    def test_no_optional_keys_when_none(self) -> None:
        """Absent optional params should not appear in JSON output."""
        op = Count(window="30m")
        result = op.to_json("c")
        assert "where" not in result
        assert "bucket" not in result
        assert "field" not in result
        assert "expr" not in result
        assert "optional" not in result

    def test_missing_window_raises(self) -> None:
        with pytest.raises(TypeError):
            Count()  # type: ignore[call-arg]


# -----------------------------------------------------------------------
# Sum
# -----------------------------------------------------------------------


class TestSum:
    def test_basic_json(self) -> None:
        op = Sum("amount", window="1h")
        result = op.to_json("tx_sum")
        assert result == {
            "name": "tx_sum",
            "type": "sum",
            "field": "amount",
            "window": "1h",
        }

    def test_with_optional_true(self) -> None:
        op = Sum("amount", window="1h", optional=True)
        result = op.to_json("tx_sum")
        assert result == {
            "name": "tx_sum",
            "type": "sum",
            "field": "amount",
            "window": "1h",
            "optional": True,
        }

    def test_optional_false_excluded(self) -> None:
        """optional=False is the default and should not appear in JSON."""
        op = Sum("amount", window="1h", optional=False)
        result = op.to_json("tx_sum")
        assert "optional" not in result

    def test_with_bucket(self) -> None:
        op = Sum("amount", window="1h", bucket="30s")
        result = op.to_json("s")
        assert result["bucket"] == "30s"

    def test_missing_field_raises(self) -> None:
        with pytest.raises(TypeError):
            Sum(window="1h")  # type: ignore[call-arg]

    def test_missing_window_raises(self) -> None:
        with pytest.raises(TypeError):
            Sum("amount")  # type: ignore[call-arg]


# -----------------------------------------------------------------------
# Avg
# -----------------------------------------------------------------------


class TestAvg:
    def test_basic_json(self) -> None:
        op = Avg("amount", window="1h")
        result = op.to_json("avg_amt")
        assert result == {
            "name": "avg_amt",
            "type": "avg",
            "field": "amount",
            "window": "1h",
        }

    def test_with_optional_true(self) -> None:
        op = Avg("amount", window="1h", optional=True)
        result = op.to_json("avg_amt")
        assert result["optional"] is True

    def test_optional_false_excluded(self) -> None:
        op = Avg("amount", window="1h")
        result = op.to_json("avg_amt")
        assert "optional" not in result

    def test_with_bucket(self) -> None:
        op = Avg("amount", window="1h", bucket="1m")
        result = op.to_json("a")
        assert result["bucket"] == "1m"


# -----------------------------------------------------------------------
# Min
# -----------------------------------------------------------------------


class TestMin:
    def test_basic_json(self) -> None:
        op = Min("amount", window="1h")
        result = op.to_json("min_amt")
        assert result == {
            "name": "min_amt",
            "type": "min",
            "field": "amount",
            "window": "1h",
        }

    def test_no_optional_flag(self) -> None:
        """Min does not have an optional flag."""
        op = Min("amount", window="1h")
        result = op.to_json("m")
        assert "optional" not in result

    def test_with_bucket(self) -> None:
        op = Min("amount", window="24h", bucket="5m")
        result = op.to_json("m")
        assert result["bucket"] == "5m"


# -----------------------------------------------------------------------
# Max
# -----------------------------------------------------------------------


class TestMax:
    def test_basic_json(self) -> None:
        op = Max("amount", window="24h")
        result = op.to_json("max_amt")
        assert result == {
            "name": "max_amt",
            "type": "max",
            "field": "amount",
            "window": "24h",
        }

    def test_no_optional_flag(self) -> None:
        op = Max("amount", window="24h")
        result = op.to_json("m")
        assert "optional" not in result


# -----------------------------------------------------------------------
# DistinctCount
# -----------------------------------------------------------------------


class TestDistinctCount:
    def test_basic_json(self) -> None:
        op = DistinctCount("merchant_id", window="24h")
        result = op.to_json("uniq")
        assert result == {
            "name": "uniq",
            "type": "distinct_count",
            "field": "merchant_id",
            "window": "24h",
        }

    def test_with_bucket(self) -> None:
        op = DistinctCount("user_id", window="1h", bucket="1m")
        result = op.to_json("dc")
        assert result["bucket"] == "1m"

    def test_missing_field_raises(self) -> None:
        with pytest.raises(TypeError):
            DistinctCount(window="24h")  # type: ignore[call-arg]


# -----------------------------------------------------------------------
# Last
# -----------------------------------------------------------------------


class TestLast:
    def test_basic_json(self) -> None:
        op = Last("country")
        result = op.to_json("last_c")
        assert result == {
            "name": "last_c",
            "type": "last",
            "field": "country",
        }

    def test_no_window_key(self) -> None:
        """Last does not have a window."""
        op = Last("country")
        result = op.to_json("lc")
        assert "window" not in result

    def test_missing_field_raises(self) -> None:
        with pytest.raises(TypeError):
            Last()  # type: ignore[call-arg]


# -----------------------------------------------------------------------
# Derive
# -----------------------------------------------------------------------


class TestDerive:
    def test_basic_json(self) -> None:
        op = Derive("failed / total")
        result = op.to_json("rate")
        assert result == {
            "name": "rate",
            "type": "derive",
            "expr": "failed / total",
        }

    def test_complex_expression(self) -> None:
        op = Derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
        result = op.to_json("velocity")
        assert result["expr"] == "(tx_count_1h / 1) / (tx_count_24h / 24)"

    def test_no_extra_keys(self) -> None:
        op = Derive("a + b")
        result = op.to_json("d")
        assert "field" not in result
        assert "window" not in result

    def test_missing_expr_raises(self) -> None:
        with pytest.raises(TypeError):
            Derive()  # type: ignore[call-arg]


# -----------------------------------------------------------------------
# Lookup
# -----------------------------------------------------------------------


class TestLookup:
    def test_basic_json(self) -> None:
        op = Lookup("MerchantActivity.chargeback_count_24h", on="merchant_id")
        result = op.to_json("merch_cb")
        assert result == {
            "name": "merch_cb",
            "type": "lookup",
            "target": "MerchantActivity.chargeback_count_24h",
            "on": "merchant_id",
        }

    def test_missing_target_raises(self) -> None:
        with pytest.raises(TypeError):
            Lookup(on="merchant_id")  # type: ignore[call-arg]

    def test_missing_on_raises(self) -> None:
        with pytest.raises(TypeError):
            Lookup("MerchantActivity.chargeback_count_24h")  # type: ignore[call-arg]


# -----------------------------------------------------------------------
# Backfill kwarg tests (SCHM-01/02)
# -----------------------------------------------------------------------


class TestBackfill:
    def test_count_backfill_default_false(self) -> None:
        """Count with default backfill should NOT include backfill key in JSON."""
        result = Count(window="1h").to_json("c")
        assert "backfill" not in result

    def test_count_backfill_true(self) -> None:
        """Count with backfill=True should include backfill: True in JSON."""
        result = Count(window="1h", backfill=True).to_json("c")
        assert result["backfill"] is True

    def test_sum_backfill_true(self) -> None:
        result = Sum("amount", window="1h", backfill=True).to_json("s")
        assert result["backfill"] is True

    def test_avg_backfill_true(self) -> None:
        result = Avg("amount", window="1h", backfill=True).to_json("a")
        assert result["backfill"] is True

    def test_min_backfill_true(self) -> None:
        result = Min("amount", window="1h", backfill=True).to_json("m")
        assert result["backfill"] is True

    def test_max_backfill_true(self) -> None:
        result = Max("amount", window="1h", backfill=True).to_json("m")
        assert result["backfill"] is True

    def test_distinct_count_backfill_true(self) -> None:
        result = DistinctCount("mid", window="1h", backfill=True).to_json("dc")
        assert result["backfill"] is True

    def test_last_backfill_true(self) -> None:
        result = Last("country", backfill=True).to_json("l")
        assert result["backfill"] is True

    def test_derive_no_backfill(self) -> None:
        """Derive does not have a backfill attribute."""
        d = Derive("a + b")
        assert not hasattr(d, "backfill")
        result = d.to_json("d")
        assert "backfill" not in result

    def test_sum_backfill_false_excluded(self) -> None:
        """backfill=False (default) should not appear in JSON."""
        result = Sum("amount", window="1h", backfill=False).to_json("s")
        assert "backfill" not in result

    def test_last_backfill_false_excluded(self) -> None:
        """backfill=False (default) should not appear in JSON."""
        result = Last("country", backfill=False).to_json("l")
        assert "backfill" not in result
