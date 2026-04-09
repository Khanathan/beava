"""Tests for the @stream decorator and StreamMeta metaclass.

Verifies:
- Feature collection from class body
- Mixin inheritance support (multiple inheritance, MRO)
- Class body overrides mixin features with same name
- _tally_key_field, _tally_stream_name, _tally_is_view metadata
- _to_register_json() produces correct RegisterRequest dict
- Validation: missing key raises TypeError
"""

from __future__ import annotations

import pytest

from tally._operators import Avg, Count, Derive, Last, Lookup, Sum
from tally._stream import StreamMeta, stream


# -----------------------------------------------------------------------
# Basic @stream decorator
# -----------------------------------------------------------------------


class TestStreamDecorator:
    def test_basic_stream_collects_features(self) -> None:
        @stream(key="user_id")
        class Transactions:
            tx_count = Count(window="30m")

        assert "tx_count" in Transactions._tally_features
        assert isinstance(Transactions._tally_features["tx_count"], Count)

    def test_key_field_set(self) -> None:
        @stream(key="user_id")
        class Transactions:
            tx_count = Count(window="30m")

        assert Transactions._tally_key_field == "user_id"

    def test_stream_name_is_class_name(self) -> None:
        @stream(key="user_id")
        class Transactions:
            tx_count = Count(window="30m")

        assert Transactions._tally_stream_name == "Transactions"

    def test_is_view_false(self) -> None:
        @stream(key="user_id")
        class Transactions:
            tx_count = Count(window="30m")

        assert Transactions._tally_is_view is False

    def test_empty_stream_is_valid(self) -> None:
        @stream(key="user_id")
        class EmptyStream:
            pass

        assert EmptyStream._tally_features == {}
        assert EmptyStream._tally_key_field == "user_id"

    def test_multiple_features(self) -> None:
        @stream(key="user_id")
        class Transactions:
            tx_count_30m = Count(window="30m")
            tx_count_1h = Count(window="1h")
            tx_sum_1h = Sum("amount", window="1h")
            avg_amount_1h = Avg("amount", window="1h")
            last_country = Last("country")

        assert len(Transactions._tally_features) == 5
        assert isinstance(Transactions._tally_features["tx_count_30m"], Count)
        assert isinstance(Transactions._tally_features["tx_sum_1h"], Sum)
        assert isinstance(Transactions._tally_features["last_country"], Last)

    def test_non_operator_attributes_ignored(self) -> None:
        @stream(key="user_id")
        class Transactions:
            tx_count = Count(window="30m")
            helper_value = 42
            helper_string = "not an operator"

        assert len(Transactions._tally_features) == 1
        assert "helper_value" not in Transactions._tally_features

    def test_missing_key_raises(self) -> None:
        with pytest.raises(TypeError):
            @stream()  # type: ignore[call-arg]
            class Bad:
                pass


# -----------------------------------------------------------------------
# Mixin support
# -----------------------------------------------------------------------


class TestMixinSupport:
    def test_mixin_features_collected(self) -> None:
        class VelocityMixin:
            count_1h = Count(window="1h")
            count_24h = Count(window="24h")

        @stream(key="user_id")
        class Transactions(VelocityMixin):
            pass

        assert "count_1h" in Transactions._tally_features
        assert "count_24h" in Transactions._tally_features
        assert len(Transactions._tally_features) == 2

    def test_class_body_overrides_mixin(self) -> None:
        class VelocityMixin:
            count_1h = Count(window="1h")

        @stream(key="user_id")
        class Transactions(VelocityMixin):
            count_1h = Count(window="30m")  # Override

        assert Transactions._tally_features["count_1h"].window == "30m"

    def test_multiple_mixins_merge(self) -> None:
        class VelocityMixin:
            count_1h = Count(window="1h")
            count_24h = Count(window="24h")

        class AmountMixin:
            total_1h = Sum("amount", window="1h")
            avg_1h = Avg("amount", window="1h")

        @stream(key="user_id")
        class Transactions(VelocityMixin, AmountMixin):
            failed_count_1h = Count(window="1h", where="status == 'failed'")

        # 2 from VelocityMixin + 2 from AmountMixin + 1 from body
        assert len(Transactions._tally_features) == 5
        assert "count_1h" in Transactions._tally_features
        assert "total_1h" in Transactions._tally_features
        assert "failed_count_1h" in Transactions._tally_features

    def test_full_claude_md_example(self) -> None:
        """Full mixin example from CLAUDE.md."""

        class VelocityMixin:
            count_1h = Count(window="1h")
            count_24h = Count(window="24h")
            rate_spike = Derive("(count_1h / 1) / (count_24h / 24)")

        class AmountMixin:
            total_1h = Sum("amount", window="1h")
            avg_1h = Avg("amount", window="1h")

        @stream(key="user_id")
        class Transactions(VelocityMixin, AmountMixin):
            failed_count_1h = Count(window="1h", where="status == 'failed'")
            failure_rate = Derive("failed_count_1h / count_1h")

        # 3 from VelocityMixin + 2 from AmountMixin + 2 from body = 7
        assert len(Transactions._tally_features) == 7
        assert isinstance(Transactions._tally_features["rate_spike"], Derive)
        assert isinstance(Transactions._tally_features["failure_rate"], Derive)


# -----------------------------------------------------------------------
# _to_register_json
# -----------------------------------------------------------------------


class TestToRegisterJson:
    def test_basic_register_json(self) -> None:
        @stream(key="user_id")
        class Transactions:
            tx_count_30m = Count(window="30m")

        result = Transactions._to_register_json()
        assert result["name"] == "Transactions"
        assert result["key_field"] == "user_id"
        assert len(result["features"]) == 1
        assert result["features"][0] == {"name": "tx_count_30m", "type": "count", "window": "30m"}

    def test_multi_feature_register_json(self) -> None:
        @stream(key="user_id")
        class Transactions:
            tx_count = Count(window="30m")
            tx_sum = Sum("amount", window="1h")
            rate = Derive("tx_sum / tx_count")

        result = Transactions._to_register_json()
        assert result["name"] == "Transactions"
        assert result["key_field"] == "user_id"
        assert len(result["features"]) == 3

        # Features should be present (order may vary by dict iteration)
        feature_names = {f["name"] for f in result["features"]}
        assert feature_names == {"tx_count", "tx_sum", "rate"}

    def test_register_json_with_mixin(self) -> None:
        class VelocityMixin:
            count_1h = Count(window="1h")

        @stream(key="user_id")
        class Transactions(VelocityMixin):
            tx_sum = Sum("amount", window="1h")

        result = Transactions._to_register_json()
        feature_names = {f["name"] for f in result["features"]}
        assert feature_names == {"count_1h", "tx_sum"}


# -----------------------------------------------------------------------
# StreamMeta metaclass
# -----------------------------------------------------------------------


class TestStreamMeta:
    def test_metaclass_is_stream_meta(self) -> None:
        @stream(key="user_id")
        class Tx:
            pass

        assert type(Tx) is StreamMeta
