"""Tests for the @view decorator.

Verifies:
- View collects only Derive and Lookup features
- View rejects non-derive/non-lookup operators at definition time
- View has _tally_is_view == True
- View _to_register_json works correctly
"""

from __future__ import annotations

import pytest

from tally._operators import Count, Derive, Lookup, Sum
from tally._view import view


# -----------------------------------------------------------------------
# Basic @view decorator
# -----------------------------------------------------------------------


class TestViewDecorator:
    def test_view_with_derive_features(self) -> None:
        @view(key="user_id")
        class UserRisk:
            risk_score = Derive("tx_count_1h > 10")

        assert "risk_score" in UserRisk._tally_features
        assert isinstance(UserRisk._tally_features["risk_score"], Derive)

    def test_view_is_view_true(self) -> None:
        @view(key="user_id")
        class UserRisk:
            score = Derive("a + b")

        assert UserRisk._tally_is_view is True

    def test_view_key_field(self) -> None:
        @view(key="user_id")
        class UserRisk:
            score = Derive("a + b")

        assert UserRisk._tally_key_field == "user_id"

    def test_view_stream_name(self) -> None:
        @view(key="user_id")
        class UserRisk:
            score = Derive("a + b")

        assert UserRisk._tally_stream_name == "UserRisk"

    def test_view_with_lookup_feature(self) -> None:
        @view(key="user_id")
        class FraudSignals:
            merchant_chargebacks = Lookup(
                "MerchantActivity.chargeback_count_24h", on="merchant_id"
            )

        assert "merchant_chargebacks" in FraudSignals._tally_features
        assert isinstance(FraudSignals._tally_features["merchant_chargebacks"], Lookup)

    def test_view_with_derive_and_lookup(self) -> None:
        @view(key="user_id")
        class FraudSignals:
            merchant_cb = Lookup(
                "MerchantActivity.chargeback_count_24h", on="merchant_id"
            )
            risk = Derive("Transactions.velocity_spike > 3 and merchant_cb > 5")

        assert len(FraudSignals._tally_features) == 2

    def test_empty_view_is_valid(self) -> None:
        @view(key="user_id")
        class EmptyView:
            pass

        assert EmptyView._tally_features == {}


# -----------------------------------------------------------------------
# View restrictions: reject non-derive/non-lookup operators
# -----------------------------------------------------------------------


class TestViewRestrictions:
    def test_view_rejects_count(self) -> None:
        with pytest.raises(TypeError, match="only.*derive.*lookup"):
            @view(key="user_id")
            class Bad:
                c = Count(window="30m")

    def test_view_rejects_sum(self) -> None:
        with pytest.raises(TypeError, match="only.*derive.*lookup"):
            @view(key="user_id")
            class Bad:
                s = Sum("amount", window="1h")

    def test_view_rejects_mixed_valid_invalid(self) -> None:
        """Even one non-derive/non-lookup feature should fail the whole view."""
        with pytest.raises(TypeError, match="only.*derive.*lookup"):
            @view(key="user_id")
            class Bad:
                rate = Derive("a / b")
                c = Count(window="30m")


# -----------------------------------------------------------------------
# View _to_register_json
# -----------------------------------------------------------------------


class TestViewRegisterJson:
    def test_view_register_json(self) -> None:
        @view(key="user_id")
        class UserRisk:
            score = Derive("tx_count_1h > 10")
            cb = Lookup("MerchantActivity.chargeback_count_24h", on="merchant_id")

        result = UserRisk._to_register_json()
        assert result["name"] == "UserRisk"
        assert result["key_field"] == "user_id"
        assert len(result["features"]) == 2

        feature_names = {f["name"] for f in result["features"]}
        assert feature_names == {"score", "cb"}


# -----------------------------------------------------------------------
# View missing key
# -----------------------------------------------------------------------


class TestViewValidation:
    def test_missing_key_raises(self) -> None:
        with pytest.raises(TypeError):
            @view()  # type: ignore[call-arg]
            class Bad:
                pass
