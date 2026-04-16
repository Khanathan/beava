"""Tests for beava._types: FeatureResult and exception hierarchy."""

import pytest
from beava._types import FeatureResult, BeavaError, ConnectionError, ProtocolError


# ---------------------------------------------------------------------------
# FeatureResult attribute access
# ---------------------------------------------------------------------------


class TestFeatureResultAttrAccess:
    def test_int_attribute(self):
        r = FeatureResult({"tx_count": 7, "rate": 0.14})
        assert r.tx_count == 7

    def test_float_attribute(self):
        r = FeatureResult({"tx_count": 7, "rate": 0.14})
        assert r.rate == 0.14

    def test_missing_attribute_raises(self):
        r = FeatureResult({"tx_count": 7})
        with pytest.raises(AttributeError, match="no feature named 'rate'"):
            _ = r.rate

    def test_none_value(self):
        r = FeatureResult({"x": None})
        assert r.x is None

    def test_string_value(self):
        r = FeatureResult({"country": "US"})
        assert r.country == "US"


# ---------------------------------------------------------------------------
# FeatureResult dict-style access
# ---------------------------------------------------------------------------


class TestFeatureResultDictAccess:
    def test_getitem(self):
        r = FeatureResult({"a": 1})
        assert r["a"] == 1

    def test_getitem_missing_raises(self):
        r = FeatureResult({"a": 1})
        with pytest.raises(KeyError):
            _ = r["b"]

    def test_contains(self):
        r = FeatureResult({"a": 1, "b": 2})
        assert "a" in r
        assert "c" not in r


# ---------------------------------------------------------------------------
# FeatureResult to_dict and repr
# ---------------------------------------------------------------------------


class TestFeatureResultConversions:
    def test_to_dict(self):
        r = FeatureResult({"a": 1})
        assert r.to_dict() == {"a": 1}

    def test_to_dict_returns_copy(self):
        data = {"a": 1}
        r = FeatureResult(data)
        d = r.to_dict()
        d["b"] = 2
        # Original should be unmodified
        assert "b" not in r

    def test_repr(self):
        r = FeatureResult({"a": 1})
        text = repr(r)
        assert "FeatureResult" in text
        assert "a" in text


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class TestExceptionHierarchy:
    def test_beava_error_is_exception(self):
        assert issubclass(BeavaError, Exception)

    def test_connection_error_is_beava_error(self):
        assert issubclass(ConnectionError, BeavaError)
        assert isinstance(ConnectionError("x"), BeavaError)

    def test_protocol_error_is_beava_error(self):
        assert issubclass(ProtocolError, BeavaError)
        assert isinstance(ProtocolError("x"), BeavaError)

    def test_connection_error_message(self):
        e = ConnectionError("server closed")
        assert str(e) == "server closed"

    def test_protocol_error_message(self):
        e = ProtocolError("bad frame")
        assert str(e) == "bad frame"
