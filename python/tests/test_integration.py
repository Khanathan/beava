"""End-to-end integration tests for the Tally Python SDK against a live server.

These tests prove the full round-trip: Python SDK encodes commands, sends over
TCP to the Rust server, server processes and responds, Python decodes the
response. If any byte in the wire format is wrong, these tests fail.

Covers all SDK-* requirements:
  SDK-01: @stream decorator defines streams with operators
  SDK-02: @view decorator (not tested here -- unit tests cover it)
  SDK-03: Operator descriptors serialize to correct JSON
  SDK-04: Protocol encoding matches Rust wire format
  SDK-05: app.push() returns FeatureResult with correct computed values
  SDK-06: app.get() / app.set() / app.mset() work correctly
  SDK-07: app.register() sends pipeline definitions to server

Requires: ``tally_server`` and ``app`` fixtures from conftest.py.
"""

from __future__ import annotations

import tally as st


# ---------------------------------------------------------------------------
# Stream definition used by multiple tests
# ---------------------------------------------------------------------------

@st.stream(key="user_id")
class Transactions:
    tx_count_1h = st.count(window="1h")
    tx_sum_1h = st.sum("amount", window="1h")
    avg_amount_1h = st.avg("amount", window="1h")
    rate = st.derive("tx_count_1h / tx_sum_1h")


# ---------------------------------------------------------------------------
# Registration and push (SDK-01, SDK-03, SDK-05, SDK-07)
# ---------------------------------------------------------------------------


def test_register_and_push(app):
    """Register a stream and push one event; verify feature values."""
    app.register(Transactions)

    features = app.push(Transactions, {"user_id": "u1", "amount": 100.0})
    assert features.tx_count_1h == 1
    assert features.tx_sum_1h == 100.0
    assert features.avg_amount_1h == 100.0


# ---------------------------------------------------------------------------
# Push accumulation (SDK-05)
# ---------------------------------------------------------------------------


def test_push_accumulates(app):
    """Multiple pushes accumulate feature values for the same key."""
    app.push(Transactions, {"user_id": "u2", "amount": 50.0})
    features = app.push(Transactions, {"user_id": "u2", "amount": 30.0})

    assert features.tx_count_1h == 2
    assert features.tx_sum_1h == 80.0
    assert features.avg_amount_1h == 40.0


# ---------------------------------------------------------------------------
# GET returns current features (SDK-06)
# ---------------------------------------------------------------------------


def test_get_features(app):
    """GET returns features previously written by push."""
    # u2 was pushed twice in test_push_accumulates (session-scoped server)
    all_features = app.get("u2")
    assert all_features.tx_count_1h == 2
    assert all_features.tx_sum_1h == 80.0


# ---------------------------------------------------------------------------
# GET unknown key returns empty (SDK-06)
# ---------------------------------------------------------------------------


def test_get_unknown_key(app):
    """GET for an unknown key returns a FeatureResult with no live features.

    Note: derive features may evaluate to None for keys with no events,
    since the Transactions stream is already registered. The key insight
    is that no windowed aggregation features (count, sum, avg) appear.
    """
    result = app.get("nonexistent_key_xyz")
    d = result.to_dict()
    # No windowed aggregation features should be present
    assert d.get("tx_count_1h") is None or "tx_count_1h" not in d
    assert d.get("tx_sum_1h") is None or "tx_sum_1h" not in d


# ---------------------------------------------------------------------------
# SET static features (SDK-06)
# ---------------------------------------------------------------------------


def test_set_features(app):
    """SET writes static features readable via GET."""
    app.set("u3", {"lifetime_value": 4500.0, "segment": "high_value"})
    result = app.get("u3")
    assert result.lifetime_value == 4500.0
    assert result.segment == "high_value"


# ---------------------------------------------------------------------------
# MSET bulk write (SDK-06)
# ---------------------------------------------------------------------------


def test_mset_bulk(app):
    """MSET writes features for multiple keys at once."""
    app.mset({
        "bulk1": {"score": 0.9},
        "bulk2": {"score": 0.3},
    })
    r1 = app.get("bulk1")
    r2 = app.get("bulk2")
    assert r1.score == 0.9
    assert r2.score == 0.3


# ---------------------------------------------------------------------------
# FeatureResult typed access (SDK-05)
# ---------------------------------------------------------------------------


def test_feature_result_types(app):
    """FeatureResult provides typed attribute access for different value types."""
    app.set("typed", {"f": 1.5, "i": 42, "s": "hello"})
    r = app.get("typed")
    assert isinstance(r.f, float)
    assert isinstance(r.i, (int, float))  # JSON numbers may decode as int or float
    assert isinstance(r.s, str)
    assert r.f == 1.5
    assert r.s == "hello"


# ---------------------------------------------------------------------------
# Derive expression evaluation (SDK-05)
# ---------------------------------------------------------------------------


def test_derive_expression(app):
    """Derive expressions are evaluated by the server and returned in push response."""
    # u1 had 1 push of amount=100 in test_register_and_push
    # rate = tx_count_1h / tx_sum_1h = 1 / 100 = 0.01
    features = app.get("u1")
    assert features.rate == 0.01


# ---------------------------------------------------------------------------
# Wire format conformance (success criterion 5)
# ---------------------------------------------------------------------------


def test_wire_conformance(app):
    """The fact that register+push+get+set+mset all succeed proves wire format
    conformance: Python encodes bytes, Rust decodes them correctly, Rust encodes
    the response, Python decodes it. Any byte mismatch would cause a failure."""
    # Round-trip test: SET then GET
    app.set("wire_test", {"check": 42})
    result = app.get("wire_test")
    assert result.check == 42


# ---------------------------------------------------------------------------
# Multiple streams on same server (SDK-07)
# ---------------------------------------------------------------------------


@st.stream(key="device_id")
class DeviceEvents:
    event_count_1h = st.count(window="1h")


def test_register_multiple_streams(app):
    """Register and push to a second stream on the same server."""
    app.register(DeviceEvents)
    features = app.push(DeviceEvents, {"device_id": "d1"})
    assert features.event_count_1h == 1


# ---------------------------------------------------------------------------
# Push returns all features including derive (SDK-05)
# ---------------------------------------------------------------------------


def test_push_returns_derive(app):
    """Push response includes derive features computed by the server."""
    # Push to a fresh key so we know exact state
    features = app.push(Transactions, {"user_id": "u_derive", "amount": 200.0})
    # rate = tx_count_1h / tx_sum_1h = 1 / 200 = 0.005
    assert features.tx_count_1h == 1
    assert features.tx_sum_1h == 200.0
    assert features.rate == 0.005


# ---------------------------------------------------------------------------
# FeatureResult dict-style access
# ---------------------------------------------------------------------------


def test_feature_result_dict_access(app):
    """FeatureResult supports dict-style access and to_dict()."""
    app.set("dict_test", {"a": 1, "b": "two"})
    r = app.get("dict_test")
    assert r["a"] == 1
    assert r["b"] == "two"
    d = r.to_dict()
    assert isinstance(d, dict)
    assert "a" in d
    assert "b" in d
