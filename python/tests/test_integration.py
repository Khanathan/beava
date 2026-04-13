"""End-to-end integration tests for the Tally Python SDK against a live server.

These tests prove the full round-trip: Python SDK encodes commands, sends over
TCP to the Rust server, server processes and responds, Python decodes the
response. If any byte in the wire format is wrong, these tests fail.

Covers all SDK-* requirements:
  SDK-01: @source/@dataset defines pipelines with operators
  SDK-02: @dataset with derive-only (replaces @view)
  SDK-03: Operator descriptors serialize to correct JSON
  SDK-04: Protocol encoding matches Rust wire format
  SDK-05: app.push() returns FeatureResult with correct computed values
  SDK-06: app.get() / app.set() / app.mset() work correctly
  SDK-07: app.register() sends pipeline definitions to server

Requires: ``tally_server`` and ``app`` fixtures from conftest.py.
"""

from __future__ import annotations

import tally as tl
from tally import source, dataset, group_by

import pytest


# ---------------------------------------------------------------------------
# Pipeline definition used by multiple tests
# ---------------------------------------------------------------------------

@source
class RawTransactions:
    pass


@dataset(depends_on=[RawTransactions])
class Transactions:
    features = group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        avg_amount_1h=tl.avg("amount", window="1h"),
    )
    rate = tl.derive("tx_count_1h / tx_sum_1h")


# ---------------------------------------------------------------------------
# Registration and push (SDK-01, SDK-03, SDK-05, SDK-07)
# ---------------------------------------------------------------------------


def test_register_and_push(app):
    """Register a pipeline and push one event; verify feature values via GET.

    With the new @source/@dataset API, push targets the keyless source so the
    push response is empty. Features are verified via GET on the entity key.
    """
    app.register(RawTransactions, Transactions)

    app.push_sync(RawTransactions, {"user_id": "u1", "amount": 100.0})
    features = app.get("u1")
    assert features.tx_count_1h == 1
    assert features.tx_sum_1h == 100.0
    assert features.avg_amount_1h == 100.0


# ---------------------------------------------------------------------------
# Push accumulation (SDK-05)
# ---------------------------------------------------------------------------


def test_push_accumulates(app):
    """Multiple pushes accumulate feature values for the same key."""
    app.push_sync(RawTransactions, {"user_id": "u2", "amount": 50.0})
    app.push_sync(RawTransactions, {"user_id": "u2", "amount": 30.0})

    features = app.get("u2")
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


@source
class RawDeviceEvents:
    pass


@dataset(depends_on=[RawDeviceEvents])
class DeviceEvents:
    features = group_by("device_id").agg(
        event_count_1h=tl.count(window="1h"),
    )


def test_register_multiple_streams(app):
    """Register and push to a second pipeline on the same server."""
    app.register(RawDeviceEvents, DeviceEvents)
    app.push_sync(RawDeviceEvents, {"device_id": "d1"})
    features = app.get("d1")
    assert features.event_count_1h == 1


# ---------------------------------------------------------------------------
# Push returns all features including derive (SDK-05)
# ---------------------------------------------------------------------------


def test_push_returns_derive(app):
    """Push cascades through pipeline; derive features computed by the server."""
    # Push to a fresh key so we know exact state
    app.push_sync(RawTransactions, {"user_id": "u_derive", "amount": 200.0})
    features = app.get("u_derive")
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


# ===========================================================================
# Composable Pipeline Tests (Phase 7)
# ===========================================================================


def test_cascade_keyless_to_keyed(tally_server):
    """Push to keyless stream cascades to downstream keyed stream."""
    host, tcp_port, _ = tally_server

    @source
    class RawEvents_cascade1:
        pass

    @dataset(depends_on=[RawEvents_cascade1])
    class UserTx_cascade1:
        features = group_by("user_id").agg(
            tx_count=tl.count(window="1h"),
        )

    app = tl.App(f"{host}:{tcp_port}")
    app.register(RawEvents_cascade1, UserTx_cascade1)

    # Push to keyless stream (use push_sync for the empty feature-map return)
    result = app.push_sync(RawEvents_cascade1, {"user_id": "cascade_u1", "amount": 50.0})
    # Keyless stream returns empty features
    assert len(result._data) == 0

    # GET should show downstream features
    features = app.get("cascade_u1")
    assert features.tx_count == 1
    app.close()


def test_cascade_returns_error_on_cycle(tally_server):
    """Circular dependency is rejected at registration."""
    host, tcp_port, _ = tally_server
    from tally._protocol import encode_register, OP_REGISTER

    # Build cycle manually via raw JSON registrations to bypass
    # client-side DAG walk (which would recurse infinitely).
    # The server should detect the cycle and reject it.
    regs = [
        {"name": "CycleA_src_v2", "key_field": None, "features": []},
        {"name": "CycleA_v2", "key_field": "uid", "features": [], "depends_on": ["CycleB_v2"]},
        {"name": "CycleB_v2", "key_field": "uid", "features": [], "depends_on": ["CycleA_v2"]},
    ]

    app = tl.App(f"{host}:{tcp_port}")
    # Registration should fail with cycle error on the last register
    with pytest.raises(tl.ProtocolError):
        for reg in regs:
            payload = encode_register(reg)
            app._send(OP_REGISTER, payload)
    app.close()


def test_cascade_missing_key_skips_downstream(tally_server):
    """Push event missing downstream key_field skips that stream (LEFT JOIN)."""
    host, tcp_port, _ = tally_server

    @source
    class RawEvents_skip:
        pass

    @dataset(depends_on=[RawEvents_skip])
    class UserTx_skip:
        features = group_by("user_id").agg(
            tx_count=tl.count(window="1h"),
        )

    @dataset(depends_on=[RawEvents_skip])
    class MerchantTx_skip:
        features = group_by("merchant_id").agg(
            m_count=tl.count(window="1h"),
        )

    app = tl.App(f"{host}:{tcp_port}")
    app.register(RawEvents_skip, UserTx_skip, MerchantTx_skip)

    # Push event with user_id but NO merchant_id
    app.push(RawEvents_skip, {"user_id": "skip_u1", "amount": 10.0})

    # User features should exist
    user_features = app.get("skip_u1")
    assert user_features.tx_count == 1

    # Merchant entity should not exist (no merchant_id in event)
    merchant_features = app.get("skip_m1")
    d = merchant_features.to_dict()
    assert "m_count" not in d or d.get("m_count") is None
    app.close()


def test_cascade_with_filter(tally_server):
    """Stream-level filter controls which events cascade."""
    host, tcp_port, _ = tally_server

    @source
    class RawEvents_filter:
        pass

    @dataset(
        depends_on=[RawEvents_filter],
        filter="_event.status == 'failed'"
    )
    class FailedTx_filter:
        features = group_by("user_id").agg(
            fail_count=tl.count(window="1h"),
        )

    app = tl.App(f"{host}:{tcp_port}")
    app.register(RawEvents_filter, FailedTx_filter)

    # Push success event -- should NOT count
    app.push(RawEvents_filter, {"user_id": "filter_u1", "status": "success"})

    # Push failed event -- SHOULD count
    app.push(RawEvents_filter, {"user_id": "filter_u1", "status": "failed"})

    features = app.get("filter_u1")
    assert features.fail_count == 1  # Only the failed event counted
    app.close()


def test_cascade_multi_level(tally_server):
    """Multi-level cascade (3 deep) processes all levels."""
    host, tcp_port, _ = tally_server

    @source
    class Raw_multi:
        pass

    @dataset(depends_on=[Raw_multi])
    class Level1_multi:
        features = group_by("user_id").agg(
            l1_count=tl.count(window="1h"),
        )

    @dataset(depends_on=[Level1_multi])
    class Level2_multi:
        features = group_by("user_id").agg(
            l2_count=tl.count(window="1h"),
        )

    app = tl.App(f"{host}:{tcp_port}")
    app.register(Raw_multi, Level1_multi, Level2_multi)

    app.push(Raw_multi, {"user_id": "multi_u1"})

    features = app.get("multi_u1")
    assert features.l1_count == 1
    assert features.l2_count == 1
    app.close()
