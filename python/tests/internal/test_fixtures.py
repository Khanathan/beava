"""Phase 13.5 Plan 07: beava.test fixtures + assertions + MockApp red tests."""
from __future__ import annotations

import inspect

import pytest

from beava.test import MockApp, assert_features_eq, fixture, replay


def test_fixture_default_test_mode_true() -> None:
    """``fixture()`` defaults to ``test_mode=True`` per D-05 cross-amendment."""
    sig = inspect.signature(fixture)
    assert sig.parameters["test_mode"].default is True
    assert sig.parameters["reset_each"].default is True


def test_replay_calls_push_in_order() -> None:
    """``replay(app, events)`` calls ``app.push(name, fields)`` in order."""
    app = MockApp()
    events = [
        {"_event": "Txn", "user_id": "alice", "amount": 1.0},
        {"_event": "Txn", "user_id": "bob", "amount": 2.0},
        {"_event": "Login", "user_id": "alice"},
    ]
    replay(app, events)
    assert len(app._calls) == 3
    assert app._calls[0] == ("push", "Txn", {"user_id": "alice", "amount": 1.0})
    assert app._calls[1] == ("push", "Txn", {"user_id": "bob", "amount": 2.0})
    assert app._calls[2] == ("push", "Login", {"user_id": "alice"})


def test_assert_features_eq_exact_match() -> None:
    assert_features_eq({"a": 1, "b": "x"}, {"a": 1, "b": "x"})


def test_assert_features_eq_float_tolerance() -> None:
    """rel_tol=1e-9 — sketch ops are not bitwise stable."""
    assert_features_eq({"q": 1.0000000001}, {"q": 1.0})


def test_assert_features_eq_mismatch_raises_AssertionError() -> None:
    with pytest.raises(AssertionError, match="differ"):
        assert_features_eq({"a": 1}, {"a": 2})


def test_assert_features_eq_missing_key_raises_AssertionError() -> None:
    with pytest.raises(AssertionError, match="missing|extra"):
        assert_features_eq({"a": 1}, {"a": 1, "b": 2})


def test_mock_records_pushes() -> None:
    app = MockApp()
    app.push("Txn", {"user_id": "alice", "amount": 42.0})
    app.push("Txn", {"user_id": "bob", "amount": 13.0})
    assert app._calls == [
        ("push", "Txn", {"user_id": "alice", "amount": 42.0}),
        ("push", "Txn", {"user_id": "bob", "amount": 13.0}),
    ]


def test_mock_get_returns_canned_features() -> None:
    """``MockApp`` lets tests preset return values for ``app.get()``."""
    app = MockApp()
    app._set_get_response("UserStats", "alice", {"count": 5, "sum": 50.0})
    r = app.get("UserStats", "alice")
    assert r == {"count": 5, "sum": 50.0}


def test_mock_get_unknown_key_returns_empty_dict() -> None:
    """Cold-start contract per docs/sdk-api/python.md: unknown key returns ``{}``."""
    app = MockApp()
    assert app.get("Foo", "unknown") == {}


def test_mock_register_records_descriptors() -> None:
    app = MockApp()
    app.register("desc1", "desc2")
    assert app._calls[0][0] == "register"
    assert app._calls[0][1] == ("desc1", "desc2")


def test_mock_batch_get_returns_in_order() -> None:
    app = MockApp()
    app._set_get_response("T1", "a", {"x": 1})
    app._set_get_response("T2", "b", {"y": 2})
    r = app.batch_get([("T1", "a"), ("T2", "b"), ("T3", "c")])
    assert r == [{"x": 1}, {"y": 2}, {}]


def test_mock_reset_clears_state() -> None:
    app = MockApp()
    app._set_get_response("T", "a", {"x": 1})
    app.reset()
    assert app.get("T", "a") == {}
