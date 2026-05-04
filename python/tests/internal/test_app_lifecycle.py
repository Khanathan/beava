"""Phase 13.5 Plan 02: bv.App lifecycle red tests.

Validates the 7 wire-mapped methods + context manager invariants per
docs/sdk-api/python.md § App class. Transport is mocked; Plan 11 runs
the same surface against the real engine.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import beava as bv


def test_app_construct_no_url_uses_embed_mode() -> None:
    app = bv.App()
    assert app._transport_kind == "embed"


def test_app_construct_http_url() -> None:
    app = bv.App(url="http://localhost:7777")
    assert app._transport_kind == "http"


def test_app_construct_tcp_url() -> None:
    app = bv.App(url="tcp://localhost:7778")
    assert app._transport_kind == "tcp"


def test_register_calls_transport_with_force_dry_run_kwargs() -> None:
    """Plan 11 update: App.register builds the wire JSON payload and passes
    a ``bytes`` argument to ``transport.send_register(payload_json: bytes)``.
    The ``force`` / ``dry_run`` flags are encoded into the JSON payload, not
    passed as separate kwargs (so they survive the wire protocol)."""
    import json

    with patch("beava._app.make_transport") as mk:
        t = MagicMock()
        t.send_register.return_value = {"status": "ok", "registry_version": 1}
        mk.return_value = t
        with bv.App() as app:
            app.register(force=True, dry_run=True)
        t.send_register.assert_called_once()
        # Single positional bytes arg; payload should encode the flags.
        args = t.send_register.call_args.args
        assert len(args) == 1
        assert isinstance(args[0], bytes)
        payload = json.loads(args[0].decode("utf-8"))
        assert payload.get("force") is True
        assert payload.get("dry_run") is True
        assert payload.get("nodes") == []


def test_push_signature_event_name_and_fields() -> None:
    with patch("beava._app.make_transport") as mk:
        t = MagicMock()
        t.send_push.return_value = {"ack_lsn": 42, "registry_version": 1}
        mk.return_value = t
        with bv.App() as app:
            r = app.push("Txn", {"user_id": "alice", "amount": 1.0})
        assert r["ack_lsn"] == 42


def test_get_returns_row_shape_dict() -> None:
    with patch("beava._app.make_transport") as mk:
        t = MagicMock()
        t.send_get.return_value = {"feature_a": 1, "feature_b": 2.5}
        mk.return_value = t
        with bv.App() as app:
            r = app.get("MyTable", "alice")
        assert r == {"feature_a": 1, "feature_b": 2.5}


def test_batch_get_returns_list_in_request_order() -> None:
    with patch("beava._app.make_transport") as mk:
        t = MagicMock()
        t.send_batch_get.return_value = [{"x": 1}, {"x": 2}, {}]
        mk.return_value = t
        with bv.App() as app:
            r = app.batch_get([("T1", "a"), ("T2", "b"), ("T3", "c")])
        assert r == [{"x": 1}, {"x": 2}, {}]
        assert len(r) == 3


def test_reset_calls_transport() -> None:
    with patch("beava._app.make_transport") as mk:
        t = MagicMock()
        t.send_reset.return_value = None
        mk.return_value = t
        with bv.App() as app:
            app.reset()
        t.send_reset.assert_called_once()


def test_ping_returns_server_version_and_registry_version() -> None:
    with patch("beava._app.make_transport") as mk:
        t = MagicMock()
        t.send_ping.return_value = {"server_version": "0.0.0", "registry_version": 0}
        mk.return_value = t
        with bv.App() as app:
            r = app.ping()
        assert "server_version" in r
        assert "registry_version" in r


def test_close_is_idempotent() -> None:
    with patch("beava._app.make_transport") as mk:
        mk.return_value = MagicMock()
        app = bv.App(url="http://localhost:7777")
        app.close()
        app.close()  # second call must not raise


def test_embed_mode_requires_context_manager() -> None:
    """Calling register on an embed-mode App outside `with` raises RuntimeError per docs/sdk-api/python.md."""
    app = bv.App()  # embed mode, no `with`
    with pytest.raises(RuntimeError, match="context manager"):
        app.register()
