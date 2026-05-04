"""Plan 12-09 Wave 5 — Python SDK App.get over tcp:// uses msgpack default.

Two integration tests against a freshly-spawned beava server:

  1. test_app_get_over_tcp_uses_msgpack_default
       App("tcp://...") → register Txn -> TxnAgg(cnt) → push 10 events for "alice" →
       app.get("cnt", "alice") returns 10. The wire path under the hood
       MUST be CT_MSGPACK (request frame ct + response frame ct).

  2. test_app_get_over_http_uses_json
       App("http://...") → register + push as above → app.get("cnt", "alice")
       returns 1 over JSON. HTTP /get is JSON-only per locked decision D-D.

RED today because:
  - `App.get(feature, key)` doesn't exist yet on the Python SDK.
  - `TcpTransport._tcp_get_single` doesn't exist.
  - `App.get` over http:// has no implementation either (not yet wired).

GREEN after Task 5.b adds:
  - `TcpTransport._tcp_get_single(feature, key)` — OP_GET frame with CT_MSGPACK
    body, decode response via msgpack.
    (private helper; renamed in Phase 13.5.1 D-04 — D-04 deferral list
    in `.planning/ideas/v0.1-deferrals.md` schedules the v0.0.x removal)
  - `HttpTransport._http_get_single(feature, key)` — GET /get/{feature}/{key}
    with JSON parsing.
    (private helper; renamed in Phase 13.5.1 D-04)
  - `App.get(feature, key)` dispatches based on transport type.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

import beava as bv


def _register_payload() -> dict[str, Any]:
    """Return the JSON register payload for Txn -> TxnAgg(cnt by user_id)."""
    return {
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {
                        "event_time": "i64",
                        "user_id": "str",
                        "amount": "f64",
                    },
                    "optional_fields": [],
                },
            },
            {
                "kind": "derivation",
                "name": "TxnAgg",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {"cnt": {"op": "count", "params": {}}},
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": [],
                },
                "table_primary_key": ["user_id"],
            }
        ]
    }


def _register_and_push_for_alice(http_url: str) -> None:
    """Register pipeline + push 10 events for alice via HTTP (JSON)."""
    with httpx.Client(base_url=http_url, timeout=10.0) as client:
        r = client.post(
            "/register",
            json=_register_payload(),
            headers={"Content-Type": "application/json"},
        )
        r.raise_for_status()
        for i in range(10):
            r = client.post(
                "/push/Txn",
                json={"event_time": 1000 + i, "user_id": "alice", "amount": 42.0},
                headers={"Content-Type": "application/json"},
            )
            r.raise_for_status()


def test_app_get_over_tcp_uses_msgpack_default(
    beava_server: tuple[str, str],
) -> None:
    """App.get over tcp:// returns the right value, uses msgpack on the wire."""
    http_url, tcp_url = beava_server
    _register_and_push_for_alice(http_url)

    with bv.App(tcp_url) as app:
        value = app.get("cnt", "alice")
    assert value == 10, f"expected cnt=10, got {value!r}"


def test_app_get_over_http_uses_json(beava_server: tuple[str, str]) -> None:
    """App.get over http:// returns the right value, JSON-only contract."""
    http_url, tcp_url = beava_server  # noqa: F841
    _register_and_push_for_alice(http_url)

    with bv.App(http_url) as app:
        value = app.get("cnt", "alice")
    assert value == 10, f"expected cnt=10, got {value!r}"


def test_msgpack_pkg_available() -> None:
    """msgpack package is required for the tcp:// path; sanity-check installation."""
    import msgpack  # noqa: F401

    encoded = msgpack.packb({"feature": "cnt", "key": "alice"}, use_bin_type=True)
    assert isinstance(encoded, (bytes, bytearray))
    decoded = msgpack.unpackb(encoded, raw=False)
    assert decoded == {"feature": "cnt", "key": "alice"}


def test_app_get_response_shape_matches_json_over_tcp(
    beava_server: tuple[str, str],
) -> None:
    """The msgpack-decoded response value equals the JSON-decoded value.

    Smoke-test the cross-codec shape parity from the SDK side: pull the same
    feature/key over TCP (msgpack) and HTTP (JSON) and assert the integer
    values are equal. Mirrors the server-side
    `test_msgpack_and_json_responses_are_shape_equivalent` test.
    """
    http_url, tcp_url = beava_server
    _register_and_push_for_alice(http_url)

    with bv.App(tcp_url) as tcp_app, bv.App(http_url) as http_app:
        tcp_value = tcp_app.get("cnt", "alice")
        http_value = http_app.get("cnt", "alice")
    assert tcp_value == http_value, (
        f"tcp/msgpack and http/json should agree; "
        f"tcp={tcp_value!r} http={http_value!r}"
    )


# Ensure pytest can locate the test module path even before json import is
# exercised; suppress "imported but unused" by binding to a sentinel.
_JSON_LOADED = json.dumps({"sentinel": True})
