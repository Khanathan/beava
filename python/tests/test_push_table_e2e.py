"""Phase 24-02 Task 2 — End-to-end Python SDK tests for OP_PUSH_TABLE /
OP_DELETE_TABLE and the merged GET view.

Runs against the session-scoped ``tally_server`` fixture in conftest.py.
"""

from __future__ import annotations

import pytest

import tally as tl
from tally._protocol import (
    OP_DELETE_TABLE,
    OP_PUSH_TABLE,
    encode_delete_table,
    encode_push_table,
    encode_string,
)
from tally._types import ProtocolError


# ---------------------------------------------------------------------------
# Wire-format unit tests (no server needed)
# ---------------------------------------------------------------------------


def test_encode_push_table_wire_format_matches_rust():
    """``encode_push_table`` emits ``[u16 name][u16 key][JSON]`` verbatim."""
    out = encode_push_table("UserProfile", "u123", {"country": "US", "score": 42})

    # [u16 name_len=11]["UserProfile"]
    assert out[:2] == b"\x00\x0b"
    assert out[2:13] == b"UserProfile"
    # [u16 key_len=4]["u123"]
    assert out[13:15] == b"\x00\x04"
    assert out[15:19] == b"u123"
    # JSON tail — parse to avoid key-ordering brittleness
    import json
    tail = out[19:].decode("utf-8")
    assert json.loads(tail) == {"country": "US", "score": 42}


def test_encode_delete_table_wire_format():
    """``encode_delete_table`` emits just the two u16-prefixed strings."""
    out = encode_delete_table("UserProfile", "u123")
    assert out == encode_string("UserProfile") + encode_string("u123")
    assert len(out) == 2 + len("UserProfile") + 2 + len("u123")


# ---------------------------------------------------------------------------
# End-to-end tests — use session-scoped tally_server fixture.
# ---------------------------------------------------------------------------


def test_push_delete_get_roundtrip(app):
    """Push a row, GET sees flattened Table.field; delete, GET filters it."""

    @tl.table(key="user_id")
    class E2EProfile:
        user_id: str
        country: str
        score: int

    app.register(E2EProfile)

    # Push a Table row via the new 3-arg overload.
    app.push(E2EProfile, "pt_u1", {"country": "US", "score": 42})

    row = app.get("pt_u1").to_dict()
    assert row.get("E2EProfile.country") == "US", f"got: {row!r}"
    assert row.get("E2EProfile.score") == 42, f"got: {row!r}"

    # Delete and verify the row is filtered from GET (T-24-02-03).
    app.delete(E2EProfile, "pt_u1")
    row = app.get("pt_u1").to_dict()
    assert "E2EProfile.country" not in row, f"tombstoned row leaked: {row!r}"
    assert "E2EProfile.score" not in row, f"tombstoned row leaked: {row!r}"


def test_push_stream_vs_push_table_disambiguation(app):
    """Same App can push to a Stream (2-arg) and a Table (3-arg) without crossing wires."""

    @tl.stream
    class E2EClicks:
        user_id: str
        page: str

    @tl.table(key="user_id")
    class E2EBuyers:
        user_id: str
        plan: str

    app.register(E2EClicks, E2EBuyers)

    # Stream form — fire-and-forget.
    app.push(E2EClicks, {"user_id": "dis_u1", "page": "/home"})

    # Table form — sync push-through.
    app.push(E2EBuyers, "dis_u1", {"plan": "gold"})
    app.flush()  # drain any pending stream pushes

    row = app.get("dis_u1").to_dict()
    assert row.get("E2EBuyers.plan") == "gold", f"got: {row!r}"


def test_push_table_unknown_table_raises_protocol_error(app):
    """Pushing a Table that was not registered surfaces a ProtocolError."""

    @tl.table(key="user_id")
    class NotRegisteredTable:
        user_id: str
        x: int

    # Deliberately do NOT register. The server rejects with STATUS_ERROR
    # and the SDK converts that into :class:`ProtocolError`.
    with pytest.raises(ProtocolError) as ei:
        app.push(NotRegisteredTable, "u1", {"x": 1})
    assert "unknown table" in str(ei.value).lower()


def test_delete_unknown_table_raises_protocol_error(app):
    """``app.delete`` on an unregistered Table raises ProtocolError."""

    @tl.table(key="user_id")
    class AlsoNotRegistered:
        user_id: str
        x: int

    with pytest.raises(ProtocolError) as ei:
        app.delete(AlsoNotRegistered, "u1")
    assert "unknown table" in str(ei.value).lower()


def test_push_table_bad_arity_type_error(app):
    """3-arg Stream or 2-arg Table raises TypeError before any wire I/O."""

    @tl.stream
    class E2EBadArityStream:
        user_id: str

    @tl.table(key="user_id")
    class E2EBadArityTable:
        user_id: str
        x: int

    app.register(E2EBadArityStream, E2EBadArityTable)

    # Stream form with 2 extra args -> TypeError.
    with pytest.raises(TypeError):
        app.push(E2EBadArityStream, "u1", {"x": 1})

    # Table form with only 1 extra arg -> TypeError.
    with pytest.raises(TypeError):
        app.push(E2EBadArityTable, {"x": 1})

    # Table form with non-dict fields -> TypeError.
    with pytest.raises(TypeError):
        app.push(E2EBadArityTable, "u1", "not a dict")
