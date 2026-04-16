"""Phase 25-01 Task 3 — Python SDK end-to-end tests for ``app.get_multi``.

Runs against the session-scoped ``beava_server`` fixture in conftest.py.
"""

from __future__ import annotations

import pytest

import beava as bv
from beava._protocol import (
    GET_MULTI_MAX_TABLES,
    OP_GET_MULTI,
    encode_get_multi,
    encode_string,
)
from beava._types import ProtocolError


# ---------------------------------------------------------------------------
# Wire-format unit tests (no server needed)
# ---------------------------------------------------------------------------


def test_encode_get_multi_wire_format():
    """``encode_get_multi`` emits ``[u16 count][u16 name][u16 name][u16 key]``."""
    out = encode_get_multi(["A", "BC"], "u1")
    # [u16 count=2]
    assert out[:2] == b"\x00\x02"
    # [u16 name_len=1]["A"]
    assert out[2:4] == b"\x00\x01"
    assert out[4:5] == b"A"
    # [u16 name_len=2]["BC"]
    assert out[5:7] == b"\x00\x02"
    assert out[7:9] == b"BC"
    # [u16 key_len=2]["u1"]
    assert out[9:11] == b"\x00\x02"
    assert out[11:13] == b"u1"
    assert len(out) == 13


def test_encode_get_multi_rejects_empty_list():
    with pytest.raises(ProtocolError, match="at least one"):
        encode_get_multi([], "u1")


def test_encode_get_multi_rejects_oversized_count():
    names = [f"T{i}" for i in range(GET_MULTI_MAX_TABLES + 1)]
    with pytest.raises(ProtocolError, match="exceeds 256"):
        encode_get_multi(names, "u1")


def test_op_get_multi_constant_is_0x0d():
    assert OP_GET_MULTI == 0x0D


def test_encode_get_multi_matches_multiple_prefixed_strings():
    """Layout equivalence: name blocks == encode_string(name) concatenated."""
    out = encode_get_multi(["UserProfile", "RiskScore"], "u1")
    expected_tail = (
        encode_string("UserProfile") + encode_string("RiskScore") + encode_string("u1")
    )
    assert out[2:] == expected_tail


# ---------------------------------------------------------------------------
# End-to-end tests against the fixture server.
# ---------------------------------------------------------------------------


def test_get_multi_three_tables(app):
    """Push to three Tables; get_multi returns a FeatureResult per table."""

    @bv.table(key="user_id")
    class GM3Profile:
        user_id: str
        country: str

    @bv.table(key="user_id")
    class GM3Risk:
        user_id: str
        score: int

    @bv.table(key="user_id")
    class GM3Sub:
        user_id: str
        plan: str

    app.register(GM3Profile, GM3Risk, GM3Sub)

    app.push(GM3Profile, "gm_u1", {"country": "US"})
    app.push(GM3Risk, "gm_u1", {"score": 42})
    app.push(GM3Sub, "gm_u1", {"plan": "gold"})

    result = app.get_multi([GM3Profile, GM3Risk, GM3Sub], "gm_u1")

    # Keyed by the ORIGINAL Table class objects.
    assert set(result.keys()) == {GM3Profile, GM3Risk, GM3Sub}
    assert result[GM3Profile] is not None
    assert result[GM3Profile].to_dict() == {"country": "US"}
    assert result[GM3Risk].to_dict() == {"score": 42}
    assert result[GM3Sub].to_dict() == {"plan": "gold"}


def test_get_multi_null_collapse(app):
    """Table never pushed for this key collapses to ``None`` — not raise."""

    @bv.table(key="user_id")
    class GMNCAlpha:
        user_id: str
        a: int

    @bv.table(key="user_id")
    class GMNCBeta:
        user_id: str
        b: int

    @bv.table(key="user_id")
    class GMNCGamma:
        user_id: str
        g: int

    app.register(GMNCAlpha, GMNCBeta, GMNCGamma)

    app.push(GMNCAlpha, "gm_nc_u1", {"a": 1})
    app.push(GMNCBeta, "gm_nc_u1", {"b": 2})
    # GMNCGamma never pushed for this key.

    result = app.get_multi([GMNCAlpha, GMNCBeta, GMNCGamma], "gm_nc_u1")
    assert result[GMNCAlpha].to_dict() == {"a": 1}
    assert result[GMNCBeta].to_dict() == {"b": 2}
    assert result[GMNCGamma] is None, "never-pushed table must be None"


def test_get_multi_after_delete(app):
    """Tombstoned row collapses to ``None`` in the response."""

    @bv.table(key="user_id")
    class GMADProfile:
        user_id: str
        country: str

    @bv.table(key="user_id")
    class GMADRisk:
        user_id: str
        score: int

    app.register(GMADProfile, GMADRisk)

    app.push(GMADProfile, "gm_del_u1", {"country": "DE"})
    app.push(GMADRisk, "gm_del_u1", {"score": 5})
    app.delete(GMADProfile, "gm_del_u1")

    result = app.get_multi([GMADProfile, GMADRisk], "gm_del_u1")
    assert (
        result[GMADProfile] is None
    ), "tombstoned row must collapse to None"
    assert result[GMADRisk].to_dict() == {"score": 5}


def test_get_multi_empty_list_rejects(app):
    with pytest.raises(ValueError, match="at least one"):
        app.get_multi([], "u1")


def test_get_multi_non_table_rejects(app):
    """Passing a Stream or arbitrary object raises TypeError before wire I/O."""

    @bv.stream
    class GMNTClicks:
        user_id: str
        page: str

    app.register(GMNTClicks)

    with pytest.raises(TypeError, match="Table descriptors"):
        app.get_multi([GMNTClicks], "u1")

    class NotATable:
        pass

    with pytest.raises(TypeError, match="Table descriptors"):
        app.get_multi([NotATable], "u1")


def test_get_multi_unknown_table_surfaces_server_error(app):
    """Unregistered table name → ProtocolError surfaced from server STATUS_ERROR."""

    @bv.table(key="user_id")
    class GMURegistered:
        user_id: str
        x: int

    # Do NOT register a sibling; build one only for the client call so it
    # has a _beava_stream_name the server has never seen.
    @bv.table(key="user_id")
    class GMUGhost:
        user_id: str
        x: int

    app.register(GMURegistered)
    # Note: GMUGhost intentionally NOT registered.

    with pytest.raises(ProtocolError, match="unknown table"):
        app.get_multi([GMURegistered, GMUGhost], "u1")


def test_get_multi_composite_key(app):
    """Dict-form key is \\x1f-joined; server sees the flat string."""

    @bv.table(key="user_id")
    class GMCKProfile:
        user_id: str
        country: str

    app.register(GMCKProfile)

    # Push with the joined string representation — the server only sees a
    # single flat entity key string. We verify the SDK accepts a dict and
    # produces the same effective wire key.
    joined = "tenant_7\x1fu1"
    app.push(GMCKProfile, joined, {"country": "CA"})

    # get_multi with the dict form must return the same row.
    result = app.get_multi(
        [GMCKProfile], {"tenant_id": "tenant_7", "user_id": "u1"}
    )
    assert result[GMCKProfile] is not None, (
        "composite key via dict must resolve to the same row as the flat string"
    )
    assert result[GMCKProfile].to_dict() == {"country": "CA"}
