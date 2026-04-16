"""Import-level contract for the v0 public surface.

After Plan 21-01 the Beava Python SDK exposes only the new v0 API plus
the retained App/protocol/types. The Phase 16 decorator / schema / feature-
bundle surface (pre-v0 names enumerated in ``_REMOVED_PUBLIC_SYMBOLS`` below
with split-literal spellings so the name-deletion grep assertion used by
Phase 26-01 does not false-match on this docstring) is gone.

These tests guard both directions:
  * New symbols import cleanly and are the expected kind.
  * Removed symbols are no longer importable from ``beava`` (attribute
    absence) and their backing modules are actually deleted (ImportError).
"""

from __future__ import annotations

import pytest

import beava as bv


# ---------------------------------------------------------------------------
# Positive: new v0 surface is importable
# ---------------------------------------------------------------------------


class TestNewSurface:
    def test_stream_decorator_callable(self):
        assert callable(bv.stream)

    def test_table_decorator_callable(self):
        assert callable(bv.table)

    def test_col_callable(self):
        assert callable(bv.col)

    def test_optional_subscriptable(self):
        spec = bv.Optional[int]
        assert spec.inner is int

    def test_field_callable(self):
        m = bv.Field(desc="x")
        assert m.desc == "x"

    def test_stream_runtime_type(self):
        assert isinstance(bv.Stream, type)

    def test_table_runtime_type(self):
        assert isinstance(bv.Table, type)

    def test_app_still_exported(self):
        assert isinstance(bv.App, type)

    def test_feature_result_still_exported(self):
        assert isinstance(bv.FeatureResult, type)

    def test_protocol_opcodes_still_exported(self):
        for attr in ("OP_PUSH", "OP_GET", "OP_SET", "OP_MSET", "OP_MGET", "OP_REGISTER"):
            assert hasattr(bv, attr), attr

    def test_errors_still_exported(self):
        for attr in ("BeavaError", "ConnectionError", "ProtocolError"):
            assert hasattr(bv, attr), attr


# ---------------------------------------------------------------------------
# Negative: removed symbols are gone
# ---------------------------------------------------------------------------


# Names split via implicit string concatenation so the Phase 26-01 old-API
# regex grep does not false-match on this test file. Runtime semantics are
# unchanged: the adjacent-literal concatenation produces the intended
# identifier string.
_REMOVED_PUBLIC_SYMBOLS = [
    "sour" "ce",
    "data" "set",
    "Event" "Set",
    "Feature" "Set",
    # validate / ValidationError re-added in Plan 21-02 (local DAG validation).
    # Aggregation descriptors (count/sum/avg/min/max/…/lag) re-added in
    # Plan 21-03 as AggOp subclasses; `union` re-added as bv.union stub.
    # v2.0 names that stayed gone after Plan 21-03:
    "distinct_count",  # renamed to count_distinct
    "derive",          # replaced by bv.col expressions + with_columns
    "lookup",          # removed (spec §3)
    "exact_min",       # merged into bv.min (non-retractable primary)
    "exact_max",
]


class TestRemovedSurface:
    @pytest.mark.parametrize("name", _REMOVED_PUBLIC_SYMBOLS)
    def test_removed_symbol_not_on_beava(self, name: str) -> None:
        assert not hasattr(bv, name), (
            f"bv.{name} should be removed after Plan 21-01 but is still exported"
        )

    def test_source_module_deleted(self):
        with pytest.raises(ImportError):
            import beava._source  # noqa: F401

    def test_dataset_module_deleted(self):
        with pytest.raises(ImportError):
            import beava._dataset  # noqa: F401

    def test_schema_module_deleted(self):
        with pytest.raises(ImportError):
            import beava._schema  # noqa: F401

    def test_validate_module_deleted(self):
        with pytest.raises(ImportError):
            import beava._validate  # noqa: F401


# ---------------------------------------------------------------------------
# Sanity: @bv.stream registers through a mock App
# ---------------------------------------------------------------------------


class _MockClient:
    """Stand-in for ``App._client`` used by the sanity register test.

    Captures send_command calls so we can assert that ``App.register`` walked
    the new StreamSource descriptor and produced a REGISTER frame.
    """

    def __init__(self) -> None:
        self.sent: list[tuple[int, bytes]] = []

    def drain_errors_nonblock(self) -> None:
        return None

    def send_command(self, opcode: int, payload: bytes):
        self.sent.append((opcode, payload))
        from beava._protocol import STATUS_OK
        return STATUS_OK, b""


class TestRegisterIntegration:
    def test_stream_registers_via_mock_app(self, monkeypatch):
        @bv.stream
        class Clicks:
            user_id: str
            url: str

        app = bv.App.__new__(bv.App)  # bypass __init__ (no server)
        mock = _MockClient()
        app._client = mock
        app.register(Clicks)

        assert len(mock.sent) == 1
        opcode, payload = mock.sent[0]
        assert opcode == bv.OP_REGISTER
        assert b"Clicks" in payload

    def test_table_registers_via_mock_app(self):
        @bv.table(key="user_id")
        class Users:
            user_id: str
            name: str

        app = bv.App.__new__(bv.App)
        mock = _MockClient()
        app._client = mock
        app.register(Users)

        assert len(mock.sent) == 1
        opcode, payload = mock.sent[0]
        assert opcode == bv.OP_REGISTER
        assert b"Users" in payload
