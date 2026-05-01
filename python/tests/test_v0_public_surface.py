"""Import-level contract for the v0 public surface.

After Plan 21-01 the Tally Python SDK exposes only the new v0 API plus
the retained App/protocol/types. The Phase 16 decorator / schema / feature-
bundle surface (pre-v0 names enumerated in ``_REMOVED_PUBLIC_SYMBOLS`` below
with split-literal spellings so the name-deletion grep assertion used by
Phase 26-01 does not false-match on this docstring) is gone.

These tests guard both directions:
  * New symbols import cleanly and are the expected kind.
  * Removed symbols are no longer importable from ``tally`` (attribute
    absence) and their backing modules are actually deleted (ImportError).
"""

from __future__ import annotations

import pytest

import tally as tl


# ---------------------------------------------------------------------------
# Positive: new v0 surface is importable
# ---------------------------------------------------------------------------


class TestNewSurface:
    def test_stream_decorator_callable(self):
        assert callable(tl.stream)

    # Plan 12.7-06: tl.table assertions removed per project_v0_events_only_scope
    # (v0 ships events-only). The `tally` SDK's table surface is out of v0
    # scope; if the tally package surfaces tl.table after this plan, that's
    # a separate audit (tally is a parallel/historical SDK, not the beava
    # public surface).

    def test_col_callable(self):
        assert callable(tl.col)

    def test_optional_subscriptable(self):
        spec = tl.Optional[int]
        assert spec.inner is int

    def test_field_callable(self):
        m = tl.Field(desc="x")
        assert m.desc == "x"

    def test_stream_runtime_type(self):
        assert isinstance(tl.Stream, type)

    # Plan 12.7-06: tl.Table assertion removed per project_v0_events_only_scope.

    def test_app_still_exported(self):
        assert isinstance(tl.App, type)

    def test_feature_result_still_exported(self):
        assert isinstance(tl.FeatureResult, type)

    def test_protocol_opcodes_still_exported(self):
        for attr in ("OP_PUSH", "OP_GET", "OP_SET", "OP_MSET", "OP_MGET", "OP_REGISTER"):
            assert hasattr(tl, attr), attr

    def test_errors_still_exported(self):
        for attr in ("TallyError", "ConnectionError", "ProtocolError"):
            assert hasattr(tl, attr), attr


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
    # Plan 21-03 as AggOp subclasses; `union` re-added as tl.union stub.
    # v2.0 names that stayed gone after Plan 21-03:
    "distinct_count",  # renamed to count_distinct
    "derive",          # replaced by tl.col expressions + with_columns
    "lookup",          # removed (spec §3)
    "exact_min",       # merged into tl.min (non-retractable primary)
    "exact_max",
]


class TestRemovedSurface:
    @pytest.mark.parametrize("name", _REMOVED_PUBLIC_SYMBOLS)
    def test_removed_symbol_not_on_tally(self, name: str) -> None:
        assert not hasattr(tl, name), (
            f"tl.{name} should be removed after Plan 21-01 but is still exported"
        )

    def test_source_module_deleted(self):
        with pytest.raises(ImportError):
            import tally._source  # noqa: F401

    def test_dataset_module_deleted(self):
        with pytest.raises(ImportError):
            import tally._dataset  # noqa: F401

    def test_schema_module_deleted(self):
        with pytest.raises(ImportError):
            import tally._schema  # noqa: F401

    def test_validate_module_deleted(self):
        with pytest.raises(ImportError):
            import tally._validate  # noqa: F401


# ---------------------------------------------------------------------------
# Sanity: @tl.stream registers through a mock App
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
        from tally._protocol import STATUS_OK
        return STATUS_OK, b""


class TestRegisterIntegration:
    def test_stream_registers_via_mock_app(self, monkeypatch):
        @tl.stream
        class Clicks:
            user_id: str
            url: str

        app = tl.App.__new__(tl.App)  # bypass __init__ (no server)
        mock = _MockClient()
        app._client = mock
        app.register(Clicks)

        assert len(mock.sent) == 1
        opcode, payload = mock.sent[0]
        assert opcode == tl.OP_REGISTER
        assert b"Clicks" in payload

    # Plan 12.7-06: test_table_registers_via_mock_app removed per
    # project_v0_events_only_scope (v0 ships events-only via @bv.event /
    # @tl.stream). Table register tests return in v0.1+ if tables revive.
