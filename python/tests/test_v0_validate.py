"""Tests for local validation + App.register / App.validate wiring."""

from __future__ import annotations

import pytest

import tally as tl
from tally._col import col
from tally._stream import Stream, stream
from tally._table import Table, table
from tally._validate_v0 import ValidationError, validate


# ---------------------------------------------------------------------------
# validate() happy path
# ---------------------------------------------------------------------------


class TestValidateHappyPath:
    def test_valid_pipeline_returns_empty_list(self):
        @stream
        class Clicks:
            user_id: str
            page: str

        @stream
        def Checkouts(clicks: Clicks) -> Stream:
            return clicks.filter(col("page") == "/checkout")

        errs = validate(Clicks, Checkouts)
        assert errs == []

    def test_only_source_is_valid(self):
        @stream
        class Clicks:
            user_id: str

        assert validate(Clicks) == []


# ---------------------------------------------------------------------------
# Missing upstream → ValidationError(kind="missing_dep")
# ---------------------------------------------------------------------------


class TestMissingUpstream:
    def test_missing_upstream_produces_missing_dep_error(self):
        @stream
        class Clicks:
            user_id: str
            page: str

        @stream
        def Checkouts(clicks: Clicks) -> Stream:
            return clicks.filter(col("page") == "/checkout")

        errs = validate(Checkouts)  # Clicks missing
        assert len(errs) == 1
        assert errs[0].kind == "missing_dep"
        assert "Clicks" in errs[0].message


# ---------------------------------------------------------------------------
# Cycle → ValidationError(kind="cycle")
# ---------------------------------------------------------------------------


class TestCycleValidation:
    def test_cycle_produces_cycle_error(self):
        @stream
        class Seed:
            x: int

        @stream
        def A(s: Seed) -> Stream:
            return s

        @stream
        def B(s: Seed) -> Stream:
            return s

        A._upstreams = [B]
        B._upstreams = [A]

        errs = validate(A, B)
        assert len(errs) >= 1
        assert errs[0].kind == "cycle"
        assert "→" in errs[0].message


# ---------------------------------------------------------------------------
# Schema mismatch inside a derivation → ValidationError(kind="schema_mismatch")
# ---------------------------------------------------------------------------


class TestSchemaMismatchValidation:
    def test_bad_field_reference_caught_if_somehow_bypassed(self):
        """Normal construction catches bad refs eagerly; simulate a bypassed
        op by injecting one directly and running validate()."""
        @stream
        class Clicks:
            user_id: str
            page: str

        @stream
        def Checkouts(clicks: Clicks) -> Stream:
            return clicks.filter(col("page") == "/checkout")

        # Inject a fabricated op referencing a non-existent field.
        Checkouts._ops.append({"op": "filter", "expr": "(bogus > 1)"})

        errs = validate(Clicks, Checkouts)
        assert any(e.kind == "schema_mismatch" for e in errs)
        mm = [e for e in errs if e.kind == "schema_mismatch"][0]
        assert "bogus" in mm.message


# ---------------------------------------------------------------------------
# ValidationError shape
# ---------------------------------------------------------------------------


class TestValidationErrorShape:
    def test_has_kind_path_message(self):
        @stream
        class Clicks:
            user_id: str

        @stream
        def B(c: Clicks) -> Stream:
            return c

        errs = validate(B)
        e = errs[0]
        assert hasattr(e, "kind")
        assert hasattr(e, "path")
        assert hasattr(e, "message")
        assert "[" in str(e) and "]" in str(e)

    def test_validation_error_exported(self):
        assert tl.ValidationError is ValidationError

    def test_validate_exported(self):
        assert tl.validate is validate


# ---------------------------------------------------------------------------
# App.validate / App.register wiring
# ---------------------------------------------------------------------------


class _MockClient:
    def __init__(self) -> None:
        self.sent: list[tuple[int, bytes]] = []

    def drain_errors_nonblock(self) -> None:
        return None

    def send_command(self, opcode: int, payload: bytes):
        from tally._protocol import STATUS_OK
        self.sent.append((opcode, payload))
        return STATUS_OK, b""


def _mock_app() -> tuple[tl.App, _MockClient]:
    app = tl.App.__new__(tl.App)
    mock = _MockClient()
    app._client = mock
    return app, mock


class TestAppValidateAndRegister:
    def test_app_validate_returns_list_no_tcp(self):
        @stream
        class Clicks:
            user_id: str

        app, mock = _mock_app()
        errs = app.validate(Clicks)
        assert errs == []
        assert mock.sent == []

    def test_app_register_rejects_invalid_dag(self):
        @stream
        class Clicks:
            user_id: str

        @stream
        def B(c: Clicks) -> Stream:
            return c

        app, mock = _mock_app()
        # Clicks omitted — MissingDependency
        with pytest.raises(ValidationError):
            app.register(B)
        assert mock.sent == []  # nothing sent

    def test_app_register_sends_in_topological_order(self):
        @stream
        class Clicks:
            user_id: str
            page: str

        @stream
        def Checkouts(clicks: Clicks) -> Stream:
            return clicks.filter(col("page") == "/checkout")

        app, mock = _mock_app()
        app.register(Checkouts, Clicks)  # reversed input order
        # Clicks frame must precede Checkouts frame.
        payloads = [p for _, p in mock.sent]
        clicks_idx = next(i for i, p in enumerate(payloads) if b"Clicks" in p and b"Checkouts" not in p)
        checkouts_idx = next(i for i, p in enumerate(payloads) if b"Checkouts" in p)
        assert clicks_idx < checkouts_idx

    def test_app_register_dedupes_shared_upstream(self):
        @stream
        class Clicks:
            user_id: str
            page: str

        @stream
        def A(clicks: Clicks) -> Stream:
            return clicks.filter(col("page") == "/checkout")

        @stream
        def B(clicks: Clicks) -> Stream:
            return clicks.filter(col("page") == "/cart")

        app, mock = _mock_app()
        app.register(Clicks, A, B)
        # Clicks must be registered exactly once.
        clicks_frames = [
            p for _, p in mock.sent if b"Clicks" in p and b"Checkouts" not in p
        ]
        # Each frame containing b"Clicks" as its name (the source's REGISTER
        # JSON). Count by parsing.
        import json
        names = []
        for opcode, payload in mock.sent:
            # encode_register wraps JSON in a length-prefixed body; parse the
            # JSON suffix loosely by finding the first '{'.
            try:
                idx = payload.index(b"{")
                names.append(json.loads(payload[idx:].decode()).get("name"))
            except (ValueError, json.JSONDecodeError):
                pass
        assert names.count("Clicks") == 1
        assert "A" in names and "B" in names
