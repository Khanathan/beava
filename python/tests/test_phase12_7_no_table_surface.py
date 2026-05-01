"""Phase 12.7 Plan 06 — RED-then-GREEN test for events-only Python public surface.

Per `project_v0_events_only_scope` (locked 2026-04-30) v0 Beava ships
events-only. The Python SDK MUST drop:

- `bv.table` decorator (re-exported from `beava._tables`)
- `"table"` token in `beava.__all__`
- `App.upsert(...)` method
- `App.delete(...)` method (table-tombstone)
- `App.retract(...)` method (was never present; sanity-confirm absence)
- `GroupBy.agg(...)` returning a `TableDerivation` — instead raises
  `RuntimeError` with v0 framing (D-02: "not supported in v0", forward-
  looking; NOT "removed" / retrospective).
- `beava._tables` submodule entirely

These 7 tests are RED at HEAD because Plan 06 hasn't deleted/rewired the
surface yet. After Plan 06's GREEN commit lands, all 7 turn GREEN. Plan
12.7-09 (closure) anchors the events-only invariant in CI for good via
the architectural-test pair (`phase12_7_no_table_surface.rs` +
`phase12_7_legacy_table_handlers_killed.rs`); this Python-side test is
the SDK-level mirror.
"""

from __future__ import annotations

import pytest


def test_bv_table_attribute_does_not_exist() -> None:
    """`bv.table` must raise AttributeError after Plan 06.

    D-02 framing: natural Python `AttributeError` from re-export deletion;
    no explicit deny stub at the namespace level.
    """
    import beava as bv

    with pytest.raises(AttributeError):
        bv.table  # type: ignore[attr-defined]  # noqa: B018


def test_bv_all_does_not_contain_table() -> None:
    """`"table"` must not be in `beava.__all__` after Plan 06."""
    import beava as bv

    assert "table" not in bv.__all__, (
        f"`'table'` survived in beava.__all__ post-Plan-12.7-06: "
        f"{sorted(bv.__all__)}"
    )


def test_app_upsert_method_does_not_exist() -> None:
    """`app.upsert(...)` must raise AttributeError after Plan 06.

    D-02 framing: natural Python `AttributeError` from method deletion;
    no no-op stub.
    """
    import beava as bv

    app = bv.App("http://localhost:9999")  # any URL; only checking attribute access
    try:
        with pytest.raises(AttributeError):
            app.upsert  # type: ignore[attr-defined]  # noqa: B018
    finally:
        app.close()


def test_app_delete_method_does_not_exist() -> None:
    """`app.delete(...)` must raise AttributeError after Plan 06."""
    import beava as bv

    app = bv.App("http://localhost:9999")
    try:
        with pytest.raises(AttributeError):
            app.delete  # type: ignore[attr-defined]  # noqa: B018
    finally:
        app.close()


def test_app_retract_method_does_not_exist() -> None:
    """`app.retract(...)` must raise AttributeError.

    Sanity-positive: was never a public method on the beava SDK (CONTEXT.md
    references it from the temporal-MVCC era; the actual code only had
    `upsert`/`delete`). This test confirms it stays absent post-Plan-06.
    """
    import beava as bv

    app = bv.App("http://localhost:9999")
    try:
        with pytest.raises(AttributeError):
            app.retract  # type: ignore[attr-defined]  # noqa: B018
    finally:
        app.close()


def test_groupby_agg_raises_v0_error() -> None:
    """`GroupBy.agg(...)` must raise `RuntimeError` with v0 framing.

    D-04 (Plan 06 interpretation): keep `.agg()` as a method-shaped stub on
    `GroupBy` so legacy import paths don't break, but always-error semantics
    at call time. The v0 message must contain both "not supported in v0" AND
    "events-only" tokens (D-02 framing — forward-looking, NOT "removed").
    """
    import beava as bv

    @bv.event
    class Tx:
        user_id: str
        amount: float

    with pytest.raises(RuntimeError) as exc_info:
        Tx.group_by("user_id").agg(c=bv.count(window="5m"))

    msg = str(exc_info.value)
    assert "not supported in v0" in msg, f"v0 framing missing: {msg!r}"
    assert "events-only" in msg, f"events-only framing missing: {msg!r}"


def test_tables_module_does_not_exist() -> None:
    """`beava._tables` submodule must be deleted on disk after Plan 06.

    Importing it raises `ModuleNotFoundError` natively; no explicit deny
    stub, no `__getattr__` shim. The module file is gone from disk.
    """
    with pytest.raises(ModuleNotFoundError):
        from beava._tables import TableDerivation  # type: ignore[import-not-found]  # noqa: F401
