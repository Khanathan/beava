"""Tests for python/beava/_validate.py — DAG topo-sort, cycle detection, local validation.

All tests in this file are pure-Python (no server needed).  RED commit: all fail because
_validate.py does not exist yet.
"""

from __future__ import annotations

import beava as bv
from beava._errors import ValidationError
from beava._events import EventDerivation, EventSource
from beava._schema import FieldSpec
from beava._tables import TableSource

# ---------------------------------------------------------------------------
# Helpers: build minimal descriptors without using the decorators
# ---------------------------------------------------------------------------


def _make_event(name: str, upstreams: list[str] | None = None) -> EventSource:
    """Return a minimal EventSource with a single str field.

    Post-Plan-12.6-08: EventSource no longer accepts event_time_field /
    tolerate_delay_ms parameters per the no-event-time pivot.
    """
    src = EventSource(
        name=name,
        schema={"x": FieldSpec(name="x", py_type=str, optional=False)},
        dedupe_key=None,
        dedupe_window_ms=None,
        keep_events_for_ms=None,
    )
    if upstreams is not None:
        src._upstreams = upstreams  # type: ignore[assignment]
    return src


def _make_derivation(name: str, upstreams: list[str]) -> EventDerivation:
    """Return a minimal EventDerivation with given upstreams."""
    return EventDerivation(
        name=name,
        schema={"x": FieldSpec(name="x", py_type=str, optional=False)},
        upstreams=upstreams,
        ops=[],
        output_kind="event",
    )


def _make_table(name: str, key: str = "id") -> TableSource:
    """Return a minimal TableSource."""
    return TableSource(
        name=name,
        schema={key: FieldSpec(name=key, py_type=str, optional=False)},
        primary_key=[key],
        ttl_ms=None,
        mode="upsert",
    )


# ---------------------------------------------------------------------------
# topo_sort tests
# ---------------------------------------------------------------------------


def test_topo_sort_simple() -> None:
    """Sources come before derivations in the sorted result."""
    from beava._validate import topo_sort

    src1 = _make_event("Src1")
    src2 = _make_event("Src2")
    derived = _make_derivation("Derived", upstreams=["Src1", "Src2"])

    result = topo_sort([derived, src1, src2])
    names = [d._name for d in result]
    assert names.index("Src1") < names.index("Derived")
    assert names.index("Src2") < names.index("Derived")


def test_topo_sort_preserves_input_order_for_independents() -> None:
    """Three independent sources keep their relative input order."""
    from beava._validate import topo_sort

    a = _make_event("A")
    b = _make_event("B")
    c = _make_event("C")

    result = topo_sort([a, b, c])
    assert [d._name for d in result] == ["A", "B", "C"]


# ---------------------------------------------------------------------------
# Cycle detection tests
# ---------------------------------------------------------------------------


def test_detect_cycle_direct() -> None:
    """A → B, B → A: validate returns a ValidationError(kind='cycle', ...)."""
    from beava._validate import validate_descriptors

    a = _make_derivation("A", upstreams=["B"])
    b = _make_derivation("B", upstreams=["A"])

    errs = validate_descriptors([a, b])
    cycle_errs = [e for e in errs if e.kind == "cycle"]
    assert cycle_errs, f"Expected cycle error, got: {errs}"


def test_detect_cycle_three_node() -> None:
    """A → B → C → A: validate returns a ValidationError whose path mentions A, B, C."""
    from beava._validate import validate_descriptors

    a = _make_derivation("A", upstreams=["C"])
    b = _make_derivation("B", upstreams=["A"])
    c = _make_derivation("C", upstreams=["B"])

    errs = validate_descriptors([a, b, c])
    cycle_errs = [e for e in errs if e.kind == "cycle"]
    assert cycle_errs, f"Expected cycle error, got: {errs}"
    path = cycle_errs[0].path
    assert "A" in path and "B" in path and "C" in path, (
        f"Expected A, B, C in cycle path but got: {path!r}"
    )


# ---------------------------------------------------------------------------
# Duplicate name detection
# ---------------------------------------------------------------------------


def test_duplicate_name_detected() -> None:
    """Two descriptors with the same _name → ValidationError(kind='duplicate_name')."""
    from beava._validate import validate_descriptors

    # Use the decorator to get a real descriptor; then create a second one with same name
    @bv.event
    class A:  # type: ignore[no-redef]
        x: int

    a2 = _make_event("A")

    errs = validate_descriptors([A, a2])
    dup_errs = [e for e in errs if e.kind == "duplicate_name"]
    assert dup_errs, f"Expected duplicate_name error, got: {errs}"
    assert dup_errs[0].path == "A"


# ---------------------------------------------------------------------------
# Missing upstream detection
# ---------------------------------------------------------------------------


def test_missing_upstream_detected() -> None:
    """A derivation referencing an unknown upstream → ValidationError(kind='missing_upstream')."""
    from beava._validate import validate_descriptors

    derived = _make_derivation("MyDeriv", upstreams=["Nonexistent"])

    errs = validate_descriptors([derived])
    missing_errs = [e for e in errs if e.kind == "missing_upstream"]
    assert missing_errs, f"Expected missing_upstream error, got: {errs}"
    assert "Nonexistent" in missing_errs[0].message


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_validate_succeeds_for_valid_descriptors() -> None:
    """One event + one table with no issues → empty error list."""
    from beava._validate import validate_descriptors

    @bv.event
    class Transaction:  # type: ignore[no-redef]
        amount: float
        user_id: str

    @bv.table(key="user_id")
    class UserProfile:  # type: ignore[no-redef]
        user_id: str
        balance: float

    errs = validate_descriptors([Transaction, UserProfile])
    assert errs == [], f"Expected no errors, got: {errs}"


# ---------------------------------------------------------------------------
# Fail-soft: multiple errors collected
# ---------------------------------------------------------------------------


def test_validate_collects_multiple_errors() -> None:
    """Two duplicate descriptors: error list has >= 2 ValidationError entries (fail-soft)."""
    from beava._validate import validate_descriptors

    # duplicate_name: two A's
    a1 = _make_event("A")
    a2 = _make_event("A")
    # cycle: C->D->C
    c = _make_derivation("C", upstreams=["D"])
    d = _make_derivation("D", upstreams=["C"])

    errs = validate_descriptors([a1, a2, c, d])
    assert len(errs) >= 2, f"Expected >= 2 errors, got: {errs}"
    kinds = {e.kind for e in errs}
    assert "duplicate_name" in kinds or "cycle" in kinds


# ---------------------------------------------------------------------------
# validate_descriptors returns list[ValidationError] (not exceptions)
# ---------------------------------------------------------------------------


def test_validate_returns_validation_error_instances() -> None:
    """Each error in the returned list is a ValidationError instance."""
    from beava._validate import validate_descriptors

    a = _make_derivation("X", upstreams=["Unknown"])
    errs = validate_descriptors([a])
    assert all(isinstance(e, ValidationError) for e in errs)
    # Plan 12.6-08: event_time_field_invalid removed per no-event-time pivot.
    assert errs[0].kind in {
        "cycle",
        "missing_upstream",
        "duplicate_name",
        "unknown_field_type",
        "table_key_invalid",
        "bad_return_type",
        "schema_mismatch",
        "registration_conflict",
    }
