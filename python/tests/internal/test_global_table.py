"""Phase 13.5 Plan 03 red+green: @bv.table keyed + global per ADR-001 + ADR-003.

Plan 04 ships the 53 op helpers (bv.count, bv.sum, etc.). Plan 03's internal
tests stand alone — they substitute a ``_FakeAgg`` descriptor whose
``to_dict()`` mirrors what Plan 04 will emit. The shape contract checked
here is the @bv.table decorator's three call shapes (keyed-str /
keyed-list / global) per ADR-001 + ADR-003.
"""
from __future__ import annotations

from typing import Any

import beava as bv


class _FakeAgg:
    """Stand-in for Plan 04 AggDescriptor. Exposes a ``to_dict`` for serialization."""

    def __init__(self, op: str, **kwargs: Any) -> None:
        self.op = op
        self.kwargs = kwargs

    def to_dict(self) -> dict[str, Any]:
        return {"op": self.op, **self.kwargs}


def _count(window: str) -> _FakeAgg:
    return _FakeAgg("count", window=window)


@bv.event
class _Click:
    user_id: str
    page: str


def test_table_with_key_str() -> None:
    @bv.table(key="user_id")
    def UserClicks(click: _Click) -> Any:
        return click.group_by("user_id").agg(c=_count(window="1h"))

    assert UserClicks._name == "UserClicks"
    assert UserClicks._key_cols == ["user_id"]
    assert UserClicks._kind == "table"


def test_table_with_composite_key_list() -> None:
    @bv.table(key=["user_id", "page"])
    def UserPageClicks(click: _Click) -> Any:
        return click.group_by("user_id", "page").agg(c=_count(window="1h"))

    assert UserPageClicks._key_cols == ["user_id", "page"]


def test_table_no_key_is_global() -> None:
    """ADR-003: @bv.table without key= → global table (key_cols=[])."""

    @bv.table
    def TotalClicks(click: _Click) -> Any:
        return click.agg(total=_count(window="forever"))

    assert TotalClicks._name == "TotalClicks"
    assert TotalClicks._key_cols == []
    assert TotalClicks._kind == "table"


def test_table_three_equivalent_global_forms() -> None:
    """All three forms compile to key_cols=[]."""

    @bv.table
    def G1(click: _Click) -> Any:
        return click.agg(total=_count(window="forever"))

    @bv.table
    def G2(click: _Click) -> Any:
        return click.group_by().agg(total=_count(window="forever"))

    @bv.table()
    def G3(click: _Click) -> Any:
        return click.group_by().agg(total=_count(window="forever"))

    assert G1._key_cols == []
    assert G2._key_cols == []
    assert G3._key_cols == []


def test_table_chain_carries_aggregation_step() -> None:
    """The decorated table's chain ends in an ``agg`` step with serialized aggs."""

    @bv.table(key="user_id")
    def UC(click: _Click) -> Any:
        return click.group_by("user_id").agg(n=_count(window="1h"))

    assert UC._chain[-1]["op"] == "agg"
    assert UC._chain[-1]["keys"] == ["user_id"]
    assert UC._chain[-1]["aggs"]["n"]["op"] == "count"
    assert UC._chain[-1]["aggs"]["n"]["window"] == "1h"


def test_table_function_must_return_chain() -> None:
    """@bv.table function body must return an EventDerivation (not e.g. a dict)."""

    import pytest

    with pytest.raises(TypeError, match="must return"):
        @bv.table(key="user_id")
        def Bad(click: _Click) -> Any:
            return {"not": "a chain"}
