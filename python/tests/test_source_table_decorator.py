"""Phase 55 Wave 0 RED — @bv.source_table decorator contract (SC-2 / TPC-SOURCE-01).

Tests are pytest.mark.skip'd pending Wave 2 (plan 55-02 Task 3) which lands the
`SourceTable` subclass + `source_table` decorator alongside the existing
`@bv.table` / `@bv.stream` decorators in python/beava/_table.py.

Contract:
  - @bv.source_table(key=K) registers a passive keyed enrichment table.
  - key= is required — TypeError("... requires key") on omission.
  - .group_by(...) is rejected at register time — RuntimeError matching
    "passive enrichment" (source tables do not fire cascade in Phase 55,
    so group_by is nonsensical — D-B6).
  - Registration path generates the same wire call as @bv.table but with
    _beava_kind == "source_table".

Wave 2 wiring plan:
  python/beava/_table.py:
    class SourceTable(TableSource):
        _beava_kind = "source_table"
        def group_by(self, *_a, **_kw):
            raise RuntimeError("source tables are passive enrichment ...")

  python/beava/__init__.py:
    from ._table import source_table, SourceTable

Run (post-Wave-2):
  cd python && python -m pytest tests/test_source_table_decorator.py -v
"""

import pytest


@pytest.mark.skip(reason="55-W2 — source_table decorator lands in Wave 2 (plan 55-02 Task 3)")
def test_source_table_basic():
    """Decorator returns a SourceTable class with _beava_kind == 'source_table'."""
    import beava as bv

    @bv.source_table(key="country_code")
    class Countries:
        country_code: str
        name: str

    assert isinstance(Countries, bv.SourceTable)
    assert Countries._beava_kind == "source_table"
    assert Countries._key == ["country_code"]


@pytest.mark.skip(reason="55-W2 — source_table decorator lands in Wave 2")
def test_source_table_rejects_group_by():
    """D-B6: source tables are passive enrichment — .group_by() rejected."""
    import beava as bv

    @bv.source_table(key="country_code")
    class Countries:
        country_code: str

    with pytest.raises(RuntimeError, match="passive enrichment"):
        Countries.group_by("country_code")


@pytest.mark.skip(reason="55-W2 — source_table decorator lands in Wave 2")
def test_source_table_requires_key():
    """key= is required at decoration time — TypeError on omission."""
    import beava as bv

    with pytest.raises(TypeError, match="requires key"):

        @bv.source_table()
        class X:
            id: str
