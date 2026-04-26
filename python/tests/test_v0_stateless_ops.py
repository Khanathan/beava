"""Tests for the 8 stateless per-row operators on Stream and Table.

Each op must behave identically on both input types, returning the SAME
outer runtime type (Stream→Stream, Table→Table), preserving and transforming
the inferred schema, and raising surgical errors with Levenshtein hints when
user input references unknown fields.
"""

from __future__ import annotations

import pytest

from tally._col import col
from tally._stream import Stream, StreamSource, stream
from tally._table import Table, TableSource, table


# ---------------------------------------------------------------------------
# Fixture helpers: build a source schema on both Stream and Table inputs.
# ---------------------------------------------------------------------------


def _make_stream_source() -> StreamSource:
    @stream
    class Purchases:
        user_id: str
        amount: float
        merchant_id: str
        status: str

    return Purchases


def _make_table_source() -> TableSource:
    @table(key="user_id")
    class Users:
        user_id: str
        amount: float
        merchant_id: str
        status: str

    return Users


# Parametrised matrix: (factory, outer-type) so each op is tested on both.
_PARAMS = [
    pytest.param(_make_stream_source, Stream, id="stream"),
    pytest.param(_make_table_source, Table, id="table"),
]


# ---------------------------------------------------------------------------
# .filter
# ---------------------------------------------------------------------------


class TestFilter:
    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_filter_preserves_schema_and_outer_type(self, factory, outer):
        src = factory()
        out = src.filter(col("amount") > 100)
        assert isinstance(out, outer)
        assert list(out.describe()["fields"].keys()) == list(
            src.describe()["fields"].keys()
        )

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_filter_records_op(self, factory, outer):
        src = factory()
        out = src.filter(col("amount") > 100)
        assert out._ops[-1]["op"] == "filter"
        assert "amount" in out._ops[-1]["expr"]

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_filter_string_expr_rejected(self, factory, outer):
        src = factory()
        with pytest.raises(TypeError) as ei:
            src.filter("amount > 100")
        msg = str(ei.value)
        assert "tl.col" in msg
        assert "string" in msg

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_filter_unknown_field_raises_with_suggestion(self, factory, outer):
        src = factory()
        with pytest.raises(TypeError) as ei:
            src.filter(col("amout") > 100)
        msg = str(ei.value)
        assert "'amout'" in msg
        assert "'amount'" in msg
        assert "available" in msg

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_filter_reports_all_unknown_fields(self, factory, outer):
        src = factory()
        with pytest.raises(TypeError) as ei:
            src.filter((col("amout") > 100) & (col("statuz") == "failed"))
        msg = str(ei.value)
        assert "'amout'" in msg
        assert "'statuz'" in msg


# ---------------------------------------------------------------------------
# .select
# ---------------------------------------------------------------------------


class TestSelect:
    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_select_reduces_schema(self, factory, outer):
        src = factory()
        out = src.select("user_id", "amount")
        assert isinstance(out, outer)
        assert list(out.describe()["fields"].keys()) == ["user_id", "amount"]

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_select_preserves_order_from_args(self, factory, outer):
        src = factory()
        out = src.select("amount", "user_id")
        assert list(out.describe()["fields"].keys()) == ["amount", "user_id"]

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_select_unknown_field_raises_with_suggestion(self, factory, outer):
        src = factory()
        with pytest.raises(TypeError) as ei:
            src.select("user_id", "amout")
        msg = str(ei.value)
        assert "'amout'" in msg
        assert "'amount'" in msg


# ---------------------------------------------------------------------------
# .drop
# ---------------------------------------------------------------------------


class TestDrop:
    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_drop_removes_field(self, factory, outer):
        src = factory()
        out = src.drop("merchant_id")
        assert "merchant_id" not in out.describe()["fields"]
        assert isinstance(out, outer)

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_drop_preserves_remaining_order(self, factory, outer):
        src = factory()
        out = src.drop("merchant_id")
        assert list(out.describe()["fields"].keys()) == [
            "user_id", "amount", "status"
        ]

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_drop_unknown_raises(self, factory, outer):
        src = factory()
        with pytest.raises(TypeError) as ei:
            src.drop("mrchnt_id")
        assert "'mrchnt_id'" in str(ei.value)

    def test_table_cannot_drop_key_field(self):
        t = _make_table_source()
        with pytest.raises(TypeError) as ei:
            t.drop("user_id")
        msg = str(ei.value)
        assert "cannot drop key field" in msg
        assert "'user_id'" in msg


# ---------------------------------------------------------------------------
# .rename
# ---------------------------------------------------------------------------


class TestRename:
    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_rename_updates_field_name(self, factory, outer):
        src = factory()
        out = src.rename(amount="total")
        fields = out.describe()["fields"]
        assert "amount" not in fields
        assert "total" in fields

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_rename_unknown_source_raises(self, factory, outer):
        src = factory()
        with pytest.raises(TypeError):
            src.rename(amout="total")

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_rename_target_collision_raises(self, factory, outer):
        src = factory()
        with pytest.raises(TypeError) as ei:
            src.rename(amount="status")
        msg = str(ei.value)
        assert "status" in msg
        assert "collides" in msg

    def test_table_rename_cascades_to_key(self):
        t = _make_table_source()
        out = t.rename(user_id="uid")
        assert out._key == ["uid"]
        assert "uid" in out.describe()["fields"]


# ---------------------------------------------------------------------------
# .with_columns / .map
# ---------------------------------------------------------------------------


class TestWithColumns:
    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_with_columns_adds_new_field(self, factory, outer):
        src = factory()
        out = src.with_columns(doubled=col("amount") * 2)
        assert "doubled" in out.describe()["fields"]
        assert isinstance(out, outer)

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_with_columns_infers_float_for_arithmetic(self, factory, outer):
        src = factory()
        out = src.with_columns(doubled=col("amount") * 2)
        assert out.describe()["fields"]["doubled"]["type"] == "float"

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_with_columns_infers_bool_for_comparison(self, factory, outer):
        src = factory()
        out = src.with_columns(big=col("amount") > 100)
        assert out.describe()["fields"]["big"]["type"] == "bool"

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_with_columns_unknown_field_raises(self, factory, outer):
        src = factory()
        with pytest.raises(TypeError) as ei:
            src.with_columns(d=col("amout") * 2)
        assert "'amout'" in str(ei.value)

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_with_columns_replaces_existing_field(self, factory, outer):
        src = factory()
        out = src.with_columns(amount=col("amount") * 2)
        # Still there, type still float (arithmetic)
        assert out.describe()["fields"]["amount"]["type"] == "float"

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_map_is_alias_for_with_columns(self, factory, outer):
        src = factory()
        out = src.map(doubled=col("amount") * 2)
        assert "doubled" in out.describe()["fields"]
        assert out._ops[-1]["op"] == "with_columns"


# ---------------------------------------------------------------------------
# .cast
# ---------------------------------------------------------------------------


class TestCast:
    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_cast_updates_type(self, factory, outer):
        src = factory()
        out = src.cast(amount="int")
        assert out.describe()["fields"]["amount"]["type"] == "int"

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_cast_unknown_field_raises(self, factory, outer):
        src = factory()
        with pytest.raises(TypeError) as ei:
            src.cast(amout="int")
        assert "'amout'" in str(ei.value)

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_cast_invalid_target_type_raises(self, factory, outer):
        src = factory()
        with pytest.raises(TypeError) as ei:
            src.cast(amount="quux")
        msg = str(ei.value)
        assert "quux" in msg


# ---------------------------------------------------------------------------
# .fillna
# ---------------------------------------------------------------------------


class TestFillna:
    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_fillna_clears_optional(self, factory, outer):
        # Start with a source that has an optional field
        from tally._types_core import Optional
        if outer is Stream:
            @stream
            class S:
                user_id: str
                amount: Optional[float]
            src = S
        else:
            @table(key="user_id")
            class T:
                user_id: str
                amount: Optional[float]
            src = T

        assert src.describe()["fields"]["amount"]["optional"] is True
        out = src.fillna(amount=0.0)
        assert out.describe()["fields"]["amount"]["optional"] is False

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_fillna_unknown_raises(self, factory, outer):
        src = factory()
        with pytest.raises(TypeError):
            src.fillna(amout=0)


# ---------------------------------------------------------------------------
# Chaining + schema propagation end-to-end
# ---------------------------------------------------------------------------


class TestChaining:
    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_chain_select_rename_with_columns_filter(self, factory, outer):
        src = factory()
        # Use non-key-renaming chain that works on both Stream and Table
        out = (
            src.select("user_id", "amount", "status")
            .rename(amount="total")
            .with_columns(big=col("total") > 100)
            .filter(col("big"))
        )
        names = list(out.describe()["fields"].keys())
        assert names == ["user_id", "total", "status", "big"]
        # All four ops recorded
        assert [o["op"] for o in out._ops] == [
            "select", "rename", "with_columns", "filter"
        ]

    @pytest.mark.parametrize("factory,outer", _PARAMS)
    def test_outer_type_preserved_across_chain(self, factory, outer):
        src = factory()
        out = src.select("user_id", "amount").with_columns(d=col("amount") * 2)
        assert isinstance(out, outer)
