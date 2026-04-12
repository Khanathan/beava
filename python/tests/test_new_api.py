"""Tests for the new v2.0 API: EventSet, FeatureSet, Field, @tl.source, @tl.dataset."""

from __future__ import annotations

import pytest


class TestSchema:
    """Tests for EventSet, FeatureSet, and Field from _schema.py."""

    def test_eventset_collects_fields(self):
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()

        assert "user_id" in TxnEvent._fields
        assert "amount" in TxnEvent._fields
        assert len(TxnEvent._fields) == 2

    def test_featureset_collects_fields(self):
        from tally._schema import FeatureSet, Field

        class TxnFeatures(FeatureSet):
            tx_count: int = Field()

        assert "tx_count" in TxnFeatures._fields
        assert len(TxnFeatures._fields) == 1

    def test_field_stores_attributes(self):
        from tally._schema import Field

        f = Field(dtype=float, description="amount in USD", default=0.0)
        assert f.dtype is float
        assert f.description == "amount in USD"
        assert f.default == 0.0

    def test_eventset_instantiation(self):
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()

        evt = TxnEvent(user_id="u1", amount=50.0)
        assert evt.user_id == "u1"
        assert evt.amount == 50.0

    def test_eventset_missing_required_raises(self):
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()

        with pytest.raises(TypeError):
            TxnEvent(user_id="u1")  # missing amount

    def test_field_infers_dtype_from_annotation(self):
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()

        assert TxnEvent._fields["user_id"].dtype is str
        assert TxnEvent._fields["amount"].dtype is float

    def test_field_with_default_is_optional(self):
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field(default=0.0)

        evt = TxnEvent(user_id="u1")
        assert evt.amount == 0.0

    def test_bare_annotation_creates_field(self):
        """Annotation without explicit Field() should auto-create a Field."""
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str
            amount: float = Field()

        assert "user_id" in TxnEvent._fields
        assert "amount" in TxnEvent._fields


class TestSource:
    """Tests for @tl.source decorator from _source.py."""

    def test_source_compile_basic(self):
        from tally._source import source

        @source
        class Transactions:
            pass

        result = Transactions._compile()
        assert result == {"name": "Transactions", "key_field": None, "features": []}

    def test_source_stores_event_schema(self):
        from tally._schema import EventSet, Field
        from tally._source import source

        class TxnEvent(EventSet):
            user_id: str = Field()

        @source
        class Transactions:
            event = TxnEvent

        assert Transactions._event_schema is TxnEvent

    def test_source_has_tally_stream_name(self):
        from tally._source import source

        @source
        class Transactions:
            pass

        assert Transactions._tally_stream_name == "Transactions"

    def test_source_to_register_json_compat(self):
        from tally._source import source

        @source
        class Transactions:
            pass

        assert Transactions._to_register_json() == Transactions._compile()

    def test_source_collect_registrations(self):
        from tally._source import source

        @source
        class Transactions:
            pass

        regs = Transactions._collect_registrations()
        assert len(regs) == 1
        assert regs[0] == Transactions._compile()

    def test_source_with_entity_ttl(self):
        from tally._source import source

        @source(entity_ttl="5m")
        class Transactions:
            pass

        result = Transactions._compile()
        assert result["entity_ttl"] == "5m"

    def test_source_with_history_ttl(self):
        from tally._source import source

        @source(history_ttl="72h")
        class Transactions:
            pass

        result = Transactions._compile()
        assert result["history_ttl"] == "72h"


class TestGroupByAgg:
    """Tests for group_by() free function and GroupedDataset."""

    def test_group_by_agg_returns_grouped_dataset(self):
        from tally._dataset import group_by, GroupedDataset
        from tally._operators import Count

        gd = group_by("user_id").agg(tx_count=Count(window="1h"))
        assert isinstance(gd, GroupedDataset)
        assert gd._key == "user_id"
        assert "tx_count" in gd._features

    def test_group_by_agg_features_match_operator_json(self):
        from tally._dataset import group_by
        from tally._operators import Count, Sum

        gd = group_by("user_id").agg(
            tx_count=Count(window="1h"),
            tx_sum=Sum("amount", window="1h"),
        )
        assert gd._features["tx_count"].to_json("tx_count") == {
            "name": "tx_count", "type": "count", "window": "1h"
        }
        assert gd._features["tx_sum"].to_json("tx_sum") == {
            "name": "tx_sum", "type": "sum", "field": "amount", "window": "1h"
        }

    def test_group_by_agg_rejects_non_operator(self):
        from tally._dataset import group_by

        with pytest.raises(TypeError):
            group_by("user_id").agg(bad="not an operator")


class TestDataset:
    """Tests for @tl.dataset decorator and DatasetDef."""

    def _make_source(self):
        from tally._source import source

        @source
        class RawTxns:
            pass

        return RawTxns

    def test_dataset_compile_basic(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count

        src = self._make_source()

        @dataset(depends_on=[src])
        class UserTxns:
            features = group_by("user_id").agg(tx_count=Count(window="1h"))

        result = UserTxns._compile()
        assert result["name"] == "UserTxns"
        assert result["key_field"] == "user_id"
        assert result["depends_on"] == ["RawTxns"]
        assert len(result["features"]) == 1
        assert result["features"][0] == {"name": "tx_count", "type": "count", "window": "1h"}

    def test_dataset_depends_on_resolves_names(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count
        from tally._source import source

        @source
        class SourceA:
            pass

        @source
        class SourceB:
            pass

        @dataset(depends_on=[SourceA])
        class DS:
            features = group_by("key").agg(c=Count(window="1h"))

        assert DS._compile()["depends_on"] == ["SourceA"]

    def test_dataset_with_ttls(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count

        src = self._make_source()

        @dataset(depends_on=[src], entity_ttl="5m", history_ttl="72h")
        class UserTxns:
            features = group_by("user_id").agg(tx_count=Count(window="1h"))

        result = UserTxns._compile()
        assert result["entity_ttl"] == "5m"
        assert result["history_ttl"] == "72h"

    def test_dataset_tally_stream_name_compat(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count

        src = self._make_source()

        @dataset(depends_on=[src])
        class UserTxns:
            features = group_by("user_id").agg(tx_count=Count(window="1h"))

        assert UserTxns._tally_stream_name == "UserTxns"
        assert UserTxns._to_register_json() == UserTxns._compile()

    def test_dataset_collect_registrations_includes_upstream(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count

        src = self._make_source()

        @dataset(depends_on=[src])
        class UserTxns:
            features = group_by("user_id").agg(tx_count=Count(window="1h"))

        regs = UserTxns._collect_registrations()
        assert len(regs) == 2  # source + dataset
        assert regs[0]["name"] == "RawTxns"
        assert regs[1]["name"] == "UserTxns"

    def test_dataset_with_derive_features(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count, Derive

        src = self._make_source()

        @dataset(depends_on=[src])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=Count(window="1h"),
                failed_count=Count(window="1h", where="status == 'failed'"),
            )
            failure_rate = Derive("failed_count / tx_count")

        result = UserTxns._compile()
        feature_names = [f["name"] for f in result["features"]]
        assert "tx_count" in feature_names
        assert "failed_count" in feature_names
        assert "failure_rate" in feature_names


class TestUnion:
    """Tests for tl.union() and UnionSource."""

    def test_union_basic(self):
        from tally._dataset import union, UnionSource
        from tally._source import source

        @source
        class SourceA:
            pass

        @source
        class SourceB:
            pass

        u = union(SourceA, SourceB)
        assert isinstance(u, UnionSource)
        names = u._get_depends_on_names()
        assert names == ["SourceA", "SourceB"]

    def test_dataset_with_union_depends_on(self):
        from tally._dataset import dataset, group_by, union
        from tally._operators import Count
        from tally._source import source

        @source
        class SourceA:
            pass

        @source
        class SourceB:
            pass

        @dataset(depends_on=[union(SourceA, SourceB)])
        class Combined:
            features = group_by("user_id").agg(total=Count(window="1h"))

        result = Combined._compile()
        assert sorted(result["depends_on"]) == ["SourceA", "SourceB"]

    def test_union_collect_registrations_includes_all_sources(self):
        from tally._dataset import dataset, group_by, union
        from tally._operators import Count
        from tally._source import source

        @source
        class SourceA:
            pass

        @source
        class SourceB:
            pass

        @dataset(depends_on=[union(SourceA, SourceB)])
        class Combined:
            features = group_by("user_id").agg(total=Count(window="1h"))

        regs = Combined._collect_registrations()
        reg_names = [r["name"] for r in regs]
        assert "SourceA" in reg_names
        assert "SourceB" in reg_names
        assert "Combined" in reg_names


class TestValidate:
    """Tests for validate() from _validate.py."""

    def _make_source(self, name="RawTxns", event_schema=None):
        from tally._source import SourceDef
        return SourceDef(name=name, event_schema=event_schema)

    def _make_dataset(self, name, depends_on, grouped=None, extra_features=None):
        from tally._dataset import DatasetDef
        return DatasetDef(
            name=name,
            depends_on=depends_on,
            grouped_dataset=grouped,
            extra_features=extra_features,
        )

    def test_valid_pipeline_returns_empty_list(self):
        from tally._validate import validate
        from tally._dataset import group_by
        from tally._operators import Count

        src = self._make_source()
        ds = self._make_dataset(
            "UserTxns",
            depends_on=[src],
            grouped=group_by("user_id").agg(tx_count=Count(window="1h")),
        )
        errors = validate(src, ds)
        assert errors == []

    def test_cycle_detection_two_nodes(self):
        from tally._validate import validate, ValidationError

        # A depends on B, B depends on A
        a = self._make_dataset("A", depends_on=[])
        b = self._make_dataset("B", depends_on=[])
        # Manually wire circular deps
        a._depends_on = [b]
        b._depends_on = [a]
        errors = validate(a, b)
        assert len(errors) >= 1
        assert any(e.kind == "cycle" for e in errors)

    def test_missing_dep_detection(self):
        from tally._validate import validate, ValidationError

        # Dataset depends on a source not in the validate() call
        unregistered = self._make_source("Ghost")
        ds = self._make_dataset("MyDS", depends_on=[unregistered])
        errors = validate(ds)  # Ghost not passed to validate
        assert len(errors) >= 1
        assert any(e.kind == "missing_dep" for e in errors)

    def test_validation_error_attributes(self):
        from tally._validate import ValidationError

        err = ValidationError(path="A -> B", message="cycle detected", kind="cycle")
        assert err.path == "A -> B"
        assert err.message == "cycle detected"
        assert err.kind == "cycle"
        assert "cycle" in repr(err)

    def test_type_mismatch_field_not_in_eventset(self):
        from tally._validate import validate
        from tally._schema import EventSet, Field
        from tally._dataset import group_by
        from tally._operators import Sum

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()

        src = self._make_source("RawTxns", event_schema=TxnEvent)
        # Sum on "nonexistent_field" which is not in TxnEvent
        ds = self._make_dataset(
            "UserTxns",
            depends_on=[src],
            grouped=group_by("user_id").agg(
                bad_sum=Sum("nonexistent_field", window="1h"),
            ),
        )
        errors = validate(src, ds)
        assert len(errors) >= 1
        assert any(e.kind == "type_mismatch" for e in errors)

    def test_validate_with_union_sources(self):
        from tally._validate import validate
        from tally._dataset import group_by, union
        from tally._operators import Count

        src_a = self._make_source("SourceA")
        src_b = self._make_source("SourceB")
        ds = self._make_dataset(
            "Combined",
            depends_on=[union(src_a, src_b)],
            grouped=group_by("key").agg(total=Count(window="1h")),
        )
        errors = validate(src_a, src_b, ds)
        assert errors == []

    def test_self_referencing_dataset_returns_cycle(self):
        from tally._validate import validate

        ds = self._make_dataset("SelfRef", depends_on=[])
        ds._depends_on = [ds]  # self-reference
        errors = validate(ds)
        assert len(errors) >= 1
        assert any(e.kind == "cycle" for e in errors)

    def test_validate_is_pure_function_no_network(self):
        """validate() should not import any network/socket modules."""
        import importlib
        import tally._validate as mod
        source_code = importlib.util.find_spec("tally._validate")
        # Check the module doesn't reference network
        import inspect
        src = inspect.getsource(mod)
        assert "socket" not in src
        assert "TallyClient" not in src
        assert "_client" not in src


class TestExports:
    """Tests that all new API symbols are importable via 'import tally as tl'."""

    def test_source_importable(self):
        import tally as tl
        from tally._source import source
        assert tl.source is source

    def test_dataset_importable(self):
        import tally as tl
        from tally._dataset import dataset
        assert tl.dataset is dataset

    def test_group_by_importable(self):
        import tally as tl
        from tally._dataset import group_by
        assert tl.group_by is group_by

    def test_union_importable(self):
        import tally as tl
        from tally._dataset import union
        assert tl.union is union

    def test_validate_importable(self):
        import tally as tl
        from tally._validate import validate
        assert tl.validate is validate

    def test_eventset_importable(self):
        import tally as tl
        from tally._schema import EventSet
        assert tl.EventSet is EventSet

    def test_featureset_importable(self):
        import tally as tl
        from tally._schema import FeatureSet
        assert tl.FeatureSet is FeatureSet

    def test_field_importable(self):
        import tally as tl
        from tally._schema import Field
        assert tl.Field is Field

    def test_validation_error_importable(self):
        import tally as tl
        from tally._validate import ValidationError
        assert tl.ValidationError is ValidationError

    def test_old_api_still_works(self):
        import tally as tl
        assert hasattr(tl, "stream")
        assert hasattr(tl, "view")
        assert hasattr(tl, "App")


class TestJsonCompat:
    """Tests that new API JSON output matches old API format."""

    def test_keyed_stream_json_matches(self):
        """Old @st.stream(key=...) and new @tl.dataset produce same JSON shape."""
        import tally as tl
        from tally._stream import stream as st_stream
        from tally._source import source
        from tally._dataset import dataset, group_by
        from tally._operators import Count, Sum

        # Old API
        @st_stream(key="user_id")
        class OldTxns:
            tx_count = Count(window="1h")
            tx_sum = Sum("amount", window="1h")

        old_json = OldTxns._to_register_json()

        # New API
        @source
        class RawTxns:
            pass

        @dataset(depends_on=[RawTxns])
        class NewTxns:
            features = group_by("user_id").agg(
                tx_count=Count(window="1h"),
                tx_sum=Sum("amount", window="1h"),
            )

        new_json = NewTxns._to_register_json()

        # Key field must match
        assert old_json["key_field"] == new_json["key_field"] == "user_id"

        # Features must match (same dicts, order may differ)
        old_features = sorted(old_json["features"], key=lambda f: f["name"])
        new_features = sorted(new_json["features"], key=lambda f: f["name"])
        assert old_features == new_features

    def test_keyless_source_json_matches(self):
        """Old keyless @st.stream() and new @tl.source produce same JSON shape."""
        from tally._stream import stream as st_stream
        from tally._source import source

        @st_stream()
        class OldRaw:
            pass

        old_json = OldRaw._to_register_json()

        @source
        class NewRaw:
            pass

        new_json = NewRaw._to_register_json()

        # Both keyless
        assert old_json["key_field"] is None
        assert new_json["key_field"] is None
        # Both empty features
        assert old_json["features"] == []
        assert new_json["features"] == []


class TestIntegration:
    """Integration tests: _collect_registrations ordering and App.register compat."""

    def test_collect_registrations_deps_first(self):
        """_collect_registrations() returns sources before datasets."""
        from tally._source import source
        from tally._dataset import dataset, group_by
        from tally._operators import Count

        @source
        class RawTxns:
            pass

        @dataset(depends_on=[RawTxns])
        class UserTxns:
            features = group_by("user_id").agg(tx_count=Count(window="1h"))

        regs = UserTxns._collect_registrations()
        names = [r["name"] for r in regs]
        assert names == ["RawTxns", "UserTxns"]

    def test_register_accepts_new_api_objects(self):
        """App.register() should not raise for SourceDef/DatasetDef objects.

        We test the JSON generation path without an actual server connection
        by verifying _collect_registrations works and produces valid dicts.
        """
        from tally._source import source
        from tally._dataset import dataset, group_by
        from tally._operators import Count

        @source
        class RawTxns:
            pass

        @dataset(depends_on=[RawTxns])
        class UserTxns:
            features = group_by("user_id").agg(tx_count=Count(window="1h"))

        # Verify _collect_registrations works (this is what App.register calls)
        assert hasattr(RawTxns, "_collect_registrations")
        assert hasattr(UserTxns, "_collect_registrations")

        src_regs = RawTxns._collect_registrations()
        assert len(src_regs) == 1
        assert "name" in src_regs[0]
        assert "key_field" in src_regs[0]
        assert "features" in src_regs[0]

        ds_regs = UserTxns._collect_registrations()
        assert len(ds_regs) == 2
        for reg in ds_regs:
            assert "name" in reg
            assert "key_field" in reg
            assert "features" in reg


class TestProjection:
    """Tests for DatasetDef.select() and .drop() projection methods."""

    def _make_source(self):
        from tally._source import source

        @source
        class RawTxns:
            pass

        return RawTxns

    def _make_dataset(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count, Sum, Derive

        src = self._make_source()

        @dataset(depends_on=[src])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=Count(window="1h"),
                tx_sum=Sum("amount", window="1h"),
            )
            ratio = Derive("tx_count / tx_sum")

        return UserTxns

    def test_select_returns_new_instance(self):
        ds = self._make_dataset()
        projected = ds.select(["tx_count", "tx_sum"])
        assert projected is not ds
        assert id(projected) != id(ds)

    def test_select_compile_emits_projection(self):
        ds = self._make_dataset()
        projected = ds.select(["tx_count", "tx_sum"])
        result = projected._compile()
        assert "projection" in result
        assert result["projection"] == {"select": ["tx_count", "tx_sum"]}

    def test_drop_compile_emits_projection(self):
        ds = self._make_dataset()
        projected = ds.drop(["ratio"])
        result = projected._compile()
        assert "projection" in result
        assert result["projection"] == {"drop": ["ratio"]}

    def test_select_preserves_original(self):
        ds = self._make_dataset()
        _ = ds.select(["tx_count"])
        result = ds._compile()
        assert "projection" not in result

    def test_no_projection_by_default(self):
        ds = self._make_dataset()
        result = ds._compile()
        assert "projection" not in result

    def test_select_preserves_fields(self):
        ds = self._make_dataset()
        projected = ds.select(["tx_count"])
        original_compile = ds._compile()
        projected_compile = projected._compile()

        # Name, key_field, depends_on, features should match
        assert projected_compile["name"] == original_compile["name"]
        assert projected_compile["key_field"] == original_compile["key_field"]
        assert projected_compile.get("depends_on") == original_compile.get("depends_on")
        assert projected_compile["features"] == original_compile["features"]

    def test_select_preserves_ttls(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count
        from tally._source import source

        @source
        class Src:
            pass

        @dataset(depends_on=[Src], entity_ttl="5m", history_ttl="72h")
        class DS:
            features = group_by("k").agg(c=Count(window="1h"))

        projected = DS.select(["c"])
        result = projected._compile()
        assert result["entity_ttl"] == "5m"
        assert result["history_ttl"] == "72h"
        assert result["projection"] == {"select": ["c"]}


# ===========================================================================
# End-to-end integration tests for projection (Task 2)
#
# These tests use a standalone server (not the session-scoped tally_server)
# because registering streams with projection on a shared server causes
# cross-stream interference in get_features (known limitation: projections
# apply globally, not per-stream).
# ===========================================================================


@pytest.fixture(scope="function")
def projection_server():
    """Start a fresh Tally server for projection E2E tests."""
    import os
    import socket
    import subprocess
    import time

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    binary = os.path.join(project_root, "target", "debug", "tally")

    def find_port():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    tcp_port = find_port()
    http_port = find_port()

    env = os.environ.copy()
    env["TALLY_TCP_PORT"] = str(tcp_port)
    env["TALLY_HTTP_PORT"] = str(http_port)

    proc = subprocess.Popen(
        [binary], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", tcp_port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.1)

    yield "127.0.0.1", tcp_port, http_port

    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=3)


def test_projection_select_e2e(projection_server):
    """select() filters GET responses to only named features."""
    import tally as tl

    host, tcp_port, _ = projection_server

    @tl.source
    class RawTxns_sel:
        pass

    @tl.dataset(depends_on=[RawTxns_sel])
    class UserTxns_sel:
        features = tl.group_by("user_id").agg(
            sel_count_1h=tl.count(window="1h"),
            sel_sum_1h=tl.sum("amount", window="1h"),
            sel_internal=tl.count(window="24h"),
        )

    projected = UserTxns_sel.select(["sel_count_1h", "sel_sum_1h"])

    app = tl.App(f"{host}:{tcp_port}")
    app.register(RawTxns_sel, projected)

    # Push to keyless source (sync to ensure cascade completes before GET)
    app.push_sync(RawTxns_sel, {"user_id": "sel_u1", "amount": 42.0})

    # GET should return only selected features (projection applied)
    get_result = app.get("sel_u1")
    gd = get_result.to_dict()
    assert gd.get("sel_count_1h") == 1
    assert gd.get("sel_sum_1h") == 42.0
    assert "sel_internal" not in gd
    app.close()


def test_projection_drop_e2e(projection_server):
    """drop() excludes named features from GET responses."""
    import tally as tl

    host, tcp_port, _ = projection_server

    @tl.source
    class RawTxns_drp:
        pass

    @tl.dataset(depends_on=[RawTxns_drp])
    class UserTxns_drp:
        features = tl.group_by("user_id").agg(
            drp_count_1h=tl.count(window="1h"),
            drp_sum_1h=tl.sum("amount", window="1h"),
            drp_internal=tl.count(window="24h"),
        )

    projected = UserTxns_drp.drop(["drp_internal"])

    app = tl.App(f"{host}:{tcp_port}")
    app.register(RawTxns_drp, projected)

    # Push to keyless source (sync to ensure cascade completes before GET)
    app.push_sync(RawTxns_drp, {"user_id": "drp_u1", "amount": 55.0})

    # GET should exclude dropped features
    get_result = app.get("drp_u1")
    gd = get_result.to_dict()
    assert gd.get("drp_count_1h") == 1
    assert gd.get("drp_sum_1h") == 55.0
    assert "drp_internal" not in gd
    app.close()


def test_projection_derive_e2e(projection_server):
    """Derives evaluate correctly even when referenced features are projected out."""
    import tally as tl

    host, tcp_port, _ = projection_server

    @tl.source
    class RawTxns_derv:
        pass

    @tl.dataset(depends_on=[RawTxns_derv])
    class UserTxns_derv:
        features = tl.group_by("user_id").agg(
            derv_cnt=tl.count(window="1h"),
            derv_internal=tl.count(window="24h"),
        )
        derv_ratio = tl.derive("derv_cnt / derv_internal")

    # Select derv_cnt and derv_ratio but NOT derv_internal
    projected = UserTxns_derv.select(["derv_cnt", "derv_ratio"])

    app = tl.App(f"{host}:{tcp_port}")
    app.register(RawTxns_derv, projected)

    # Push to keyless source (sync to ensure cascade completes)
    app.push_sync(RawTxns_derv, {"user_id": "derv_u1"})

    # GET verifies projection: derive evaluates BEFORE projection,
    # so derv_ratio is correct even though derv_internal is projected out
    get_result = app.get("derv_u1")
    d = get_result.to_dict()
    assert d.get("derv_cnt") == 1
    # derv_ratio = derv_cnt / derv_internal = 1 / 1 = 1.0
    assert d.get("derv_ratio") == 1.0
    assert "derv_internal" not in d
    app.close()
