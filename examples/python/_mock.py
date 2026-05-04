"""
Phase 13.0 mock backend for runnable demos.

In-memory App shim with MINIMAL AGGREGATION LOGIC. Drop-in for bv.App()
during Phase 13.0 verification. Replaced by real bv.App() once Phase 13.5
lands (single-line edit per demo).

Per Q2 locked answer + BLOCKER 4 checker fix: this mock COMPUTES features
by applying registered descriptors on push. Demos go through the full
register -> push -> query flow (no _seed pre-population) so contract drift
between specs and the real engine surfaces immediately at the
13.5/13.6 re-verification step.

Supported ops in this mock (minimum for the 9 vertical demos):
- count: increment per matching event
- sum: accumulate field value
- mean: running sum / count
- min, max: comparison

Sketches (n_unique, quantile, top_k), decays (ewma, ewvar, ew_zscore,
decayed_*), velocity (rate_of_change, trend, etc.), and geo
(geo_velocity, geo_distance, etc.) are NOT computed here -- demo files
that use these ops document the no-op fallback inline. The real engine
in 13.4 + 13.5 + 13.6 fills the gap; demos must pick ops covered here
for the assertions, then add the more-complex ops as additional
register-target features (their values are surfaced as None or absent
from the row).
"""
from typing import Any


class _AggSpec:
    """Minimal in-memory record of a registered aggregation op."""

    __slots__ = ("op", "field", "where")

    def __init__(self, op: str, field: str | None, where: dict | None = None):
        self.op = op
        self.field = field
        self.where = where  # not used in this minimal mock


class _Descriptor:
    """A registered event source or table aggregation."""

    __slots__ = ("name", "kind", "source", "key_cols", "ops")

    def __init__(
        self,
        name: str,
        kind: str,
        source: str | None,
        key_cols: list[str],
        ops: dict[str, _AggSpec],
    ):
        self.name = name
        self.kind = kind  # "event" | "table"
        self.source = source  # source event name (None for kind="event")
        self.key_cols = key_cols
        self.ops = ops  # feature_name -> _AggSpec


class MockApp:
    """Phase 13.0 stub. Replaced by real bv.App() in Phase 13.5.

    Computes aggregations via push (count, sum, mean, min, max).
    Other ops (sketches, decays, geo) are no-ops -- demos pick coverage-
    aware features for assertions.
    """

    def __init__(self):
        self._registered: list[_Descriptor] = []
        self._tables: dict[str, dict[str, dict[str, Any]]] = {}
        # _agg_state[(table, key, feature)] = {sum, count, min, max} for streaming compute
        self._agg_state: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._registry_version = 0

    def register(self, *descriptors, force: bool = False, dry_run: bool = False):
        if dry_run:
            return {
                "status": "ok",
                "registry_version": self._registry_version,
                "added": [],
                "removed": [],
                "changed": [],
                "diff": {},
            }
        for desc in descriptors:
            if isinstance(desc, _Descriptor):
                self._registered.append(desc)
            else:
                # Demo helpers may pass a class or dict; tolerate it.
                name = (
                    getattr(desc, "_name", None)
                    or getattr(desc, "name", None)
                    or str(desc)
                )
                kind = getattr(desc, "_kind", "event")
                source = getattr(desc, "_source", None)
                key_cols = getattr(desc, "_key", [])
                ops = getattr(desc, "_ops", {})
                self._registered.append(
                    _Descriptor(name, kind, source, key_cols, ops)
                )
        self._registry_version = len(self._registered)
        return {
            "status": "ok",
            "registry_version": self._registry_version,
            "added": [d.name for d in self._registered],
        }

    def push(self, source: str, event: dict) -> dict:
        """Apply each registered table descriptor whose source matches the pushed event."""
        for desc in self._registered:
            if desc.kind != "table":
                continue
            if desc.source != source:
                continue
            key = self._key_from_event(desc.key_cols, event)
            for feature_name, agg in desc.ops.items():
                self._update(desc.name, key, feature_name, agg, event)
        return {"ack_lsn": 1, "registry_version": self._registry_version}

    def _key_from_event(self, key_cols: list[str], event: dict) -> str:
        if not key_cols:
            return "_global"
        return "|".join(str(event.get(k, "")) for k in key_cols)

    def _update(
        self, table: str, key: str, feature: str, agg: _AggSpec, event: dict
    ) -> None:
        state_key = (table, key, feature)
        state = self._agg_state.setdefault(state_key, {})
        op = agg.op
        if op == "count":
            state["count"] = state.get("count", 0) + 1
            self._set_value(table, key, feature, state["count"])
        elif op == "sum":
            value = float(event.get(agg.field, 0)) if agg.field else 0.0
            state["sum"] = state.get("sum", 0.0) + value
            self._set_value(table, key, feature, state["sum"])
        elif op == "mean":
            value = float(event.get(agg.field, 0)) if agg.field else 0.0
            state["sum"] = state.get("sum", 0.0) + value
            state["count"] = state.get("count", 0) + 1
            self._set_value(table, key, feature, state["sum"] / state["count"])
        elif op == "min":
            value = float(event.get(agg.field, 0)) if agg.field else 0.0
            if "min" not in state:
                state["min"] = value
            else:
                state["min"] = min(state["min"], value)
            self._set_value(table, key, feature, state["min"])
        elif op == "max":
            value = float(event.get(agg.field, 0)) if agg.field else 0.0
            if "max" not in state:
                state["max"] = value
            else:
                state["max"] = max(state["max"], value)
            self._set_value(table, key, feature, state["max"])
        else:
            # Unsupported in mock (sketches, decays, geo, etc.): no-op.
            # Real engine in 13.4 + 13.5 + 13.6 fills the gap.
            pass

    def _set_value(self, table: str, key: str, feature: str, value: Any) -> None:
        self._tables.setdefault(table, {}).setdefault(key, {})[feature] = value

    def get(self, table: str, key) -> dict[str, Any]:
        key_str = key if isinstance(key, str) else "|".join(str(k) for k in key)
        return self._tables.get(table, {}).get(key_str, {})

    def batch_get(self, requests: list) -> list[dict[str, Any]]:
        out = []
        for r in requests:
            if isinstance(r, dict):
                out.append(self.get(r["table"], r["key"]))
            else:  # tuple-like
                out.append(self.get(r[0], r[1]))
        return out

    def reset(self) -> None:
        self._tables.clear()
        self._agg_state.clear()

    def ping(self) -> dict:
        return {
            "server_version": "0.0.0-mock",
            "registry_version": self._registry_version,
        }

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def App(*args, **kwargs):
    """Phase 13.0 stub. Replaced by `from beava import App` in Phase 13.5."""
    return MockApp()


# --- Demo helpers -- describe descriptors as plain Python objects.
# In real bv.App, these would be @bv.event / @bv.table decorators.


def event(name: str) -> _Descriptor:
    return _Descriptor(name=name, kind="event", source=None, key_cols=[], ops={})


def table(
    name: str,
    *,
    source: str,
    key: list[str] | str,
    ops: dict[str, tuple[str, str | None]],
) -> _Descriptor:
    """Build a table descriptor.

    ops: dict feature_name -> (op_name, field) -- e.g.
         {"tx_count_1h": ("count", None), "tx_sum_1h": ("sum", "amount")}
    """
    key_cols = [key] if isinstance(key, str) else list(key)
    op_specs = {
        feat: _AggSpec(op=op_name, field=field)
        for feat, (op_name, field) in ops.items()
    }
    return _Descriptor(
        name=name, kind="table", source=source, key_cols=key_cols, ops=op_specs
    )
