"""
Mock backend for runnable demos.

In-memory App shim that computes minimal aggregations on push so demos go
through the full register -> push -> query flow. Drop-in for `bv.App()`;
swap to the real SDK by replacing the import in each demo.

Supported ops (the minimum needed by the 9 vertical demos):
    count, sum, mean, min, max

Sketches (n_unique, quantile, top_k), decays (ewma, ewvar, ew_zscore,
decayed_*), velocity (rate_of_change, trend, ...), and geo
(geo_velocity, geo_distance, ...) are NOT computed here -- demos that
reference these ops surface them as no-ops, with the real engine
filling the gap.
"""
from typing import Any


class _AggSpec:
    __slots__ = ("op", "field", "where")

    def __init__(self, op: str, field: str | None, where: dict | None = None):
        self.op = op
        self.field = field
        self.where = where


class _Descriptor:
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
        self.kind = kind
        self.source = source
        self.key_cols = key_cols
        self.ops = ops


class MockApp:
    """In-memory drop-in for `bv.App()`.

    Computes count / sum / mean / min / max on push. Other ops are no-ops;
    demos pick coverage-aware features for assertions.
    """

    def __init__(self):
        self._registered: list[_Descriptor] = []
        self._tables: dict[str, dict[str, dict[str, Any]]] = {}
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
                # Tolerate alternate descriptor shapes a real SDK may produce
                # (decorated classes, dicts) so the mock stays drop-in.
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
            # Sketches, decays, geo, etc.: no-op in the mock; real engine fills.
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
            else:
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
    """Drop-in for `bv.App()`; swap to `from beava import App` for the real SDK."""
    return MockApp()


# Demo helpers -- describe descriptors as plain Python objects.
# In real bv.App these would be `@bv.event` / `@bv.table` decorators.


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

    `ops` maps feature name to `(op_name, field)` -- e.g.
    `{"tx_count_1h": ("count", None), "tx_sum_1h": ("sum", "amount")}`.
    """
    key_cols = [key] if isinstance(key, str) else list(key)
    op_specs = {
        feat: _AggSpec(op=op_name, field=field)
        for feat, (op_name, field) in ops.items()
    }
    return _Descriptor(
        name=name, kind="table", source=source, key_cols=key_cols, ops=op_specs
    )
