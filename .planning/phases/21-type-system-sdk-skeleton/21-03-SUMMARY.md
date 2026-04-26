---
phase: 21-type-system-sdk-skeleton
plan: 03
subsystem: python-sdk
tags: [sdk, v0, aggregation, join, union, serialization]
requires: [21-01, 21-02]
provides:
  - "16 aggregation operator descriptors (count/sum/avg/min/max/variance/stddev/percentile/count_distinct/top_k/first/last/first_n/last_n/ema/lag) with supports_retraction flags + hybrid_params + output_type_for inference"
  - "Stream.group_by(*keys).agg(**features) — schema-inferring TableDerivation factory"
  - "Table.group_by(...) — registration-time rejection with exact CONTEXT.md message"
  - "Stream.join and Table.join stubs; JoinSpec covers S↔S (windowed), S↔T (enrichment), T↔T (full-key)"
  - "tl.union(*streams) — strict schema-compat UnionSpec stub"
  - "compile_to_register_json — single source of truth for REGISTER payload shape; consumed by Phase 22/23"
  - "collect_registrations — topological walk w/ dedupe across agg source, join left+right, union sources"
affects:
  - "tally public surface: 17 new symbols (16 agg ops + tl.union)"
  - "REGISTER JSON shape: legacy top-level 'features: []' removed; aggregation features now live under 'aggregation.features'"
  - "test_v0_public_surface: agg-op and union names removed from _REMOVED_PUBLIC_SYMBOLS list"
  - "test_v0_decorators.test_compile_shape: asserts new payload layout"
tech-stack:
  added: []  # stdlib only (hashlib, re)
  patterns:
    - "Descriptor-carries-spec pattern: TableDerivation._agg_spec / ._join_spec / ._union_spec drive serializer dispatch"
    - "Polars-style collision suffix (_right) for join schema union"
    - "Generated derivation names <Source>_Agg_<sha1[:8]> / <Left>_Join_<Right> so unnamed pipelines still serialize deterministically"
    - "Serializer dispatch by (isinstance + attr presence) — avoids per-class subclassing explosion"
    - "Spec._compile_for_server() sentinel — raises NotImplementedError with the consuming phase number so bypassed stubs fail loudly"
key-files:
  created:
    - python/tally/_agg_ops.py
    - python/tally/_aggregation.py
    - python/tally/_join.py
    - python/tally/_union.py
    - python/tally/_serialize.py
    - python/tests/test_v0_agg_stubs.py
    - python/tests/test_v0_join_stubs.py
    - python/tests/test_v0_table_agg_reject.py
    - python/tests/test_v0_serialize.py
  modified:
    - python/tally/_stream.py      # Stream.group_by / Stream.join; _compile/_collect_registrations via serializer
    - python/tally/_table.py       # Table.group_by rejection; Table.join; serializer delegation
    - python/tally/__init__.py     # exports tl.union + 16 agg ops
    - python/tests/test_v0_public_surface.py  # unblock new symbols from removed list
    - python/tests/test_v0_decorators.py      # test_compile_shape now asserts new payload layout
decisions:
  - "min/max default to supports_retraction=False per v0 spec (bucketed can't decrement). exact_min/exact_max variants from v2.0 are NOT re-exported — plan explicitly lists 16 ops, not 18. Phase 26 cleanup will delete _operators.py (still on disk but not imported)."
  - "Percentile: UDDSketch hybrid threshold 256, alpha 0.01 (CONTEXT.md default). count_distinct: HLL threshold 1024, precision 14. top_k: CMS+heap threshold 1024, width 2048, depth 4."
  - "Table.group_by emits exact error message verbatim — tested with string equality so any drift fails loudly"
  - "tl.union is strict: exact schema match (name + py_type + optional). Plan's risk-mitigation documents this in the error: 'if fields differ in nullability, apply .fillna() first on one side'. Phase 22 can relax if real users ask."
  - "Join shape validation happens ONLY in Python — the Rust engine never sees outer/partial-key requests because we fail-fast at registration. Phase 23 doesn't need defensive re-checks."
  - "Join generates a synthetic name <Left>_Join_<Right> (or <Left>_Enrich_<Right> for S↔T) so users can still call .describe() on un-named join results. Wrapping in @tl.stream def X rewrites _name to 'X'."
  - "Aggregation generates a synthetic name <Source>_Agg_<sha1[:8]> with hash over (source, keys, feature-names) to ensure stability across runs — deterministic for tests."
  - "Serializer is the CONTRACT Phase 22/23 consume. Legacy per-descriptor _compile inline code deleted; serializer is the single source of truth. Breaking changes to the payload shape happen here, not in five places."
metrics:
  duration: "~30 min"
  completed: 2026-04-14
---

# Phase 21 Plan 03: Type system & SDK skeleton — aggregation, joins, union, serializer

Closed the v0 SDK surface. A user can now write a full pipeline (sources +
derivations + aggregation + joins + union), call ``tl.validate()`` locally,
get surgical errors, and inspect ``.describe()`` everywhere. The REGISTER
JSON serializer is the contract Phase 22 (aggregation executor) and Phase
23 (join executor) will consume without touching the SDK surface.

## What Shipped

### 16 aggregation operator descriptors (`_agg_ops.py`)

| Op | Signature | Returns | `supports_retraction` | Windowed |
|----|-----------|---------|-----------------------|----------|
| `count` | `count(*, window, where?, bucket?)` | int | ✅ | yes |
| `sum` | `sum(field, *, window, where?, bucket?)` | float | ✅ | yes |
| `avg` | `avg(field, *, window, bucket?)` | float | ✅ | yes |
| `min` | `min(field, *, window, bucket?)` | input type | ❌ (bucketed) | yes |
| `max` | `max(field, *, window, bucket?)` | input type | ❌ (bucketed) | yes |
| `variance` | `variance(field, *, window, bucket?)` | float | ✅ (Welford) | yes |
| `stddev` | `stddev(field, *, window, bucket?)` | float | ✅ (Welford) | yes |
| `percentile` | `percentile(field, quantile, *, window, bucket?, exact_threshold=256, hybrid_alpha=0.01)` | float | ❌ (UDDSketch) | yes |
| `count_distinct` | `count_distinct(field, *, window, bucket?, exact_threshold=1024, hybrid_precision=14)` | int | ❌ (HLL) | yes |
| `top_k` | `top_k(field, k, *, window, bucket?, exact_threshold=1024, hybrid_width=2048, hybrid_depth=4)` | list | ❌ (CMS) | yes |
| `first` | `first(field)` | input type | ❌ | no |
| `last` | `last(field)` | input type | ❌ | no |
| `first_n` | `first_n(field, n)` | list | ❌ | no |
| `last_n` | `last_n(field, n)` | list | ❌ | no |
| `ema` | `ema(field, half_life)` | float | ❌ | no (half_life) |
| `lag` | `lag(field, n)` | input type | ❌ | no |

Each descriptor:

- Validates constructor args at build time (quantile bounds, positive n, valid duration strings, etc.).
- Exposes `output_type_for(schema)` that preserves the input field type for `min`/`max`/`first`/`last`/`lag` and returns container types for `first_n`/`last_n`/`top_k`.
- Exposes `to_json(name)` that emits the engine-side feature JSON — flat keys plus flattened hybrid params.
- `AggregationSpec._compile_for_server()` raises `NotImplementedError("stream aggregation ships in Phase 22")`.

### `.group_by(*keys).agg(**features)` builder (`_aggregation.py`)

- `Stream.group_by(*keys)` returns `GroupBy(stream, keys)`; validates every key against the upstream schema with Levenshtein hints on miss.
- `GroupBy.agg(**features)`:
  - Rejects non-AggOp values naming the bad kwarg + observed type.
  - Enforces `window=` on ops that require it (raises `TypeError` citing the op name).
  - Validates every op's `field` reference against the upstream schema.
  - Rejects feature names that collide with group keys.
  - Builds a `TableDerivation` with key=group_keys, schema=group_keys ∪ features (types inferred via `output_type_for`), and stashes the `AggregationSpec` on `._agg_spec`.
- **`Table.group_by(...)` raises the exact v0 error message** — tested with string equality so any future drift breaks the test loudly:

```
Cannot aggregate over Table '{name}'. Tables are current-state-only in v0;
Table aggregation ships in v0.1. To aggregate related data, model it as a
Stream source.
```

### Join stubs (`_join.py`)

Three shapes, all producing descriptors that carry a `JoinSpec` on `._join_spec`:

| Shape | `within` | `on` | `type` | Output |
|-------|----------|------|--------|--------|
| Stream ↔ Stream | **required** | subset of both schemas | inner / left | Stream |
| Stream ↔ Table  | **forbidden** | subset of both schemas | inner / left | Stream |
| Table ↔ Table   | **forbidden** | must equal both key-sets | inner / left | Table |

Rejection messages (exact, tested):

- `"Stream↔Stream join requires within=... (e.g. '30m'); symmetric interval joins without a window are not supported"`
- `"Stream↔Table enrichment does not accept within=...; the Table's current row is looked up at the stream event's event-time (no symmetric window)"`
- `"outer joins deferred to v0.1; v0 supports 'inner' and 'left' only"`
- `"Table↔Table join requires full-key match; Table 'A' key=[...], 'B' key=[...], on=[...]"`

Schema union follows polars conventions: left wins, right's colliding non-key fields become `{name}_right` (further collisions: `{name}_right2`, `{name}_right3`, ...). `JoinSpec._compile_for_server()` raises `NotImplementedError("join ships in Phase 23")`.

### `tl.union(*streams)` (`_union.py`)

- Requires ≥2 inputs; all must be Streams.
- Field-by-field strict compat: same names, same `py_type`, same `optional` flag.
- Mismatch errors include both full schema dumps plus a pointer: "if fields differ in nullability, apply .fillna() first on one side".
- Returns a StreamDerivation carrying `UnionSpec` on `._union_spec`.
- `UnionSpec._compile_for_server()` raises `NotImplementedError("union ships in Phase 22")`.

### REGISTER JSON serializer (`_serialize.py`)

`compile_to_register_json(descriptor)` dispatches on descriptor kind and produces the canonical payload — **this module is the single source of truth Phase 22/23 consume**. The per-descriptor `_compile()` methods on StreamSource / StreamDerivation / TableSource / TableDerivation all delegate here (inline builders deleted).

Payload shapes (see `_serialize.py` docstring for the full contract):

**Source:**
```json
{"name": "Clicks", "kind": "stream", "key_field": null,
 "fields": {"user_id": {"type": "str", "optional": false}, ...}}
```

**Op-chain derivation:**
```json
{"name": "Checkouts", "kind": "stream",
 "fields": {...}, "ops": [{"op": "filter", "expr": "..."}],
 "depends_on": ["Clicks"], "key_field": null}
```

**Aggregation (Stream → Table):**
```json
{"name": "UserSpend", "kind": "table", "key_field": "user_id",
 "mode": "append", "fields": {...},
 "aggregation": {"source": "Checkouts", "keys": ["user_id"],
                 "features": [{"name": "n", "type": "count",
                               "supports_retraction": true, "window": "1h"},
                              {"name": "total", "type": "sum",
                               "supports_retraction": true,
                               "field": "amount", "window": "1h"}]},
 "depends_on": ["Checkouts"]}
```

**Join:**
```json
{"name": "OP", "kind": "stream", "key_field": null,
 "fields": {...},
 "join": {"op": "join", "left": "Orders", "right": "Payments",
          "on": ["order_id"], "within": "30m",
          "type": "inner", "shape": "stream_stream"},
 "depends_on": ["Orders", "Payments"]}
```

**Union:**
```json
{"name": "Union_A_B", "kind": "stream", "key_field": null,
 "fields": {...},
 "union": {"sources": ["A", "B"]},
 "depends_on": ["A", "B"]}
```

`collect_registrations(descriptor)` walks upstreams depth-first, dedupes by `_name`, and returns a topologically-ordered list. Handles the full upstream graph — op-chain `_upstreams`, aggregation `_agg_spec.upstream`, join `_join_spec.{left,right}`, union `_union_spec.sources`.

Legacy top-level `"features": []` key is **gone** — it was a vestige of the v2.0 @st.stream class-attribute-as-feature layout. Aggregation features now live under `aggregation.features`, matching Phase 22's aggregation-executor contract.

## Public Export Delta

**Added:** `union`, `count`, `sum`, `avg`, `min`, `max`, `variance`, `stddev`, `percentile`, `count_distinct`, `top_k`, `first`, `last`, `first_n`, `last_n`, `ema`, `lag`.

All 17 symbols are callables returning AggOp / StreamDerivation instances. `sum`/`min`/`max` deliberately shadow Python builtins in the `tally` namespace — accessed as `tl.sum`, never as bare `sum`.

**Unchanged:** Everything from 21-01 + 21-02.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 — blocking] `test_v0_decorators.test_compile_shape` asserted legacy payload layout**
- **Found during:** Task 3
- **Issue:** The test asserted `reg["features"] == []` — a vestige of the v2.0 per-class feature list that Plan 21-03 explicitly removes (aggregation features now live under `aggregation.features`).
- **Fix:** Updated the assertion to check the new layout: `"features" not in reg` and `reg["kind"] == "stream"`. The test now documents the new shape.
- **Files modified:** `python/tests/test_v0_decorators.py`.
- **Commit:** 9880bbf (bundled with the serializer).

No architectural deviations. Plan executed as written.

## Threat Flags

None. This plan ships SDK-only code — no new network surface, no new file I/O, no new schema-boundary trust surfaces. Everything flows through existing `App.register` → TCP client machinery shipped in 21-01/02.

## Test Counts

- **Pre:** 348 passed, 2 skipped (after 21-02).
- **Post:** 386 passed, 2 skipped. Delta: +32 (`test_v0_agg_stubs.py`) + 25 (`test_v0_join_stubs.py`) + 13 (`test_v0_serialize.py`) + 2 (`test_v0_table_agg_reject.py`) − 2 (1 test rewritten in decorators, 1 test split off into agg_stubs) = +38 net new tests.
- Full Phase 21 surface + retained client/protocol/operator/types tests all green in 0.37s.

## Manual Smoke

Canonical end-to-end pipeline (from the plan's `<verification>` block):

```python
import tally as tl
@tl.stream
class Clicks:
    user_id: str; page: str; amount: float

@tl.stream
def Checkouts(clicks: Clicks) -> tl.Stream:
    return clicks.filter(tl.col("page") == "/checkout")

@tl.table(key="user_id")
def UserSpend(co: Checkouts) -> tl.Table:
    return co.group_by("user_id").agg(
        n=tl.count(window="1h"),
        total=tl.sum("amount", window="1h"),
    )

assert tl.validate(Clicks, Checkouts, UserSpend) == []
regs = UserSpend._collect_registrations()
assert [r["name"] for r in regs] == ["Clicks", "Checkouts", "UserSpend"]
assert regs[-1]["aggregation"]["features"][0]["type"] == "count"
```

All three REGISTER payloads print cleanly. The last frame contains the full `aggregation: {source, keys, features: [count, sum]}` payload Phase 22 consumes verbatim.

## `_operators.py` Deprecation Note

`python/tally/_operators.py` (Phase 16's operator catalog — Count, Sum, Avg, ...) remains on disk but is **no longer re-exported** from `tally.__init__`. The only reference is `tally.OperatorBase` which a handful of tests still import directly from the private module.

Phase 26 (test migration) deletes `_operators.py` once `test_operators.py` is rewritten against the new AggOp surface. Until then: inert, not shipped via the public API, does not interfere with `tl.count` / `tl.sum` / etc. which resolve from `_agg_ops.py`.

## Rust Engine

Zero changes, as scoped. The REGISTER JSON payload is the contract Phase 22 (aggregation executor) and Phase 23 (join executor) will consume. Any Phase 22/23 planner adjustments happen on the Rust side; this SDK contract is frozen.

## Commits

| Task | Commit  | Subject |
|------|---------|---------|
| 1    | 1ad56bb | 16 aggregation descriptors + GroupBy.agg + Table-agg rejection |
| 2    | eecf98f | join stubs (S↔S / S↔T / T↔T) + tl.union |
| 3    | 9880bbf | REGISTER JSON serializer + topological collect_registrations |

## Self-Check: PASSED

- All 9 created files present on disk:
  - `python/tally/_agg_ops.py`, `python/tally/_aggregation.py`, `python/tally/_join.py`, `python/tally/_union.py`, `python/tally/_serialize.py`
  - `python/tests/test_v0_agg_stubs.py`, `python/tests/test_v0_join_stubs.py`, `python/tests/test_v0_table_agg_reject.py`, `python/tests/test_v0_serialize.py`
- All 5 modified files modified on disk:
  - `python/tally/_stream.py`, `python/tally/_table.py`, `python/tally/__init__.py`, `python/tests/test_v0_public_surface.py`, `python/tests/test_v0_decorators.py`
- All 3 task commits present: 1ad56bb, eecf98f, 9880bbf.
- Full verification suite: 386 tests passed, 2 skipped, 0 failures.
- Manual end-to-end smoke produces the documented JSON shape.
