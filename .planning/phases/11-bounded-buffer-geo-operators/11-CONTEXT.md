# Phase 11: Bounded-buffer + geo operators — Context

**Gathered:** 2026-04-23
**Mode:** Auto (orchestrator-driven; --auto chain). Discuss step abbreviated; decisions captured below.
**Branch:** `worktree-agent-a71d2569` (forked from `v2/greenfield@157630f`)

## Phase Boundary

Land 13 operators across two families that compose against the existing
Phase 5 aggregation framework:

- **Bounded-buffer (AGG-BUFFER-01..07):** `histogram`, `hour_of_day_histogram`,
  `dow_hour_histogram`, `seasonal_deviation`, `event_type_mix`, `most_recent_n`,
  `reservoir_sample`. Outputs are typically dicts or lists.
- **Geo (AGG-GEO-01..06):** `geo_velocity`, `geo_distance`, `geo_spread`,
  `unique_cells`, `geo_entropy`, `distance_from_home`. Outputs are scalars
  except `unique_cells` (int).

Out of scope: SDK-level Python decorator wiring (server-side compile + apply
+ get is the only requirement; SDK `@bv.histogram(...)` syntax is not load-
bearing for v0 launch and is deferred to Phase 13 polish if needed). HTTP
JSON I/O for structured outputs is required (success criterion 3).

## Decisions

### D-01 — Value enum gains `List(Vec<Value>)` + `Map(BTreeMap<String, Value>)`

Histograms return `{bucket_label: count}` and `most_recent_n` / `reservoir_sample`
return lists. The v0 `Value` enum only has scalars. We add two recursive
variants: `Value::List(Vec<Value>)` and `Value::Map(BTreeMap<String, Value>)`.

**Rationale:** Cheaper than introducing a separate `StructuredValue` type;
keeps the `AggOp::query() -> Value` signature stable. Recursion is bounded by
the operator (histograms produce flat maps; lists hold scalars only at the
operator level — no nested structures created inside the impls).

**Implication:**
- `value_to_json` in `feature_query.rs` and `registry_debug.rs` must encode
  `List` → JSON array, `Map` → JSON object.
- `FieldType` does not need new variants — these values never appear in event
  rows or table rows; only as aggregation outputs. (No `type_of()` mapping.)
- `Value::PartialEq` extends naturally (recursive equality).
- Bytes already serialise as `Null` in JSON; lists/maps serialise as themselves.

### D-02 — Geo deps: `haversine` (cite) for great-circle distance only; geohash via hand-rolled grid

`haversine` crate is a 1-dep, ~20-line pure-Rust formula. Adding it:

```toml
haversine = "0.2"
```

provides `Distance::haversine(p1, p2, units::Kilometers)` and removes the need
for any hand-derived numeric implementation. Cited in this CONTEXT for SC2.

For `unique_cells` and `geo_entropy`, h3o is heavyweight (~2 MB pure Rust). The
prompt allows "geohash" or "H3" via h3o. **Decision: use a simple equirectangular
grid cell encoding `(floor(lat / step), floor(lon / step))` with `precision`
controlling `step`** — this avoids adding a 2 MB dep for v0, satisfies AGG-GEO-04/05
("distinct geohash cells visited" / "Shannon entropy over cell distribution"),
and keeps the dependency surface tiny. h3o can swap in later if precision matters.

`precision` is interpreted as: `step_degrees = 1.0 / precision`. So `precision=10`
≈ ~11 km cells at the equator; `precision=100` ≈ ~1.1 km cells. Documented in
operator doctests.

### D-03 — `distance_from_home` uses centroid-of-last-N fallback (top_k from Phase 10 not in worktree)

Per orchestrator instructions: top_k is not yet available in this worktree.
Implement `distance_from_home(samples=N)` as:

> distance from current event lat/lon to the running centroid (mean lat, mean
> lon) of the last N events seen for this entity.

This uses a circular `Vec<(f64, f64)>` of size `samples` (overwrite-oldest)
and recomputes the centroid on query. v0.1 follow-up: swap to top-K most-
frequent-cell centroid once Phase 10 lands.

### D-04 — Bucket layout for histograms: register-time fixed Vec<u64>

`bv.histogram(field, buckets=[10, 20, 50, 100], window=...)` declares N+1 cells:
`[(-inf, 10), [10, 20), [20, 50), [50, 100), [100, inf)]` → output keys are
`"<10"`, `"10-20"`, `"20-50"`, `"50-100"`, `">=100"`.

`hour_of_day_histogram` = 24 buckets keyed `"00".."23"`.
`dow_hour_histogram` = 168 buckets keyed `"Mon-00".."Sun-23"`.

Bucket index lookup is binary search over the `buckets[]` array (sorted at
register time). For windowed histograms we wrap each bucket count in the
existing `WindowedOp` infrastructure? **No** — histogram buckets are an
inner dimension, not the same axis as event-time buckets. We simply hold
`Vec<u64>` per histogram-cell across all time. Windowing is deferred to v1
for histograms (documented). Most fraud use cases want lifetime histograms
anyway.

`hour_of_day_histogram` and `dow_hour_histogram` derive bucket index from
`event_time_ms` directly via `chrono`-style arithmetic — added inline (no
chrono dep): `(event_time_ms / 3_600_000) % 24` for hour-of-day,
`((event_time_ms / 86_400_000 + 4) % 7) * 24 + hour` for dow-hour
(Unix epoch = Thursday → +4 to align Mon=0).

### D-05 — Seasonal_deviation: store `(count, sum, sum_sq)` per hour bucket

24-hour hour-of-day baseline. On update: increment count/sum/sum_sq for the
current hour bucket of the event. On query: return `(observed - bucket_mean)
/ bucket_stddev` for the **current hour** of the most-recently-seen event,
or `Null` if `count < 2` for that bucket. The "observed" value is the
event-field value of the most recent event that updated this op. Store the
last `(field_value, hour_bucket)` so query is deterministic.

### D-06 — `most_recent_n` storage: circular Vec, overwrite oldest

`MostRecentNState { buf: Vec<Value>, head: usize, n: usize }`. Head wraps mod n.
Output is the buf in insertion order (oldest → newest). Snapshots serialize the
struct.

### D-07 — `reservoir_sample`: Algorithm R (Vitter 1985) — but D-06 forbids `rand`

The CLAUDE.md determinism guard explicitly bans `rand::` usage. Implement
Algorithm R using a deterministic PRNG seeded by the entity_key + items_seen
counter — `wyhash` is a tiny pure-Rust hash (no dep needed; we already have
`ahash`-style hashers in scope but not wired). **Decision:** roll a tiny
inline xorshift64 PRNG seeded from `(items_seen, items_seen.wrapping_mul(0x9E37_79B9_7F4A_7C15))`
for replay determinism. Same input event sequence → same reservoir.

### D-08 — All ops are lifetime (no window) in v0

To keep this phase scoped, every Phase 11 operator is windowless. The
`window=...` kwarg in REQUIREMENTS is acknowledged but compiler will reject
windowed variants for these operators with `op_window_not_supported_v0`
(future v0.1 work). This avoids forcing buffer/geo state through the 64-bucket
WindowedOp and keeps each op's state shape minimal.

### D-09 — Throughput run: HTTP-only; new "geo-recommendations" pipeline variant

Phase 8 sibling has not landed TCP push, so throughput row records HTTP only.
Add a 4th pipeline shape ("geo") to `crates/beava-bench` that exercises
`geo_velocity` + `unique_cells` + `most_recent_n` to verify a geo-shape workload
doesn't regress simple-fraud > 25%. Result row goes to
`.planning/phases/11-bounded-buffer-geo-operators/11-throughput-row.md`
(NOT canonical ledger — orchestrator instruction).

### D-10 — Bench: one criterion file `phase11_buffer_geo.rs` covering update hot paths

8 representative bench IDs (subset of 13 ops, one per state-shape archetype):
`histogram/update`, `hour_of_day_histogram/update`, `seasonal_deviation/update`,
`most_recent_n/update`, `reservoir_sample/update`, `geo_velocity/update`,
`unique_cells/update`, `distance_from_home/update`. Per-bench row → 
`.planning/phases/11-bounded-buffer-geo-operators/11-perf-row.md`.

## Execution shape

- **Plan 01:** Value::List + Value::Map + JSON encoder (red→green).
- **Plan 02:** 7 bounded-buffer operator state types + AggKind variants + compile parsers + per-op tests + apply integration test.
- **Plan 03:** 6 geo operator state types + add `haversine` dep + same shape as Plan 02. Includes `unique_cells`, `geo_entropy`, `distance_from_home`.
- **Plan 04:** Bench file + throughput row + smoke test through GET /get/{feature}/{key} (verifying structured JSON envelope) + VERIFICATION.md + SUMMARY.md.

