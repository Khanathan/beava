# Phase 11 — Bounded-buffer + geo operators — Summary

**Shipped:** 2026-04-23 (resume session 2026-04-24)
**Branch:** `worktree-agent-a71d2569` (forked from `v2/greenfield@157630f`)
**Status:** **passed** (modulo pre-existing cli_smoke flake — see Caveats)

## What landed

13 operators across two families, fully wired through the
`AggOp::update` → state → query path, exposed via the JSON-declarative
register API and queryable via `GET /get/{feature}/{key}` with structured
JSON envelopes (lists for `most_recent_n` / `reservoir_sample`; maps for
all histograms and `event_type_mix`).

### Bounded-buffer family (7)

| op                          | output type | state shape                               | notes |
|-----------------------------|-------------|-------------------------------------------|-------|
| `histogram`                 | Map         | `Vec<u64>` over fixed bucket cells        | `<10`, `10-100`, `>=100` keys |
| `hour_of_day_histogram`     | Map         | `[u64; 24]`                                | keyed `"00".."23"` from `event_time_ms` |
| `dow_hour_histogram`        | Map         | `[u64; 168]`                               | keyed `"Mon-00".."Sun-23"` |
| `seasonal_deviation`        | F64 \| Null | per-hour `(count, sum, sum_sq)`           | (observed - bucket_mean)/bucket_stddev for last event's hour |
| `event_type_mix`            | Map         | `BTreeMap<String, u64>` + total            | shares as f64 per category |
| `most_recent_n`             | List        | circular `Vec<Value>` size `n`             | overwrite-oldest |
| `reservoir_sample`          | List        | `Vec<Value>` size `k` + items_seen counter | Algorithm R; deterministic xorshift64 PRNG (D-07) — no `rand::` |

### Geo family (6)

| op                          | output type | state shape                               | notes |
|-----------------------------|-------------|-------------------------------------------|-------|
| `geo_velocity`              | F64         | last `(t_ms, lat, lon)`                    | km/h via `haversine` |
| `geo_distance`              | F64         | last `(lat, lon)` + cumulative km         | total path length |
| `geo_spread`                | F64         | min/max lat/lon bounding-box              | bbox diagonal in km |
| `unique_cells`              | I64         | `BTreeSet<(i64,i64)>`                     | equirectangular grid `step = 1/precision` |
| `geo_entropy`               | F64         | `BTreeMap<(i64,i64), u64>` + total        | Shannon entropy in nats |
| `distance_from_home`        | F64         | circular `Vec<(lat,lon)>` size `samples`  | distance to centroid (CONTEXT D-03 fallback) |

> **`distance_from_home` ships with the centroid-of-last-N fallback.**
> Phase 10's `top_k` is not in this worktree; the requirement
> ("distance from frequent location") is approximated by the centroid
> of the last `samples` events. v0.1 follow-up: swap to top-K
> most-frequent-cell centroid once Phase 10 lands.

## Architecture

- **`Value` enum gained two variants** (`Value::List(Vec<Value>)`, `Value::Map(BTreeMap<String,Value>)`) per CONTEXT D-01. `value_to_json` in both `feature_query.rs` and `registry_debug.rs` encode List → JSON array, Map → JSON object.
- **All ops are lifetime (windowless) in v0** per D-08. Compiler accepts the existing `op` JSON without a `window` kwarg; windowed variants of these operators are a v0.1 follow-up (no compiler error path was added — windowed call would just be ignored at compile time today, since the parser doesn't read `window` for these ops).
- **Geo dep:** `haversine 0.2` (workspace dep, pure-Rust ~20-line great-circle formula).
  No `h3o` — `unique_cells` and `geo_entropy` use a hand-rolled equirectangular
  grid `(floor(lat * precision), floor(lon * precision))` per CONTEXT D-02. h3o
  can swap in later if real-world precision matters; for v0 the grid avoids a
  ~2 MB dep.
- **Reservoir RNG is deterministic** (xorshift64 seeded from `(items_seen, splittable-mix-multiplier)`) — same input event sequence yields the same reservoir on replay. SC4 verified by `phase11_smoke::replay_determinism_across_two_runs`.

## Performance

### Per-op microbench (`crates/beava-core/benches/phase11_buffer_geo.rs`, 12 IDs)

Buffer ops 1–8 ns; geo ops 12–24 ns. Cheapest: `hour_of_day_histogram` (1.05 ns). Most expensive: `geo_velocity` (24.28 ns) — haversine math dominates. See `11-perf-row.md`.

### End-to-end throughput

- **Geo pipeline (new shape):** 701 EPS HTTP, 8-way parallel, 30s.
- **Small pipeline (regression check):** 1097 EPS HTTP vs Phase 7.5 baseline 990 EPS = **+10.8% improvement**.

Full row data in `11-throughput-row.md`. Per orchestrator instruction these
rows are NOT appended to canonical `.planning/throughput-baselines.md`
(which is treated as the milestone-shipped ledger).

## Test count delta

624 (Phase 7.5) → 659 = **+35 tests** (per-op unit tests + smoke + replay-determinism + bench fixtures).

## Caveats / known issues

1. **`cli_smoke` env_var + banner tests fail consistently** in this resume session. **Verified pre-existing on parent commit `157630f`** — the 300ms post-spawn sleep is too short for the dev binary to write its banner before the test sends SIGTERM, on this macOS hardware under heavy concurrent compile load. Not caused by Phase 11. Suggested fix (carry-over follow-up): poll for banner output instead of sleeping fixed duration.

2. **`distance_from_home` uses centroid fallback** instead of top-K — see CONTEXT D-03. Document follow-up for after Phase 10 ships.

3. **TDD trace gap on Plan 02/03:** state-types and per-op tests landed in a single commit (`7d6271f`) rather than separate red/green commits because the unit tests live alongside each state struct in the same source file. CONTEXT.md was committed first; the inferred per-plan PLAN.md files were never written by the prior agent and are recorded post-hoc in `11-PHASE-STATUS.md`. Future phases should structure red/green per op.

## Open follow-ups

- Replace `distance_from_home`'s centroid fallback with top-K once Phase 10 merges.
- Windowed variants for histograms / event_type_mix (v0.1 polish).
- Real-coord workload variant for `crates/beava-bench` geo pipeline (current uses `f64 ∈ [0, 1000)` randoms — exercises math but not realistic distances).
- cli_smoke harness flake fix (poll for banner instead of fixed sleep).
