# Phase 22: Stream aggregation engine - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Auto-generated from v0 design conversation + Phase 21 artifacts

<domain>
## Phase Boundary

Implement the Rust engine side of Stream aggregation — consume the REGISTER JSON payload that Phase 21's `_serialize.py` produces, and execute `group_by(keys).agg(features)` on Stream inputs to produce a live keyed Table. All 16 aggregation operators (from 21-03's descriptors) fully functional. Ring-buffer windowing with configurable bucket granularity. Hybrid exact → sketch transitions for percentile (UDDSketch), count_distinct (HLL), top_k (CMS+heap). Benchmarks verify no regression vs v2.0 baseline.

Critical contract: the Rust engine must consume the REGISTER JSON exactly as serialized by `/data/home/tally/python/tally/_serialize.py`. That contract was frozen in Phase 21-03; any changes here need round-tripping to Python. Phase 23 (joins) and Phase 24 (watermarks) build on this aggregation engine.

**Out of scope:**
- Table-input aggregation — deferred to v0.1 (registration error raised in Phase 21-03 before engine is hit)
- Joins — Phase 23
- Watermarks / event-time — Phase 24
- TTL/warnings telemetry for aggregation — Phase 25
- Test migration — Phase 26

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Operator catalog — 16 aggregation operators

All 16 ship in this phase. Per-operator config comes from Phase 21's AggOp descriptors:

**Linear operators (cheap, always cheap retract):**
- count — bucketed counter, O(1) state per key
- sum — bucketed accumulator
- avg — sum + count pair
- min, max — bucket-granular default (for Stream with window); full-state opt-in if retraction flows through (but no retraction in v0 since Table aggregation is disabled — so bucket-granular always)
- variance, stddev — Welford running stats per bucket

**Sketch operators (hybrid exact → sketch at threshold):**
- percentile — exact sorted Vec up to `exact_threshold=256` events, then UDDSketch (vendored from Timescale Rust port + add `decrement()` method, α=0.005, max_bins=256)
- count_distinct — exact HashSet up to `exact_threshold=1024` unique values, then HLL precision=14
- top_k — exact HashMap<Value, count> up to `exact_threshold=1024` unique values, then CMS+heap (CMS w=2048, d=4; heap tracks top-k candidates with counts from CMS)

**Order-sensitive operators:**
- first (by event-time): value + event_time, tracks earliest
- last (by event-time): value + event_time, tracks most recent
- first_n, last_n: bounded ring buffer of size N

**Path-dependent operators:**
- ema — exponential moving average with α parameter
- lag — value from N events ago

### Windowing

- Sliding window via bucketed ring buffer (existing pattern from v2.0 in `src/engine/window.rs`)
- Default bucket granularity: 1 minute for windows ≥ 1h, 1 second for windows < 1h
- User-configurable via aggregation spec (`window="1h"`, optional `bucket_granularity="1m"`)
- Ring buffer bucket index = event_time / bucket_granularity (Phase 24 will add event-time semantics; for now, use wall-clock fallback)
- On bucket expiry: discard bucket state (for sketches, discard the whole sketch-per-bucket if using Model A, or for single-sketch-per-window approach, tombstone at bucket boundaries)

### Sketch memory model — one sketch per entity per feature, NOT per-bucket

Based on research (`ddsketch-retract-verification.md`, `uddsketch-retract-verification.md`, `retraction-literature-survey.md`):
- **Model B** (single sketch per window, retract on bucket expiry via UDDSketch decrement)
- **Not Model A** (per-bucket sketches) — Model A would cost 60× memory for typical windows
- Per-entity per-feature memory: ~2-4 KB for sketch operators

For hybrid operators:
- Start in exact mode (sorted Vec / HashSet / HashMap)
- Transition to sketch when exceeding `exact_threshold`
- Transition is one-way (exact → sketch). Copy all exact values into sketch at transition; continue in sketch mode.
- Expose current mode + sketch α in `/debug/key/:key`

### Ring buffer retraction semantics (without Table aggregation)

Since Table-input aggregation is disabled in v0, the only retraction source is **bucket expiry** (events aging out of the window).

For Stream-input aggregations:
- Events arrive, land in the current bucket (wall-clock for Phase 22; event-time-routed in Phase 24)
- Old buckets age out → whole bucket discarded
- For min/max: bucket-granular (discard whole-bucket-min/max on expiry; recompute window min/max from remaining buckets' per-bucket values)
- For sum/count/avg/variance: bucket-level aggregates discarded on expiry; window aggregate recomputed
- For percentile sketches: whole UDDSketch discarded when bucket expires? OR running sketch with per-bucket counts for retract on expiry?

**Decision: per-entity single sketch + retract-on-bucket-expiry.** When a bucket expires, the sketch decrements for each value that was in that bucket. Requires the bucket to remember its values until expiry — one-time cost per bucket.

For exact-mode (before hybrid transition): per-bucket exact list; on expiry, drop the bucket. No retract needed since we have full state.

For sketch-mode (post-hybrid): per-bucket list of values + global UDDSketch; on expiry, iterate bucket values and UDDSketch.decrement(v) for each.

Trade-off: pay per-bucket memory (values stored twice — once in sketch, once in bucket-retention list) until bucket expires, then release. Average case: ~2x memory of pure sketch during window lifetime. Acceptable.

### State store integration

Reuse the existing `EntityState` / `FeatureMap` pattern from v2.0 (see `src/state/store.rs`). Each aggregation feature gets its own entry in `live_features` with operator-specific state.

Extend `OperatorState` enum (`src/engine/operators.rs`) to include the new operator variants:
- `Percentile { mode: PercentileMode, buckets: RingBuffer<Vec<f64>> }` where `PercentileMode = Exact { sorted: Vec<f64> } | Sketch { sketch: UDDSketch }`
- `CountDistinct { mode: CountDistinctMode, buckets: RingBuffer<Vec<Value>> }`  
- `TopK { mode: TopKMode, k: usize, buckets: RingBuffer<Vec<Value>> }`
- (existing Counter, Sum, Avg, Min, Max variants — extend for Welford if needed)
- `Variance { count_buckets: RingBuffer<u64>, mean_buckets: RingBuffer<f64>, m2_buckets: RingBuffer<f64> }` (Welford)
- `Ema { alpha: f64, current: f64 }`
- `Lag { history: VecDeque<Value>, n: usize }`
- `First { value: Value, event_time: Timestamp }`
- `Last { value: Value, event_time: Timestamp }`
- `FirstN { values: Vec<(Timestamp, Value)>, n: usize }`
- `LastN { values: VecDeque<(Timestamp, Value)>, n: usize }`

### UDDSketch implementation

- Vendor `uddsketch` crate from docs.rs (Timescale Rust port) — check `Cargo.toml`
- Extend with `decrement(value: f64)` method — ~50 LOC
- Expose current α via public method for `/debug/key/:key` observability
- Underflow-safe: saturate bucket count at 0 on decrement

### CMS+heap implementation

- Count-Min Sketch: w=2048, d=4 (8192 counters × 4 bytes = 32 KB per top_k feature per entity)
- Top-k heap: binary heap of (estimated_count, value), bounded at k
- On insert: increment CMS for hash(value); update heap if new value's estimated count > heap min
- On retract (bucket expiry): decrement CMS; rebuild heap from CMS queries on currently-known candidates
- Known caveat from research: CMS produces over-estimates; heap is approximate; acceptable for heavy-hitters use case

### HLL implementation

- Precision=14 (16384 buckets × 1 byte = 16 KB per count_distinct feature per entity)
- No retract in HLL mode (literature-confirmed; bucket-granular only)
- Phase 22: bucket-granular retract via per-bucket HLL? Or single HLL + discard on bucket expiry?
  - Simplest: per-bucket mini-HLL (precision=10, 1 KB each × 60 buckets = 60 KB) — discard whole bucket on expiry, merge across buckets for query
  - Research suggests: production systems use sub-window HLLs + merge (vs sliding-HLL literature). Go with per-bucket mini-HLLs, merge on query.

### Integration with existing code

Preserve the existing v2.0 aggregation code where it still makes sense. Check `src/engine/operators.rs` and `src/engine/pipeline.rs` for existing operator implementations — count/sum/avg/min/max likely already work, just need wiring to the new REGISTER JSON.

The v2.0 engine doesn't know about the new operators (variance, stddev, percentile w/ UDDSketch, count_distinct w/ HLL, top_k w/ CMS, ema, lag, first_n, last_n) — these need full Rust impl.

### Performance bar

- No regression vs v2.0 baseline: 1.1M eps sustained (from `19-05-PLAN.md`). Benchmark matrix must pass within -5%.
- Expose aggregation-specific metrics: `tally_agg_ops_total{op}`, `tally_agg_transition_total{op}` (hybrid exact→sketch crossings)

</decisions>

<code_context>
## Existing Code Insights

- `src/engine/operators.rs` — existing operator impls (count, sum, avg, min, max, partial percentile, HLL approx distinct)
- `src/engine/window.rs` — ring buffer + bucket expiry
- `src/engine/pipeline.rs` — cascade pipeline; `push_with_cascade_internal` is where events hit operators
- `src/state/store.rs` — EntityState holds per-key feature state
- `src/server/tcp.rs` — protocol handling; PUSH opcode currently dispatches to v2.0 aggregation path
- `python/tally/_serialize.py` — produces REGISTER JSON payload Phase 22 must consume (contract frozen)
- `python/tally/_agg_ops.py` — AggOp descriptors Phase 22 must match operator-for-operator

Existing dependencies:
- Check `Cargo.toml` for any sketch libraries already present
- Phase 19 benchmark baseline in `bench.py` — use for regression gate

</code_context>

<specifics>
## Specific Ideas

- **Sketch vendoring**: vendor UDDSketch from Timescale's Rust Toolkit. Add `decrement()` in a small patch. CMS: probably write in-house (trivial), or check crates.io for `count-min-sketch`.
- **Observability from day one**: every hybrid operator exposes `mode`, `transition_at`, `sketch_α_current`, `memory_bytes` via `/debug/key/:key`. Don't defer to Phase 25.
- **Benchmark regression gate in-plan**: after operator impls, run the existing benchmark matrix and fail the plan if eps drops > 5%. Existing Phase 19-05 bench script is the reference.

</specifics>

<deferred>
## Deferred Ideas

- Table-input aggregation (v0.1)
- Retraction propagation through DAG (v0.1)
- Full-outer stream-stream join (v0.1, separate research doc)
- CEP / pattern matching (post-v0)
- Custom UDF reducers (indefinitely)

</deferred>

---

*Phase: 22-stream-aggregation-engine*
*Design decisions sourced from `.planning/research/v0-restructure-spec.md`, `uddsketch-retract-verification.md`, `retraction-literature-survey.md`*
