# Phase 10: Sketch operators - Context

**Gathered:** 2026-04-23 (auto mode)
**Status:** Ready for planning

<domain>
## Phase Boundary

Five approximate-algorithm aggregation operators land with documented error bounds, hybrid exact→sketch transitions, and snapshot/WAL-replay round-trip safety:

- `count_distinct(field, window=, exact_threshold=1024, hybrid_precision=14)` — HLL++ p=12 cardinality estimate; Int output. Hybrid: sorted-array (≤16) → AHashSet (≤1024) → HLL++.
- `percentile(field, q, window=, exact_threshold=256, hybrid_alpha=0.01)` — UDDSketch quantile; Float output. Hybrid: Vec<f64> sort (≤256) → UDDSketch (α₀=0.01, max_buckets=2048).
- `top_k(field, k, window=, exact_threshold=1024, hybrid_width=2048, hybrid_depth=4)` — CountMinSketch + bounded TopKHeap (NOT SpaceSaving — comment-only fix to REQ); list output. Hybrid: BTreeMap (≤1024 distinct) → CMS+heap.
- `bloom_member(field, capacity=1024, fpr=0.01)` — windowless Bloom-filter ever-seen test; Bool output. Bit-array + k optimal hashes.
- `entropy(field, window=)` — Shannon entropy (bits, log2) over the empirical categorical distribution; Float output. Categorical histogram with cap-and-spill at 1024 distinct categories.

All five MUST: (a) implement Serialize/Deserialize and round-trip through Phase 6 WAL replay + Phase 7 snapshot/recovery byte-identically, (b) bound memory per-entity by config, (c) compose with the Phase 5 `WindowedOp<>` infrastructure (where windowed semantics make sense — Bloom is ever-seen and stays windowless), (d) accept the same `where=` predicate kwarg as Phase 5 ops via the existing `where_expr` field on `AggOpDescriptor`.

**Out of scope:**
- TCP-push hot path (Phase 8 sibling wires it; Phase 10 records HTTP-only throughput rows).
- Stream retraction (v1 — sketch state stays semantically retractable but no retract API in v0).
- Custom sketch parameters beyond the documented kwargs (advanced tuning is v0.1+).
- Cross-entity / cross-shard sketch merging (architecture-locked v1+ via "per-op handcrafted per-backend").
- New sketch algorithms beyond the five above.

**Throughput run:** must re-run Phase 7.5 harness with sketches added to medium/large pipelines and write the resulting row(s) to `.planning/phases/10-sketch-operators/10-throughput-row.md` (NOT the canonical ledger — orchestrator merges to `.planning/throughput-baselines.md` post-merge). No > 25% regression on the simple-fraud (small) shape.

</domain>

<decisions>
## Implementation Decisions

### D-01 — Port from `main` whenever possible (greenfield only when no prior art)

**Port verbatim (with serde rename tags for snapshot-compat hybrid mode tags):**
- `git show main:src/engine/cms.rs` → `crates/beava-core/src/sketches/cms.rs` — CMS (W=2048, D=4) + bounded TopKHeap with Plan 22-04's O(log k) optimization (`AHashMap<TopKValue, usize>` heap-position side-index — confirmed landed on main at line 230). 8 fixed MurmurHash3 finalizer seeds. `decrement()` for ring-buffer retraction.
- `git show main:src/engine/uddsketch.rs` → `crates/beava-core/src/sketches/uddsketch.rs` — UDDSketch with α₀=0.01, max_buckets=2048, `decrement()` for retraction.
- `git show main:src/engine/hll.rs` → `crates/beava-core/src/sketches/hll.rs` — three-phase adaptive (sorted array ≤16 → AHashSet ≤1024 → HLL++ p=12 dense). All bias-correction tables ported as-is.
- `git show main:src/engine/retracting_ring.rs` → `crates/beava-core/src/sketches/retracting_ring.rs` — `RetractingRingBuffer<T: Default + Clone>` with `on_evict` callback fired before bucket reset. **No retracting variant exists in beava-core today** (verified: `agg_windowed.rs` uses fixed-`[Option<Box<AggOp>>; 64]`); port main's. Adapt the `SystemTime`-based wall clock to use **event_time_ms only** per Phase 5 D-06 (replay determinism — locked invariant).

**Greenfield (no main prior art):**
- `bloom_member` — bit-array (`Vec<u64>`) sized at register time from `capacity`/`fpr` per the standard formula `m = -capacity * ln(fpr) / (ln(2)^2)`, `k = ceil((m/capacity) * ln(2))`. Hash with two independent MurmurHash3 finalizer seeds and synthesize k hashes via Kirsch-Mitzenmacher double-hashing (`h_i = h1 + i * h2`). Fixed-size; no growth.
- `entropy` — `BTreeMap<TopKValue, u64>` category histogram + `total: u64`. Query computes `H = -Σ (p_i * log2(p_i))` over present categories. Cap-and-spill: when distinct categories exceed 1024, route subsequent novel categories to a single `"__other__"` bucket so memory stays bounded at ~1024 × (avg-key-size + 8B) per entity.

**Rationale:** Port-where-possible cuts risk dramatically — main's CMS/UDDSketch/HLL have shipped under load and have correctness tests. The two greenfield ops are arithmetically simple and main lacks prior art. Replay-determinism delta on RetractingRingBuffer (SystemTime → event_time_ms) is the only intrusive port adaptation; pattern follows Phase 5 `agg_windowed.rs`.

### D-02 — Module layout: new `crates/beava-core/src/sketches/` submodule

```
crates/beava-core/src/sketches/
├── mod.rs              — pub use re-exports
├── cms.rs              — CountMinSketch + TopKHeap + TopKValue (port from main)
├── uddsketch.rs        — UDDSketch (port from main)
├── hll.rs              — adaptive distinct (port from main)
├── retracting_ring.rs  — RetractingRingBuffer<T> (port from main, event-time clock)
├── bloom.rs            — Bloom filter (greenfield)
└── entropy.rs          — Shannon entropy histogram (greenfield)
```

Each file is self-contained with its own unit tests. `mod sketches;` added to `lib.rs` with `pub use`. **beava-core WASM-portability invariant preserved** (no syscalls; all five sketches are pure data structures).

### D-03 — Extend `AggKind` + `AggOp` enums with 5 new variants

Add to `crates/beava-core/src/agg_op.rs`:

```rust
pub enum AggKind {
    // ...existing 8 variants...
    CountDistinct,
    Percentile,
    TopK,
    BloomMember,
    Entropy,
}

pub enum AggOp {
    // ...existing 9 variants (8 core + Windowed)...
    CountDistinct(CountDistinctState),
    Percentile(PercentileState),
    TopK(TopKState),
    BloomMember(BloomMemberState),
    Entropy(EntropyState),
}
```

Each `*State` lives in `crates/beava-core/src/agg_state.rs` (extending the existing file) and wraps the relevant sketch with hybrid-mode dispatch. Per Phase 5 D-01: zero-cost enum dispatch, no `Box<dyn>`. **Windowed wrapping unchanged** — `AggOp::Windowed(Box<WindowedOp>)` already accepts any inner `AggKind` and works for sketch ops via the same fold pattern; sketch ops define their own `combine()`/`fold()` methods called by `WindowedOp::query`.

### D-04 — Hybrid mode shape with serde rename tags for snapshot stability

Each hybrid operator's state is itself an enum with mode variants tagged via `#[serde(rename = "...")]` so future modes can be added without breaking snapshots:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode")]
pub enum CountDistinctState {
    #[serde(rename = "v0_count_distinct_exact_array")]
    ExactArray { values: Vec<u64> },                    // ≤16
    #[serde(rename = "v0_count_distinct_hash_set")]
    HashSet { hashes: ahash::AHashSet<u64> },           // ≤1024
    #[serde(rename = "v0_count_distinct_hll")]
    Hll { sketch: hll::Hll },
}

#[serde(tag = "mode")]
pub enum PercentileState {
    #[serde(rename = "v0_percentile_exact")]
    Exact { values: Vec<f64> },                         // ≤256
    #[serde(rename = "v0_percentile_uddsketch")]
    Sketch { sketch: uddsketch::UDDSketch },
}

#[serde(tag = "mode")]
pub enum TopKState {
    #[serde(rename = "v0_top_k_exact")]
    Exact { counts: BTreeMap<cms::TopKValue, u64>, k: usize },  // ≤1024 distinct
    #[serde(rename = "v0_top_k_hybrid")]
    Hybrid { cms: cms::CountMinSketch, heap: cms::TopKHeap, k: usize },
}

// No hybrid mode for Bloom (fixed-size by design) or Entropy (uses spill bucket).
```

**Transition trigger:** `update()` checks the threshold AFTER inserting the new value; if exceeded, calls `promote_to_next_mode()` which rebuilds the sketch from the held data, then drops the held data. Promotion is one-way per the architectural pattern from main.

**Rationale:** Mirrors the design captured in `git show main:.planning/phases/22-stream-aggregation-engine/22-03-SUMMARY.md`. Snapshot tags are stable across all v0 → v0.x.y releases. Adding a v0.1 mode (e.g., a denser HLL precision) is additive: register a new `#[serde(rename = "v0_count_distinct_hll_p14")]` variant, ship a one-time migration on snapshot load.

### D-05 — Memory bounds per operator (SC4)

| Op | Mode | Memory (worst case per entity) |
|---|---|---|
| count_distinct | exact_array | 16 × 8 B = 128 B |
| count_distinct | hash_set | 1024 × 16 B ≈ 16 KB |
| count_distinct | hll | 4096 registers × 1 B = 4 KB |
| percentile | exact | 256 × 8 B = 2 KB |
| percentile | uddsketch | 2048 buckets × ~24 B ≈ 48 KB worst, ~few KB typical |
| top_k | exact | 1024 × (avg_key_bytes + 8) ≈ 16-32 KB at strings ≤24 B |
| top_k | hybrid | CMS 2048×4×8 = 64 KB + heap k×~32 B |
| bloom_member | (windowless only) | m bits at fpr=0.01, capacity=1024 ≈ 1.2 KB |
| entropy | always | ≤1024 categories × (avg_key_bytes + 8) + spill ≈ 16-32 KB |

Windowed ops multiply by active-bucket count (≤64). **Ledger** memory bounds in operator docs/tests verifying with proptest-shaped fixtures.

### D-06 — RetractingRingBuffer integration with windowed sketches

Windowed sketch ops use `WindowedOp` (existing enum variant) but each bucket holds the **op-specific retention payload** needed to decrement on eviction:

- **count_distinct windowed**: per-bucket holds the full mode (exact_array/hash_set/hll). Eviction = bucket dropped; query merges across active buckets. HLL merge is closed-form; hash sets union; arrays union with re-promotion at the merged level.
- **percentile windowed**: per-bucket holds either Vec<f64> (exact) or UDDSketch. Eviction drops bucket; query merges (UDDSketch supports add; for exact, concat-sort at query time).
- **top_k windowed**: per-bucket BTreeMap<TopKValue,u64> in exact mode, OR (CMS+heap+candidate-set) in hybrid mode. On eviction the bucket's CMS/heap is dropped wholesale. Query merges across active buckets (sum counts across buckets per candidate; rebuild top-k from union).
- **entropy windowed**: per-bucket BTreeMap<TopKValue,u64>. Query merges histograms then computes Shannon entropy on the merged.
- **bloom_member**: NOT windowed. `bloom_member` is ever-seen by spec (REQ AGG-SKETCH-04). Windowed semantics deferred to v0.1 via a separate `window_member` op if user demand surfaces.

The existing `WindowedOp` 64-bucket ring already iterates active buckets via `bucket_epoch_start_ms` — sketch ops fold the same way Phase 5 ops do. **No new ring data structure needed for windowed sketches** if we accept "bucket-drop wholesale" semantics (vs main's per-value retraction). RetractingRingBuffer is ported anyway because it's required to make the design forward-compatible with v0.1+ retraction APIs and is small (~200 lines) — but Phase 10's windowed sketch ops use `WindowedOp` bucket-drop, NOT `RetractingRingBuffer` per-value retraction. Document this delta.

**Decision (re-confirmed):** Phase 10 ships windowed sketches via `WindowedOp` bucket-drop (matches Phase 5 semantics). Port RetractingRingBuffer for forward-compat / v0.1 use but do NOT make Phase 10 ops depend on it. This keeps the apply-loop hot path identical to Phase 5's fast match-arm dispatch.

### D-07 — Compile-time wiring (`agg_compile.rs` + Rule 11)

Extend `crates/beava-core/src/agg_compile.rs::compile_aggregations_from_nodes` to recognize the 5 new op names from REGISTER JSON:

- `"count_distinct"` → `AggKind::CountDistinct` + parse `exact_threshold` + `hybrid_precision` kwargs (defaults 1024 / 14, both stored on a per-op config struct on the descriptor)
- `"percentile"` → parse `q` (required), `exact_threshold` (256), `hybrid_alpha` (0.01)
- `"top_k"` → parse `k` (required), `exact_threshold` (1024), `hybrid_width` (2048), `hybrid_depth` (4)
- `"bloom_member"` → parse `capacity` (1024), `fpr` (0.01); reject `window` kwarg with `kind="window_not_supported"`
- `"entropy"` → no extra kwargs

**Schema validation (Rule 11 extension):**
- `field` required for all five; must exist in upstream schema (existing helper `validate_field_exists`)
- `bloom_member` rejects `window=` kwarg (windowless-only, REQ AGG-SKETCH-04 spec)
- `percentile.q` must parse as float in (0.0, 1.0); else `kind="invalid_percentile_q"`
- `top_k.k` must parse as positive int ≤ 1024; else `kind="invalid_top_k_k"`
- `bloom_member.fpr` must be in (0.0, 1.0); else `kind="invalid_bloom_fpr"`
- `count_distinct/percentile/top_k` accept `window` kwarg (parsed via existing `parse_duration_to_ms`)
- All five accept `where=` predicate (zero new machinery — uses existing Phase 5 D-03 path)

**Error wire shape:** identical to Phase 5 — `{kind, path, message}` HTTP+TCP parity.

### D-08 — Output type mapping (`output_type_for`)

Extend `agg_op.rs::output_type_for`:
- `CountDistinct` → `FieldType::Int`
- `Percentile` → `FieldType::F64`
- `TopK` → `FieldType::Json` (list of `{value, count}` pairs; new `FieldType::Json` variant if not present, OR use an existing structured type — planner picks based on `crates/beava-core/src/schema.rs` audit)
- `BloomMember` → `FieldType::Bool`
- `Entropy` → `FieldType::F64`

**If `FieldType::Json` doesn't exist:** add it as a one-shot extension in plan 10-01 with serde-pass-through semantics. Top-k output is the only case requiring a structured value; defer "structured outputs round-trip through GET" verification to plan 10-06 (smoke test).

### D-09 — Bench coverage (Performance Discipline gate)

Per CLAUDE.md §Performance Discipline: at least one criterion microbench under `crates/beava-core/benches/`. Add `crates/beava-core/benches/phase10_sketches.rs` with groups:

```
sketch_ops/count_distinct_exact_update          # hybrid mode 1
sketch_ops/count_distinct_hash_set_update       # hybrid mode 2
sketch_ops/count_distinct_hll_update            # hybrid mode 3
sketch_ops/count_distinct_promote_array_to_set
sketch_ops/count_distinct_promote_set_to_hll
sketch_ops/percentile_exact_update
sketch_ops/percentile_uddsketch_update
sketch_ops/percentile_promote_to_sketch
sketch_ops/percentile_query_p99_uddsketch
sketch_ops/top_k_exact_update
sketch_ops/top_k_hybrid_update
sketch_ops/top_k_query_k10_hybrid
sketch_ops/bloom_member_update_1k
sketch_ops/bloom_member_query_1k
sketch_ops/entropy_update_100cat
sketch_ops/entropy_query_100cat
windowed/count_distinct_5m_1Mevt_hll
windowed/percentile_5m_1Mevt
windowed/top_k_5m_1Mevt
windowed/entropy_5m_1Mevt
```

Per-bench median rows go to `.planning/phases/10-sketch-operators/10-perf-row.md` (NOT canonical `.planning/perf-baselines.md` — orchestrator merges).

### D-10 — Throughput run

Extend the Phase 7.5 harness pipeline configs with sketch features:

- `crates/beava-bench/configs/medium-with-sketches.json` — adds `count_distinct(merchant_id, window="1h")` + `percentile(amount, q=0.99, window="1h")` to medium pipeline (5 features → 7).
- `crates/beava-bench/configs/large-with-sketches.json` — adds the four windowed sketches (count_distinct, percentile, top_k, entropy) plus `bloom_member(device_id)` to large pipeline (15 features → 20).

Run each via `cargo run -p beava-bench --release -- throughput --pipeline {name} --transport http`. Append result rows to `.planning/phases/10-sketch-operators/10-throughput-row.md` with the same column shape as `.planning/throughput-baselines.md`. **TCP transport NOT exercised** — Phase 8 sibling wires the TCP push handler; Phase 10 records HTTP-only rows with a `Notes: HTTP-only; TCP push not yet wired (Phase 8 sibling)` annotation.

**Regression check:** simple-fraud (small) shape MUST NOT regress > 25% from Phase 7.5 baselines. Sketches don't run in the small pipeline so any regression there is incidental (compile-time bloat, registry hashing); document but tolerate up to the 25% block threshold.

### D-11 — REQUIREMENTS.md one-liner fix (separate commit)

`AGG-SKETCH-03` currently reads "SpaceSaving top-K" but the actual implementation (both on main and being ported here) is **CountMinSketch + bounded min-heap with hybrid exact mode**. Fix:

```
- [ ] **AGG-SKETCH-03**: `bv.top_k(field, k, window=..., exact_threshold=1024, hybrid_width=2048, hybrid_depth=4)` — CountMinSketch + bounded min-heap (hybrid exact/sketch mode); list output
```

Commit message: `docs(requirements): fix AGG-SKETCH-03 algorithm name (CMS+heap not SpaceSaving)`. Pre-Phase-10 plans, NOT counted in the test-count delta.

### D-12 — TDD discipline (red→green per task)

Per CLAUDE.md §Conventions, mandatory from Phase 3 onward. Each plan task splits into:
- **Task N.a (red)** — failing test(s); commit `test(10-NN): subject`
- **Task N.b (green)** — passing impl; commit `feat(10-NN): subject` (or `refactor(10-NN):` / `chore(10-NN):`)

Sketch ops have natural red-green tests:
- Bit-exact correctness on small fixtures (count_distinct on 100 distinct → expect ≥99/≤101; percentile on 1000-element uniform → median in [0.49, 0.51]; etc.)
- Snapshot/replay round-trip: `serialize → deserialize → query equivalence` proptest
- Mode-promotion correctness: insert past threshold → assert mode transition + query continuity
- Window fold correctness: 64-bucket fold matches `lifetime equivalent insert order`
- Memory bound: `mem::size_of_val(&state)` + `state.estimated_bytes()` ≤ documented bound

### D-13 — Verification (success criteria mapping)

| SC | How verified |
|---|---|
| SC1 — count_distinct, percentile, top_k pass error-bound checks | Reference-dataset table-driven tests in `crates/beava-core/src/sketches/{cms,uddsketch,hll}.rs` modules. Datasets: uniform-100, zipfian-1000, gaussian-10K. Document tolerances in test comments. |
| SC2 — Sketch serialization round-trips through snapshot + WAL replay | New integration test `crates/beava-server/tests/phase10_sketch_recovery.rs` — register pipeline with all 5 sketches, push 1000 events, force snapshot, drop server, respawn, assert query values byte-equal. (Plus per-sketch bincode round-trip proptest in core.) |
| SC3 — bloom_member + entropy pass table-driven tests | Per-op tests in `bloom.rs` + `entropy.rs` modules. Fixtures: bloom (insert "a"/"b"/"c", assert "a" + "d" → true/maybe/false), entropy (uniform 4-cat → 2.0 bits, single-cat → 0.0 bits, etc.). |
| SC4 — Memory bounded per-entity by operator config | `estimated_bytes()` method on each state; assertion tests in mode-promotion tests. |
| SC5 — Throughput run, no > 25% regression on simple-fraud | `10-throughput-row.md` with HTTP rows for medium-with-sketches + large-with-sketches; comparison vs. Phase 7.5 baseline annotated inline. |

### D-14 — Plan structure (anticipated 6-7 plans)

The planner will refine this — captured as guidance, not lock:

- **10-01**: REQ comment fix + `sketches/` module scaffold + RetractingRingBuffer port (red-green per file). Lands the Bloom + Entropy greenfield ops too (simplest first; no hybrid modes).
- **10-02**: HLL port + CountDistinctState hybrid (3 modes) + windowless tests + bincode round-trip proptest.
- **10-03**: UDDSketch port + PercentileState hybrid (2 modes) + windowless tests + bincode round-trip proptest.
- **10-04**: CMS+TopKHeap port (incl. Plan 22-04 O(log k) optimization) + TopKState hybrid (2 modes) + windowless tests + bincode round-trip proptest.
- **10-05**: AggKind + AggOp enum extension + agg_compile Rule 11 extension + output_type_for + WindowedOp wrapping for the 4 windowed-eligible sketches. End-to-end smoke `crates/beava-server/tests/phase10_sketch_smoke.rs`.
- **10-06**: Phase 7 snapshot/recovery integration test for sketches (`phase10_sketch_recovery.rs`).
- **10-07**: criterion bench + throughput-run row + 10-perf-row.md + 10-throughput-row.md + 10-VERIFICATION.md.

**Plan-checker contract** (CLAUDE.md §Performance Discipline): plan 10-07 has `files_modified` containing `crates/beava-core/benches/phase10_sketches.rs`. Satisfied.

### D-15 — CMS+heap O(log k) optimization

Plan 22-04's optimization (HashMap heap-position side-index for `O(log k)` insert) **DID land on main** — verified at `git show main:src/engine/cms.rs:230` (`index: ahash::AHashMap<TopKValue, usize>`). **Port it.** Do NOT defer the optimization — it's already proven on main and the port is identity. Plan 10-04 captures it with a comment crediting Plan 22-04.

### Claude's Discretion

- **TopK output structured-value type**: whether `FieldType::Json` exists in `crates/beava-core/src/schema.rs` or needs a one-line addition; planner audits and picks. If absent: add as `FieldType::Json` taking `serde_json::Value` (passthrough semantics, not validated against any schema). Top-k output: `[{"value": "...", "count": N}, ...]` JSON array.
- **TopKValue<>FieldType bridging**: whether to coerce non-string fields (Int/Bool/F64) through TopKValue automatically (likely yes — main's CMS handles it; preserve the same semantics for top_k over numeric/bool fields).
- **Bloom hash function**: stick with main's MurmurHash3 finalizer (used in CMS) for consistency. Two seeds + double-hashing per Kirsch-Mitzenmacher.
- **Entropy spill bucket name**: `"__beava_other__"` or `"__other__"` — pick the less likely to collide with user data.
- **Windowed sketch fold algorithm choice**: bucket-drop wholesale (D-06) over per-value retraction. Document trade-off (slightly higher memory during window vs. zero retraction overhead in apply path).
- **Bench medium/large pipeline JSON shape**: align with existing `crates/beava-bench/configs/{medium,large}.json` patterns. Two new files (don't mutate existing — keeps Phase 7.5 baselines comparable apples-to-apples).
- **Window=forever for sketches**: explicitly supported (windowless mode = lifetime sketch). All hybrid modes work without `window`.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### ROADMAP & REQUIREMENTS
- `.planning/ROADMAP.md` § Phase 10 (lines 317-330) — goal + 5 success criteria + REQ-IDs
- `.planning/REQUIREMENTS.md` § AGG-SKETCH (lines 67-73) — REQ-ID rows; **AGG-SKETCH-03 SpaceSaving comment fix is plan 10-01 Task 0**
- `CLAUDE.md` § Conventions → TDD Discipline — red→green commit-pair pattern
- `CLAUDE.md` § Performance Discipline — bench gate (10% warn / 25% block); plan-checker contract requires `crates/*/benches/` file in `files_modified`

### Prior Phase context (carried forward — applies here)
- `.planning/phases/05-aggregation-framework-core-operators/05-CONTEXT.md` — locks D-01 (enum dispatch, no Box<dyn>), D-03 (where=predicate), D-04 (64-bucket event-time tumbling), D-06 (replay-determinism — no SystemTime in apply path), D-07 (Rule 11 schema validation pattern), D-08 (WAL entry shape)
- `.planning/phases/07-snapshot-recovery-schema-evolution/07-SUMMARY.md` — snapshot serialization invariants (bincode), schema-evolution survives restart
- `.planning/phases/07.5-end-to-end-throughput-harness-first-baseline/07.5-CONTEXT.md` — throughput harness invocation; row format mirrors `.planning/throughput-baselines.md`; 60s wall-time saturating workload
- `.planning/phases/05.5-perf-harness-retroactive-baselines/05.5-SUMMARY.md` — criterion bench ledger pattern

### Source code (Phase 5 + 7 reusable)
- `crates/beava-core/src/agg_op.rs` — AggKind + AggOp enum (extend with 5 variants)
- `crates/beava-core/src/agg_state.rs` — per-op state structs (add 5 hybrid-state structs)
- `crates/beava-core/src/agg_windowed.rs` — `WindowedOp` 64-bucket ring (sketch ops wrap into it; query fold extends to call sketch combine methods)
- `crates/beava-core/src/agg_compile.rs` — `compile_aggregations_from_nodes` + Rule 11 (extend op-name dispatch + new error kinds)
- `crates/beava-core/src/agg_apply.rs` — `apply_event_to_aggregations` (sketch ops fall out of existing match-arm pattern; no API change)
- `crates/beava-core/src/agg_descriptor.rs` — `AggOpDescriptor` (kwargs already extensible via JSON; planner verifies)
- `crates/beava-core/src/schema.rs` — `FieldType` (audit for `Json` variant; add if absent)
- `crates/beava-core/src/snapshot_body.rs` — bincode-based state snapshot (sketches gain `Serialize+Deserialize` via serde derive; rename tags ensure forward-compat)
- `crates/beava-core/benches/phase5_agg.rs` — bench template; mirror its pattern in `phase10_sketches.rs`
- `crates/beava-server/tests/phase7_smoke.rs` — recovery-test pattern; mirror in `phase10_sketch_recovery.rs`
- `crates/beava-bench/configs/{medium,large}.json` — pipeline JSON shape; add `*-with-sketches.json` siblings

### Main branch (port-from references)
Read via `git show main:<path>` from any branch:
- `src/engine/cms.rs` — CMS + TopKHeap with Plan 22-04 O(log k) HashMap index (line 230 confirms it landed)
- `src/engine/uddsketch.rs` — UDDSketch with `decrement()` (411 lines)
- `src/engine/hll.rs` — adaptive distinct, three-phase (944 lines incl. bias tables)
- `src/engine/retracting_ring.rs` — `RetractingRingBuffer<T>` with `on_evict` callback (206 lines; adapt SystemTime → event_time_ms)
- `src/engine/cms_test.rs` (if exists) — test fixtures to port
- `.planning/phases/22-stream-aggregation-engine/22-03-SUMMARY.md` — hybrid-mode design + serde rename tag scheme

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets (from Phases 1–7.5)
- **`crates/beava-core/src/agg_op.rs`** — `AggKind` + `AggOp` enums; extend with 5 variants. `AggOpDescriptor` already supports `field`/`window_ms`/`where_expr`; sketch ops add per-op kwargs via JSON in the registration body (planner extends `AggOpDescriptor` if needed for hybrid thresholds).
- **`crates/beava-core/src/agg_windowed.rs`** — `WindowedOp` 64-bucket event-time tumbling ring; sketch ops wrap into it for windowed mode. `WindowedOp::query` does the fold; sketch ops contribute their `combine()` method.
- **`crates/beava-core/src/agg_compile.rs`** — `compile_aggregations_from_nodes` + Rule 11; extend for 5 new op names + 4 new error kinds.
- **`crates/beava-core/src/snapshot_body.rs`** — bincode-based snapshot pattern; sketches gain `#[derive(Serialize, Deserialize)]` and serde rename tags carry mode-stability across snapshot versions.
- **`crates/beava-bench/`** — Phase 7.5 throughput harness; CLI takes `--pipeline {file}.json --transport {http|tcp}`. Add 2 new pipeline files; reuse the harness as-is.
- **`parking_lot::RwLock`** patterns from Phase 2; not relevant here (sketches are single-writer per entity).
- **`ahash`** transitively available via main's port (used in CMS TopKHeap index + HashSet); add to `crates/beava-core/Cargo.toml` if not present.

### Established Patterns
- **Per-op enum variant + match arm** dispatch (Phase 5 D-01) — keeps zero-cost dispatch through the apply loop. Sketch ops follow this exactly.
- **`where=` predicate** on every op via `AggOpDescriptor.where_expr` (Phase 5 D-03) — sketch ops inherit zero-effort.
- **Replay determinism** — no `SystemTime::now()`, no `rand`, BTreeMap for serialization-iteration-order-dependent state (Phase 5 D-06). RetractingRingBuffer port adapts `SystemTime` → `event_time_ms`.
- **Wire error parity** HTTP/TCP — same `{kind, path, message}` shape (Phase 4 + Phase 5 precedent).
- **TDD red→green** per task with `^test\(10-NN\):` then `^feat\(10-NN\):` commit-message regex (CLAUDE.md mandatory from Phase 3+).
- **Per-phase perf bench** under `crates/<crate>/benches/` (CLAUDE.md §Performance Discipline plan-checker contract).
- **Per-phase throughput row** appended to `.planning/throughput-baselines.md` via Phase 7.5 harness (every operator phase 8-12 must include a throughput-run task).
- **dev-dependency for testing** uses `[features.testing]` flag on beava-server.

### Integration Points
- **Apply loop hook**: `agg_apply::apply_event_to_aggregations` already iterates `AggOp` variants — sketch ops fall out of the same match-arm dispatch with zero apply-path API change.
- **Feature query**: `GET /get/{feature}/{key}` uses `AggOp::query()` — sketch ops add their query methods returning `Value::Int/F64/Bool/Json`.
- **Register-time validation**: Rule 11 in `agg_compile.rs` is the single hook; sketch ops add 5 op-name dispatch arms + 4 new error kinds.
- **Snapshot**: bincode + serde derive on each sketch state. Round-trip test required.
- **WAL replay**: Phase 6 replays push events; sketches re-process events through the apply loop on replay. Determinism required (D-06 carried forward).

</code_context>

<specifics>
## Specific Ideas

- **Plan 22-04's O(log k) optimization** must land alongside the CMS port (HashMap heap-position side-index — proven on main, not an open question).
- **AGG-SKETCH-03 algorithm name** is incorrect in REQUIREMENTS.md (says SpaceSaving, is actually CMS+heap) — fix as a separate, pre-Phase-10 commit so the test-count delta is clean.
- **macOS fsync ceiling ~7.4 ms** (per CLAUDE.md hard constraints + Phase 7.5 perf-baselines.md notes) — throughput rows will plateau at ~1k EPS. Document this in the throughput-row notes.
- **TCP push not wired** until Phase 8 sibling lands — Phase 10 throughput rows are HTTP-only. Annotate accordingly.
- **624-test baseline** post-Phase-7.5 — Phase 10 should not regress. Expect ~+30-50 new tests for the 5 ops + windowed + recovery + bench coverage.
- **Hybrid-mode serde rename tags** make the snapshot format extensible without breaking changes — adopt them for all 3 hybrid ops (count_distinct, percentile, top_k).
- **WASM-portability invariant**: all sketch code lands in `beava-core` (no syscalls; deterministic; pure data structures). Verifies by visual inspection — no `std::time::SystemTime`, no `std::fs`, no `std::net`, no `tokio`.

</specifics>

<deferred>
## Deferred Ideas

- **Stream retraction for sketches** — v1. Architecture supports it (`RetractingRingBuffer` ported, `decrement()` on UDDSketch + CMS preserved) but no public retract API in v0 for streams; only tables retract (v1 milestone).
- **`window_member` op (windowed Bloom)** — v0.1 if user demand surfaces; AGG-SKETCH-04 explicitly windowless.
- **DDSketch (proper) instead of UDDSketch** — v0.1+. UDDSketch is a strict superset (collapse → α-degradation) and is what main ships. AGG-SKETCH-02's "DDSketch" naming is interchangeable with UDDSketch for v0; document in operator docs.
- **Custom HLL precision** (currently fixed p=12) — v0.1+ via `hybrid_precision` kwarg actually plumbed through (v0 stores it on the descriptor but only honors p=12).
- **Cross-entity sketch merging** — v1+; locked per "per-op handcrafted per-backend" architecture.
- **TCP push throughput row for Phase 10** — Phase 8 sibling wires TCP push handler; once landed, Phase 10's throughput row can be re-run with TCP transport and appended.
- **Top-k structured output `{value, meta}` envelope** — v0 ships `{value: [...]}` only per Phase 5 D-02 minimal envelope. Metadata (e.g., total_count, error_bound) deferred to Phase 13 observability or v0.1.
- **Rejected scope-creep ideas** (none surfaced in auto mode — discussion stayed within phase boundary).

</deferred>

---

*Phase: 10-sketch-operators*
*Context gathered: 2026-04-23 via auto mode (recommended-default selection on every gray area; rationale captured inline above)*
