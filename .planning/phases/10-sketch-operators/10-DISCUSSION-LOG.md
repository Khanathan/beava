# Phase 10: Sketch operators - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-23
**Phase:** 10-sketch-operators
**Mode:** `--auto` (Claude auto-selected recommended option for every gray area; no interactive questioning)
**Areas discussed:** Port-vs-greenfield · Module layout · Hybrid-mode serde · Windowed sketch storage · CMS optimization · REQ comment fix · Bench surface · Throughput pipeline shape

---

## Port-vs-greenfield strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Port from main | Lift `cms.rs`/`uddsketch.rs`/`hll.rs`/`retracting_ring.rs` verbatim with serde rename tags | ✓ |
| Greenfield all 5 | Rewrite from spec; ignore main | |
| Hybrid: port 3, greenfield 2 | Port HLL/CMS/UDDSketch (have main impls); greenfield Bloom + Entropy (no main prior art) | ✓ |

**Auto-selected:** Hybrid (port 3, greenfield 2). Rationale: main's impls are battle-tested with shipped correctness tests; cuts risk and shrinks plan surface dramatically. Bloom + Entropy are arithmetically simple and main lacks prior art.

---

## Module layout

| Option | Description | Selected |
|--------|-------------|----------|
| `crates/beava-core/src/sketches/{cms,hll,uddsketch,bloom,entropy,retracting_ring}.rs` | New submodule, one file per sketch | ✓ |
| Inline into `crates/beava-core/src/agg_state.rs` | Single mega-file | |
| New crate `beava-sketches` | Workspace member | |

**Auto-selected:** New submodule. Mirrors how `agg_*.rs` files are organized at the crate root. Preserves WASM-portability invariant. Avoids workspace churn for what is conceptually still beava-core data structures.

---

## Hybrid-mode serde-rename-tag scheme

| Option | Description | Selected |
|--------|-------------|----------|
| `#[serde(tag = "mode")]` enum with `v0_top_k_exact` / `v0_top_k_hybrid` etc. | Snapshot-stable rename tags per main's design | ✓ |
| Untagged enum + version field on outer struct | Custom version compat | |
| Enum with default rename (variant name) | No rename — fragile to refactor | |

**Auto-selected:** Tagged enum with explicit `v0_*` rename tags per main's `22-03-SUMMARY.md` design. Snapshot format is forward-compatible: adding a v0.1 mode = additive variant + one-time migration on snapshot load.

---

## Windowed sketch storage

| Option | Description | Selected |
|--------|-------------|----------|
| Use existing `WindowedOp` 64-bucket ring with bucket-drop wholesale | Bucket holds full sketch state; eviction drops bucket; query merges buckets | ✓ |
| Use ported `RetractingRingBuffer` with per-value retraction (decrement on eviction) | Bucket holds raw values; on eviction, decrement parent sketch | |

**Auto-selected:** Bucket-drop wholesale via existing `WindowedOp`. Reuses Phase 5's apply-loop hot path identically; sketches add only `combine()`/`fold()` query methods. RetractingRingBuffer is ported anyway for forward-compat / v0.1+ retraction APIs but Phase 10 ops do not depend on it. Trade-off: slightly higher memory during the window vs. zero retraction overhead in apply path.

---

## CMS+heap O(log k) optimization

| Option | Description | Selected |
|--------|-------------|----------|
| Port Plan 22-04's HashMap heap-position side-index | O(log k) insert; verified landed on main at `git show main:src/engine/cms.rs:230` | ✓ |
| Defer to v0.1 perf follow-up | Ship O(k) linear-scan version | |

**Auto-selected:** Port. The optimization is already proven on main (commit history confirms it landed); the port is identity. Plan 10-04 captures it with a comment crediting Plan 22-04.

---

## REQUIREMENTS.md AGG-SKETCH-03 SpaceSaving comment fix

| Option | Description | Selected |
|--------|-------------|----------|
| Fix as separate pre-Phase-10 commit (`docs(requirements): ...`) | Clean test-count delta; clear commit history | ✓ |
| Bundle into plan 10-01 first commit | Mixes docs with TDD red-green | |

**Auto-selected:** Separate commit. Keeps the test-count delta clean (REQUIREMENTS.md fix has no associated tests) and matches the orchestrator's "commit separately" instruction.

---

## Bench surface

| Option | Description | Selected |
|--------|-------------|----------|
| `crates/beava-core/benches/phase10_sketches.rs` mirroring `phase5_agg.rs` pattern | 20 microbenches across 5 ops × hybrid modes + windowed folds | ✓ |
| Single-op benches under each `sketches/{cms,hll,uddsketch}.rs` as `#[bench]` | Co-located with code | |
| Skip benches (Phase 7.5 throughput is enough) | Violates CLAUDE.md §Performance Discipline plan-checker contract | |

**Auto-selected:** Phase-level bench file. Mirrors Phase 5's `phase5_agg.rs` pattern. Per-bench rows go to `10-perf-row.md` (orchestrator merges to canonical `.planning/perf-baselines.md` post-merge).

---

## Throughput pipeline JSON shape

| Option | Description | Selected |
|--------|-------------|----------|
| Add `medium-with-sketches.json` + `large-with-sketches.json` siblings to existing configs | Keeps Phase 7.5 baselines comparable apples-to-apples | ✓ |
| Mutate existing `medium.json` + `large.json` in place | Breaks Phase 7.5 baseline comparisons | |
| Single new `phase10.json` config | Loses small/medium/large tiering | |

**Auto-selected:** Sibling configs. Per-phase regression check vs. Phase 7.5 baseline depends on the original configs being unchanged. Sibling configs let the throughput-run capture Phase 10's row alongside the baseline comparison.

---

## Claude's Discretion (no user input solicited)

- TopK output structured-value type (`FieldType::Json` if present, else add)
- TopKValue<>FieldType bridging for non-string fields
- Bloom hash function (MurmurHash3 finalizer per CMS for consistency)
- Entropy spill-bucket name (`__beava_other__`)
- Bucket-drop vs per-value retraction (locked to bucket-drop above)
- Plan 10-NN file granularity (anticipated 6-7 plans; planner refines)
- Throughput row column shape (mirrors `.planning/throughput-baselines.md` exactly)

## Deferred Ideas

- Stream retraction for sketches → v1 (architecture supports it, no public API in v0)
- `window_member` (windowed Bloom) → v0.1 if user demand
- Proper DDSketch instead of UDDSketch → v0.1+
- Custom HLL precision plumbed through → v0.1+
- Cross-entity sketch merging → v1+ (locked per architecture)
- TCP push throughput row → after Phase 8 sibling wires TCP push
- Top-k structured `{value, meta}` envelope → Phase 13 / v0.1

## Scope-creep redirects

None — auto mode discussion stayed within phase boundary.
