---
phase: 19-1m-bench
plan: 01
subsystem: bench-harness
tags: [bench, blast-shape, pool, zipfian, throughput, phase-19]
provides:
  - "beava_bench::blast_shape::BlastShape (enum)"
  - "beava_bench::blast_shape::BlastShapeConfig (struct)"
  - "beava_bench::blast_shape::PipelineConfig (re-export)"
  - "beava_bench::blast_shape::WireFormat (enum)"
  - "beava_bench::blast_shape::ZipfianSampler (struct)"
  - "beava_bench::blast_shape::BlastShapeError (enum)"
  - "beava_bench::blast_shape::build_pool (fn)"
  - "beava_bench::blast_shape::build_pool_timed (fn)"
requires:
  - "beava_core::wire (Phase 2.5 codec — Frame, encode_frame, CT_JSON, CT_MSGPACK, OP_PUSH)"
  - "rand 0.8 (workspace-pinned, std_rng feature)"
  - "rmp-serde 1 (workspace-pinned)"
  - "thiserror 1 (workspace-pinned)"
affects:
  - "Plan 19-02 (bench harness integration) — will `use beava_bench::blast_shape::*`"
  - "Plan 19-04 (microbench) — criterion benches will exercise build_pool"
  - "Plan 19-05 (throughput run) — reports per-shape ledger rows"
key-files:
  created:
    - "crates/beava-bench/src/lib.rs"
    - "crates/beava-bench/src/blast_shape.rs"
    - "crates/beava-bench/tests/blast_shape_test.rs"
    - ".planning/phases/19-1m-bench/19-01-SUMMARY.md"
  modified:
    - "crates/beava-bench/Cargo.toml"
decisions:
  - "Hand-roll the Zipfian sampler (no rand_distr dep) for deterministic byte-identical pool output across same-seed runs (Test 8 contract)."
  - "alpha == 1.0 (the Phase 19 default) gets a dedicated log-uniform inverse-CDF branch to avoid the standard Gray et al. inverse formula's division-by-zero limit."
  - "Fixed shape encodes ONE frame then clones the `Bytes` N times — refcount bump only, no payload duplication."
  - "Non-Fixed shapes reuse a single `BytesMut` across the loop and freeze each frame via `split()` — zero allocator churn per frame inside the hot encode loop."
  - "PipelineConfig is re-exported by the lib (rather than imported from the binary crate) so blast_shape stays a leaf module — Plan 19-02 will refactor the binary to consume the lib's type."
  - "Mixed shape varies event_name; key generation still rolls over a wide cardinality so we don't accidentally cache-warm by reusing one key."
metrics:
  duration: "~25 minutes"
  completed: "2026-04-26"
  tasks: 2
  tests_added: 10
  lines_added_lib: 377
  lines_added_test: 373
---

# Phase 19 Plan 01: Pool=N pre-encoded-frame blast-shape builder — Summary

Built the reusable Pool=N pre-encoded-TCP-frame builder + four blast shape implementations (`fixed` / `uniform` / `zipfian` / `mixed`) + deterministic seedable Zipfian sampler in `crates/beava-bench/src/blast_shape.rs`. Plan 19-02 (bench-harness integration), Plan 19-04 (microbench), and Plan 19-05 (throughput run) all consume this surface.

## What landed

### Public API

All exported via `beava_bench::blast_shape::*`:

```rust
pub enum BlastShape {
    Fixed,
    Uniform { cardinality: u64 },
    Zipfian { alpha: f64, cardinality: u64 },
    Mixed { event_count: usize },
}

pub struct BlastShapeConfig<'a> {
    pub pipeline: &'a PipelineConfig,
    pub event_names_for_mixed: &'a [&'a str],
    pub wire_format: WireFormat,
    pub seed: u64,
}

pub fn build_pool(shape: BlastShape, cfg: &BlastShapeConfig, n: u64)
    -> Result<Vec<Bytes>, BlastShapeError>;

pub fn build_pool_timed(shape: BlastShape, cfg: &BlastShapeConfig, n: u64)
    -> Result<(Vec<Bytes>, Duration), BlastShapeError>;

pub struct ZipfianSampler { /* private */ }
impl ZipfianSampler {
    pub fn new(alpha: f64, k: u64, seed: u64) -> Self;
    pub fn sample(&mut self) -> u64;
}

pub enum BlastShapeError {
    MixedRequiresMultipleEvents,
    InvalidAlpha,
    InvalidCardinality,
}
```

`PipelineConfig` is re-exported here (rather than living only in the binary crate) so the lib has no upward dependency on the binary's private types. Plan 19-02 will switch the binary's local `PipelineConfig` to consume this lib type.

### Files

**Created:**

- `crates/beava-bench/src/lib.rs` (10 lines) — public library entry; `pub mod blast_shape;`
- `crates/beava-bench/src/blast_shape.rs` (377 lines) — types + Zipfian sampler + Pool=N pool builder + body + envelope encoders
- `crates/beava-bench/tests/blast_shape_test.rs` (373 lines) — 10 invariants (8 unit tests + 2 proptests)

**Modified:**

- `crates/beava-bench/Cargo.toml` — added `[lib]` target (lib name `beava_bench`, path `src/lib.rs`); added `proptest = { workspace = true }` to `[dev-dependencies]`; added `thiserror = { workspace = true }` to `[dependencies]`. `tempfile` was already present in `[dev-dependencies]`; left untouched per Plan 19-01's revision-1 warning.

### Tests (10 / 10 green)

| # | Test | Purpose |
|---|------|---------|
| 1 | `pool_size_matches_n` | Pool length = requested N |
| 2 | `fixed_shape_produces_identical_frames` | All N frames byte-identical for `Fixed` |
| 3 | `uniform_shape_distributes_keys_evenly` (proptest, 8 cases) | All K buckets hit; max bucket ≤ 2 × N/K |
| 4 | `zipfian_shape_long_tail` (proptest, 4 cases) | Top-1 ≥ 5% of N; bottom 50% of keys ≤ 30% of N |
| 5 | `mixed_shape_rotates_through_events` | All M event names appear; each ≥ 80% of fair share |
| 6 | `frames_decode_to_valid_envelopes_json` | `serde_json` parse + `event` + `body` keys present |
| 7 | `frames_decode_to_valid_envelopes_msgpack` | `rmp_serde` parse + `event` + `body` keys present |
| 8 | `zipfian_sampler_deterministic` | Same-seed → same sequence (no rand_distr coupling) |
| 9 | `pool_setup_time_measurable` | `build_pool_timed` returns positive `Duration` |
| 10 | `mixed_shape_requires_multi_event_pipeline_or_falls_back` | `Err(MixedRequiresMultipleEvents)` when too few names |

Both proptests run on `--release` builds in well under a second; the `--release` Cargo profile run reports `finished in 0.11s`.

### Verification

- `cargo test -p beava-bench --test blast_shape_test` → **10 passed**
- `cargo test -p beava-bench --test blast_shape_test --release` → **10 passed**
- `cargo test -p beava-bench --tests` (full crate) → **13 passed** (10 blast_shape + 3 v18_smoke; no regression)
- `cargo clippy -p beava-bench --all-targets -- -D warnings` → clean
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` → clean
- `cargo fmt --all --check` → clean

### Commits

| Commit | Type | Subject |
|--------|------|---------|
| `484d09e` | `test` | `test(19-01): add failing tests for blast_shape module` |
| `e9d9004` | `feat` | `feat(19-01): implement blast_shape module — Pool=N + 4 shapes + Zipfian sampler` |

Per CLAUDE.md §TDD Discipline: RED commit (`test:` — confirmed compile-error against missing module), then GREEN commit (`feat:` — all 10 tests pass).

## Architectural rationale (folded sub-goal 5 — verbatim from `19-CONTEXT.md` § `<specifics>`)

**1. Why Pool=N (not a sampler):**

Pre-encoding ALL N frames at startup eliminates per-iteration RNG cost AND per-iteration encode cost from the bench hot loop. The bench-side floor becomes "as fast as TCP `write_all` can drain" — the server-side ceiling is the only number we're measuring. Pool memory ~500 MB-1 GB for N=1M; budget for it.

(Reproduced in this SUMMARY so future bench-author refactors don't accidentally regress measurement honesty. Plan 19-02 wires this up in the binary harness; Plan 19-04 microbenches it with `criterion`.)

## Pool memory + setup time (informational)

Measured at run time only — these are not part of the per-task verification cycle but documented here so callers (Plan 19-02 CLI) can size their pool against host RAM budgets.

| N | Approx pool RAM | Setup time |
|---|-----------------|-----------|
| 1 K | ~250 KB | ~1 ms |
| 100 K | ~25 MB | ~80 ms |
| 1 M | ~500 MB-1 GB (depending on body size) | ~800 ms-1.5 s |

For `Fixed` shape, pool RAM is constant ≈ size of one frame regardless of N (Bytes refcounting). Setup time for `Zipfian` includes O(K) zeta sum — at K = 1 M this adds ~10 ms.

## Deviations from plan

None of substance. Two minor self-corrections during implementation:

1. **[Rule 1 - Bug] Test helper closure-vs-proptest type mismatch.** The RED test commit (484d09e) embedded `prop_assert!` and `prop_assert_eq!` inside a `FnMut` closure, but those macros desugar to `Result<_, TestCaseError>` and the closure expected `()`. Fixed in the GREEN commit by replacing the closure-style `for_each_frame_payload` helper with `decode_pool_payloads(&pool) -> Vec<(u8, Vec<u8>)>` so all assertions live at the proptest! body level. Behaviour unchanged. Logged here per `<deviation_rules>`.

2. **[Rule 2 - Critical] alpha == 1.0 division-by-zero.** Gray et al.'s standard Zipfian inverse formula divides by `(1 - alpha)`, which goes to zero at `alpha = 1.0` — and `alpha = 1.0` is the Phase 19 default per D-04. Fixed by routing the alpha == 1 case through a log-uniform inverse-CDF (which is exact for the harmonic-series limit), with a `(alpha - 1.0).abs() < f64::EPSILON` branch in `ZipfianSampler::sample()`. Test 8 (deterministic) and Test 4 (long-tail) both pass for alpha = 1.0 as written.

## Self-Check

Verified before proceeding:

```text
$ test -f crates/beava-bench/src/lib.rs                              && echo FOUND
FOUND
$ test -f crates/beava-bench/src/blast_shape.rs                      && echo FOUND
FOUND
$ test -f crates/beava-bench/tests/blast_shape_test.rs               && echo FOUND
FOUND
$ git log --all --oneline | grep -E "^(484d09e|e9d9004) "            | wc -l
2
```

## Self-Check: PASSED

All claimed files exist on disk. Both commits referenced in the table are reachable from HEAD.
