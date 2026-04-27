---
phase: 19-1m-bench
plan: 04
subsystem: bench-harness
tags: [bench, criterion, microbench, blast-shape, perf-baseline, phase-19]
provides:
  - "criterion microbench at crates/beava-bench/benches/blast_shape_bench.rs"
  - "Phase 19 start-of-line baselines in .planning/perf-baselines.md (apple-m4 hw-class)"
  - "CLAUDE.md §Performance Discipline gate satisfied for Phase 19"
requires:
  - "beava_bench::blast_shape (Plan 19-01)"
  - "criterion 0.5 (workspace dep)"
affects:
  - "Plan 19-05 (throughput run) — can reference these microbench numbers when discussing the bench-side cost contribution"
  - "future Phase 19.x tuning — sampler floor at ~55 Melem/s zipfian / ~146 Melem/s uniform; pool builder at ~2 Melem/s for non-fixed shapes"
key-files:
  created:
    - "crates/beava-bench/benches/blast_shape_bench.rs"
    - ".planning/phases/19-1m-bench/19-04-SUMMARY.md"
  modified:
    - "crates/beava-bench/Cargo.toml"
    - ".planning/perf-baselines.md"
decisions:
  - "Bench uses N=10_000 (not N=1_000_000) so criterion's default 10s warm + 100 sample budget doesn't burn ~30 minutes per bench. Future regressions detect the same way: a 10% slowdown at N=10k = a 10% slowdown at N=1M."
  - "BenchmarkId intentionally NOT imported — none of the six bench functions use parameterized benches. Re-add it (and adopt parameterized benches) only if a future Phase 19.x sweep over multiple K values needs it. Documented inline in the bench file (Warning 7 fix)."
  - "sample_uniform uses `rand::Rng::gen_range` directly (not BlastShape::Uniform via build_pool), giving a known-cheap reference point against which sample_zipfian can be measured."
metrics:
  duration: "~7 minutes"
  completed: "2026-04-26"
  tasks: 2
  benches_added: 6
  lines_added_bench: 173
---

# Phase 19 Plan 04: blast_shape criterion microbench — Summary

Added a 6-function criterion microbench at `crates/beava-bench/benches/blast_shape_bench.rs`
covering Plan 19-01's `blast_shape` module (Pool=N builder + ZipfianSampler) and recorded
the start-of-line baselines for Phase 19 in `.planning/perf-baselines.md` under hw-class
`apple-m4`. CLAUDE.md §Performance Discipline gate satisfied for Phase 19 — `files_modified`
includes a path under `crates/*/benches/`.

## What landed

### `[[bench]]` wiring

`crates/beava-bench/Cargo.toml`:

- `[dev-dependencies]` += `criterion = { workspace = true }`.
- New `[[bench]]` block at the end:
  ```toml
  [[bench]]
  name = "blast_shape_bench"
  harness = false
  ```

The criterion dep was already workspace-pinned at `0.5` with the `html_reports` feature
(default-features off) — no workspace-level changes needed.

### Six bench functions

`crates/beava-bench/benches/blast_shape_bench.rs` (173 lines):

| # | Group / function | Workload |
|---|------------------|----------|
| 1 | `build_pool/fixed/n_10000` | one pre-encoded frame, refcount-cloned 10 000 times via `Bytes` |
| 2 | `build_pool/uniform/n_10000_k_1000` | 10 000 distinct frames, K=1000 uniform sampling, JSON envelope encode per frame |
| 3 | `build_pool/zipfian/n_10000_k_1000_alpha_1.0` | 10 000 distinct frames, K=1000 hand-rolled Zipfian (α=1.0 log-uniform inverse-CDF branch) |
| 4 | `build_pool/mixed/n_10000_m_3` | 10 000 frames, 3 round-robin event names (`A`/`B`/`C`), key cardinality 1M default |
| 5 | `sampler/sample_zipfian/k_1000_alpha_1.0` | single-sample throughput of `ZipfianSampler::sample()` |
| 6 | `sampler/sample_uniform/k_1000` | single-sample baseline using `rand::Rng::gen_range` over `StdRng` |

`Throughput::Elements(10_000)` for build_pool benches and `Throughput::Elements(1)` for
sampler benches so criterion reports per-element rates in the HTML reports.

### Why N = 10 000 (not 1 000 000)

Criterion's default 10s warm + 100 sample budget would burn ~30 minutes per bench at
N = 1 M. N = 10 k gives a per-iter cost in the µs-ms range that fits criterion's sample
budget while still amortising per-frame encode cost. Regression detection scales: if the
Pool=N builder slows by 10% at N=10k, it also slows by 10% at N=1M. The relative numbers
are what the gate watches.

## Measured medians (Apple-M4, Darwin-24.3.0, 10 cores)

| Bench | Median | Throughput |
|---|---|---|
| `build_pool/fixed/n_10000` | **46.344 µs** | 215.78 Melem/s |
| `build_pool/uniform/n_10000_k_1000` | **12.528 ms** | 798.22 Kelem/s |
| `build_pool/zipfian/n_10000_k_1000_alpha_1.0` | **5.2559 ms** | 1.9026 Melem/s |
| `build_pool/mixed/n_10000_m_3` | **5.1835 ms** | 1.9292 Melem/s |
| `sampler/sample_zipfian/k_1000_alpha_1.0` | **18.384 ns** | 54.395 Melem/s |
| `sampler/sample_uniform/k_1000` | **6.8615 ns** | 145.74 Melem/s |

Numbers also recorded in `.planning/perf-baselines.md` under
`### Phase 19 — blast_shape sampler + pool builder (criterion microbench)` (hw-class
`apple-m4`). Future bench changes regress against these per CLAUDE.md
§Performance Discipline (+10% WARN / +25% BLOCK).

## Reproduction recipe

```bash
cargo bench -p beava-bench --bench blast_shape_bench
```

Total wall-clock: ~6 minutes for all 6 benches (criterion warmup + 100 samples each).
Outputs HTML reports under `target/criterion/` (gitignored — see threat model in
PLAN.md).

For a quick smoke check (no measurement, just compile + run):

```bash
cargo bench -p beava-bench --bench blast_shape_bench -- --test
```

For comparing future runs against this baseline (Plan 19-05+ pattern):

```bash
# Save the current numbers as the "19" baseline:
cargo bench -p beava-bench --bench blast_shape_bench -- --save-baseline 19
# In a later phase, compare:
cargo bench -p beava-bench --bench blast_shape_bench -- --baseline 19
```

## Pool memory budget at N=1M (extrapolated from the n=10k numbers)

The 10k pool occupies roughly:

| Shape | n=10k pool size estimate | n=1M extrapolation |
|---|---|---|
| `fixed` | ~150 B (one frame + N refcount-clone Bytes headers) | ~16 MB (almost all the cost is the per-Bytes header at N=1M) |
| `uniform` | ~1-3 MB (10k full envelopes, ~100-300 B/frame) | ~100-300 MB |
| `zipfian` | ~1-3 MB | ~100-300 MB |
| `mixed` | ~1-3 MB | ~100-300 MB |

Plan 19-01 SUMMARY's "~500 MB-1 GB at N=1M" estimate is the worst case for shapes with
larger event bodies than this bench's 6-field fixture. For the small-pipeline shape used
here (one entity field + `event_time` + one extra `f64`), the per-frame body is ~50-80 bytes
of JSON, so the N=1M pool lands closer to 100-300 MB. Operators sizing for Plan 19-02's
`--total-events 1_000_000` invocation should budget host RAM accordingly (D-02
architectural rationale).

## Observations

- **`fixed` is ~270× faster than `uniform`** because Bytes refcount clones don't repeat
  the envelope encode. This is by design — `fixed` is the cache-warm marketing peak, and
  the gap between fixed and the other three shapes quantifies the per-frame encode cost
  that real distributions pay.
- **`uniform` (12.5 ms) is ~2.4× slower than `zipfian` (5.3 ms) and `mixed` (5.2 ms)**
  even though `gen_range` is cheaper than the Zipfian sampler. This is allocator-driven:
  uniform spreads writes across 1000 distinct `format!("k{:08}")` strings whereas Zipfian
  concentrates writes on the rank-0 / rank-1 hot keys (which dominate sampling at α=1).
  More allocator cache hits when the keyspace is skewed. This is a property of the bench
  fixture (1 string-typed entity field), not a regression risk — if Plan 19-05's pipeline
  config has a different key shape, the relative numbers will shift, but the fixture-level
  ratio among shapes will hold.
- **`mixed` matches `zipfian` within 1.4%** — the encode path dominates, not the per-frame
  event-name dispatch. Confirms D-01's expectation that mixed/zipfian are essentially
  interchangeable from a bench-side cost perspective.
- **`sample_zipfian` is ~2.7× slower than `sample_uniform` (18 ns vs 7 ns)** — the α=1
  log-uniform inverse-CDF branch needs `ln`/`exp` on the hot path. Sampler floor is
  ~55 Melem/s — comfortably above the 1M-EPS Phase 19 target (sampler runs once per frame
  during pool build, not on the saturation hot path; D-02 setup-time-excluded).

## Hooks for Plan 19-05

When Plan 19-05 (the phase's throughput run) writes its ledger row analysis, it can
reference these microbench numbers to decompose where bench-side cost goes:

- Pool build at N=1M with shape=zipfian: ~525 ms predicted (5.26 ms × 100). Captured
  as setup time (excluded from `wall_clock_ms` per D-02).
- Per-frame encode cost during pool build: ~525 ns (5.26 ms / 10k). Compare to apply
  thread per-event work: 888 ns mean (Phase 18-12). The bench's pool build runs *once*
  during sender setup, then the hot loop is `write_all(&pool[i])` only — bench-side
  per-event cost during measurement is zero.
- ZipfianSampler doesn't appear on the hot loop at all; it runs only inside `build_pool`.

## Deviations from plan

None of substance. One minor rustfmt fixup:

1. **[Rule 1 - Bug] rustfmt reformatted the `BlastShape::Uniform { cardinality: 1_000 }`
   call to a single line.** Plan draft had the call broken across multiple lines for
   readability; rustfmt collapsed it to one line. No semantic change. Confirmed via
   `cargo fmt --all --check` clean exit; behaviour identical.

## Verification

| Gate | Result |
|---|---|
| `cargo bench -p beava-bench --bench blast_shape_bench --no-run` | exit 0 (compiles) |
| `cargo bench -p beava-bench --bench blast_shape_bench` | 6 measurements, 6 `time:` reports |
| `cargo clippy -p beava-bench --benches -- -D warnings` | exit 0 (Warning 7 invariant: no `BenchmarkId` import) |
| `cargo fmt --all --check` | exit 0 |
| `grep "Phase 19 — blast_shape" .planning/perf-baselines.md` | 1 match |
| Bench rows in Phase 19 section | 6 (one per criterion bench) |

## Commits

| Commit | Type | Subject |
|---|---|---|
| `44f0ae6` | `test` | `test(19-04): scaffold criterion bench harness for blast_shape module` |
| `9c3bcfd` | `feat` | `feat(19-04): add criterion microbench for blast_shape + record Phase 19 baselines` |

Per CLAUDE.md §TDD Discipline: RED commit (`test:` — stub bench compiles and runs but
black-boxes a constant; criterion `--test` mode confirms the harness wires up), then
GREEN commit (`feat:` — replaces stub with 6 real measurements + appends baselines).

## Self-Check

Verified before finalising:

```text
$ test -f crates/beava-bench/benches/blast_shape_bench.rs                && echo FOUND
FOUND
$ test -f .planning/perf-baselines.md                                    && echo FOUND
FOUND
$ git log --oneline | grep -E "^(44f0ae6|9c3bcfd) "                      | wc -l
2
$ grep -c "Phase 19 — blast_shape" .planning/perf-baselines.md
1
$ grep -cE "^fn bench_" crates/beava-bench/benches/blast_shape_bench.rs
6
$ grep -E 'use criterion::.*BenchmarkId' crates/beava-bench/benches/blast_shape_bench.rs | wc -l
0
```

## Self-Check: PASSED

All claimed files exist on disk. Both commits referenced in the table are reachable from
HEAD. Phase 19 section appended to perf-baselines.md. 6 bench functions defined. Warning 7
invariant holds (no `BenchmarkId` import).
