# Plan 10-07 — Bench + perf row + throughput row + VERIFICATION — SUMMARY

**Status:** DONE

## What landed

- `crates/beava-core/benches/phase10_sketches.rs` — 20 criterion benches across 6 group functions (count_distinct, percentile, top_k, bloom, entropy + windowed 1Mevt fold)
- `crates/beava-core/Cargo.toml` `[[bench]]` entry for phase10_sketches (harness=false)
- `crates/beava-bench/configs/medium-with-sketches.json` — medium + count_distinct + percentile (5→7 features)
- `crates/beava-bench/configs/large-with-sketches.json` — large + 5 sketches (15→20 features)
- `.planning/phases/10-sketch-operators/10-perf-row.md` — bench medians per hw-class, mode flagged as "quick" (regression tripwire established)
- `.planning/phases/10-sketch-operators/10-throughput-row.md` — HTTP rows for both sketch-augmented pipelines
- `.planning/phases/10-sketch-operators/10-VERIFICATION.md` — all 5 SCs PASS / PASS-WITH-NOTE with evidence pointers

## Key bench numbers (Darwin 24.3.0 / 10 cores, M-series)

- bloom_member query: 8.6 ns
- count_distinct HLL update: 23 ns
- count_distinct exact-array update: 17 ns
- percentile UDDSketch update: 111 ns
- top_k hybrid update: 260 ns (Plan 22-04 O(log k) HashMap heap-position index)
- bloom insert: 95 ns
- entropy query: 254 ns

Windowed 1M-event tight loops:
- HLL: 822 µs (≈822 ns/elem amortized)
- CMS: 5.0 ms (≈5 ns/elem)
- UDDSketch: 22.1 ms (≈22 ns/elem)
- Entropy: 75 ms (dominated by `format!()` String allocs in test fixture)

## Throughput numbers

- medium-with-sketches/HTTP: 982 EPS (within 1% of Phase 7.5 small/HTTP baseline of 990 EPS)
- large-with-sketches/HTTP:  976 EPS (within 2%)

macOS `F_FULLSYNC` ceiling (~7.4 ms P50) dominates wall time; sketch CPU cost invisible at this hw-class.

## Open follow-ups

1. Re-run throughput with `--transport tcp` after Phase 8 sibling wires TCP push.
2. Re-run benches at full-fidelity (drop `--warm-up-time 1 --measurement-time 3 --sample-size 10`) at orchestrator merge time to refine the perf-row.md numbers.
3. Cross-bucket merge for windowed `count_distinct` / `percentile` / `top_k` (v0.1).
4. Python SDK helpers (`bv.count_distinct`, etc.) — thin wrapper task for Phase 11.

## Gates

- `cargo fmt --all --check` — clean
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — clean
- `cargo test --workspace --features beava-server/testing -- --test-threads=1` — 705 passed
- `cargo bench -p beava-core --bench phase10_sketches --no-run` — clean compile
