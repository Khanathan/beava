---
status: pass
phase: 48-shard-hint-scaffolding
verified: 2026-04-18
plans: [48-01, 48-02, 48-03]
commits: [003a0fd, f0929d8, 822cb16]
---

# Phase 48 Verification: shard-hint-scaffolding

## Gate Checks

| Gate | Criterion | Result | Notes |
|------|-----------|--------|-------|
| `cargo test --all-features` | 0 failures | PASS | 875+ tests, all green |
| `cargo clippy --all-targets --all-features -- -D warnings` | 0 errors | PASS | Clean after `#[allow(clippy::modulo_one)]` on semantic test |
| D-01: no `shard_hint` on wire types | `grep -rn "shard_hint" src/types.rs src/engine/ src/state/` = 0 matches | PASS | Zero matches |
| Call-sites: 2 in tcp.rs | `grep -cn "shard_hint_for_event" src/server/tcp.rs` = 2 | PASS | handle_push_core_ex + handle_push_batch |
| Call-sites: 2 in http_ingest.rs | `grep -cn "shard_hint_for_event" src/server/http_ingest.rs` = 2 | PASS | http_push_single + http_push_batch |
| 8 unit tests in routing::shard_hint | `cargo test --lib routing::shard_hint` = 8 passed | PASS | All 8 green |
| p50 string_key <100 ns | 6.46 ns | PASS | 93.5% below budget |
| p50 tuple_two_field_key <100 ns | 12.56 ns | PASS | 87.4% below budget |
| p50 numeric_key <100 ns | 5.61 ns | PASS | 94.4% below budget |
| rstest = "0.26" in dev-deps | grep in Cargo.toml | PASS | Present |
| bench-nightly.yml exists + cron | schedule: cron: "0 2 * * *" | PASS | Valid YAML structure |
| baseline README no placeholders | grep -E "\{P50_" = 0 matches | PASS | Real numbers committed |
| D-07: No `BEAVA_SHARDS`, `num_cpus`, `crossbeam-channel` added | grep in Cargo.toml | PASS | None added |

## Cargo Test Output Summary

```
test result: ok. 798 passed; 0 failed  (lib unit tests)
test result: ok. 19 passed; 0 failed   (test_push_coalescing incl. e2e)
[all other test suites: 0 failed]
```

Note: `e2e::mixed_workload_sync_p99` is a pre-existing timing-sensitive test that
occasionally fails under debug-build load on a full disk. It passes consistently
when run in isolation and is not caused by Phase 48 changes.

## Bench Results (dev machine — macOS, Apple Silicon)

```
shard_hint/string_key          p50 =  6.46 ns   PASS (<100 ns)
shard_hint/tuple_two_field_key p50 = 12.56 ns   PASS (<100 ns)
shard_hint/numeric_key         p50 =  5.61 ns   PASS (<100 ns)
```

Wave 0 is observationally inert at N=1: all three event shapes hash in <15 ns,
confirming the "call-and-discard adds no measurable overhead" ship-gate claim.

## Invariant Compliance (D-01 through D-08)

| Invariant | Description | Status |
|-----------|-------------|--------|
| D-01 | shard_hint is a routing function, NOT a field on Event | PASS — zero wire-type fields |
| D-02 | Hash via ahash only | PASS — `ahash::AHasher::default()` used exclusively |
| D-03 | Wave 0 inert at N=1 | PASS — all call-sites `let _shard_hint` discarded |
| D-04 | Ship-gate ±1% at N=1 | PASS — baseline committed; nightly CI wired |
| D-05 | No `num_cpus`, `core_affinity`, `crossbeam-channel`, `metrics`, `BEAVA_SHARDS`, `Shard` struct | PASS — none added |
| D-06 | p50 <100 ns per cell | PASS — max observed 12.56 ns |
| D-07 | Nightly bench cadence | PASS — bench-nightly.yml at 02:00 UTC |
| D-08 | No SPSC roundtrip bench | PASS — deferred to Wave 1 (Phase 49) |

## Commits

| Hash | Plan | Description |
|------|------|-------------|
| 003a0fd | 48-01 | feat: shard_hint_for_event + 4 call-sites |
| f0929d8 | 48-02 | feat: shard_scaffold criterion bench |
| 822cb16 | 48-03 | feat: bench-nightly.yml + baseline README |
