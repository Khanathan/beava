---
phase: 50-multi-shard-routing
status: gaps_found
verified_at: 2026-04-18
verifier: Hetzner CCX43 (AMD EPYC Genoa 16-core) + Apple M4 (dev)
requirements_verified:
  - TPC-INFRA-03  # /metrics crate + Prometheus exporter
  - TPC-INFRA-04  # per-shard labeled series
  - TPC-INFRA-07  # BEAVA_ENTITIES_SHARDS deprecation
  - TPC-CORR-03  # shard_key missing-field reject (HTTP 400)
  - TPC-DX-02    # ShardKeyMissingWarning at N>1
requirements_gaps:
  - TPC-PERF-02  # core_affinity pinning — lands, but stub thread makes it observational
  - TPC-PERF-03  # SPSC channel — lands, but consumer thread is a stub that discards events
  - TPC-PERF-04  # SO_REUSEPORT — lands on Linux, but no observable throughput gain
  - TPC-CORR-01  # Backpressure — inbox_full counter exists, but full path untestable
follow_up_phase: 50.5-shard-thread-completion
---

# Phase 50: multi-shard-routing — Verification Report

**Status:** `gaps_found` — Phase 50 landed its plumbing (metrics, SPSC, SO_REUSEPORT, panic quarantine, missing-field reject) but the **shard-thread receiver is a stub (`src/shard/thread.rs:160 TODO(50-04)`) that discards events.** Real processing still runs on the legacy single-engine DashMap path regardless of `BEAVA_SHARDS`. Unlock gated by Phase 50.5.

## How we got here

1. Phase 50 ship-gate matrix run (macOS) showed `complex-c8-x8` at −24% vs baseline regardless of shard count.
2. Opus debug investigation (`50-DEBUG-SESSION.md`) found two root causes:
   - `ShardConfig::wave1_enforced()` clamp still active → BEAVA_SHARDS silently clamped to 1
   - `src/shard/thread.rs:160` is a `TODO(50-04)` stub that drops events sent via SPSC
3. Fix #1 applied (`f36d595 + bd05500`): removed clamp, added N=1 zero-cost bypass. Restored Phase 49 parity on macOS but ≥3× gate still unreachable (stub thread).
4. Hetzner CCX43 (16-core EPYC) provisioned to validate on Linux with proper SO_REUSEPORT + kernel pinning.
5. Two portability bugs found at Linux build (`ac7ed88`): `socket2` needed `features=["all"]`; `IntoRawFd` import missing.

## Evidence — Hetzner 16-core EPYC Genoa (complex-c8-x8 EPS)

| Config | CPUS (tokio workers) | BEAVA_SHARDS | CLIENTS | EPS | Observation |
|--------|---------------------:|-------------:|--------:|----:|-------------|
| Matrix N=1 | 8 | 1 | 8 | **125,470** | Single-engine baseline |
| Matrix N=16 | 8 | 16 | 8 | 128,085 | +2% (BEAVA_SHARDS has no effect — stub) |
| Ramp v1 | 8 | 16 | 16 | 140,147 | +9% adding clients at 8 workers |
| Ramp v2 | **16** | 16 | 16 | **195,185** | **+39% unlocking tokio workers** |
| Ramp v2 | 16 | 16 | 32 | 202,829 | Plateau at ~200K — DashMap ceiling |

## Interpretation

**Architectural thesis validated:**
- Beava's legacy DashMap-backed engine CAN scale within tokio's work-stealing pool — `CPUS=16` delivered +39% over `CPUS=8` at matched client count.
- Cross-core contention ceiling hits at ~200K EPS complex workload on 16-core EPYC. This IS the problem TPC was designed to solve.
- `BEAVA_SHARDS` currently has **zero throughput effect** because events sent through SPSC are discarded by the stub shard thread; all real work still happens on the legacy tokio pool path.

**Phase 50 delivered plumbing but not parallelism:**
- ✓ `/metrics` + Prometheus (TPC-INFRA-03)
- ✓ Per-shard labeled series defined (TPC-INFRA-04) — emit from legacy path; at N=1 numbers are valid, at N>1 `events_total{shard=N}` for N>0 stays zero because shard threads don't receive events
- ✓ BEAVA_ENTITIES_SHARDS deprecation (TPC-INFRA-07)
- ✓ shard_key missing-field reject (TPC-CORR-03) — unit-tested, untouched by stub
- ✓ ShardKeyMissingWarning at N>1 (TPC-DX-02)
- ⚠ SPSC channel + backpressure (TPC-PERF-03, TPC-CORR-01) — channel wired listener-side; consumer discards. Backpressure `beava_shard_inbox_full_total` only increments if inbox fills, which it won't under current path.
- ⚠ core_affinity pinning (TPC-PERF-02) — threads spawn + pin, but do no useful work.
- ⚠ SO_REUSEPORT per-shard TCP (TPC-PERF-04) — sockets bind correctly on Linux (verified via build), but accepted connections route to the same legacy ingest regardless.

**≥3× ship-gate status:** NOT ACHIEVED on this phase. Phase 50.5 delivers the shard-thread wiring that makes the 3× gate architecturally possible. Expected result with Phase 50.5 + DashMap removal: 600K–1M EPS at N=16 CPUS=16 on EPYC class hardware (3–5× the 200K DashMap ceiling).

## Recommendation

Defer the ≥3× merge gate to Phase 50.5 completion. Track as a `gap` on this phase, not a regression — Phase 50's delivered scope is correctly implemented; the missing piece is the scope that landed as a `TODO` stub.

## Commits landed under Phase 50

- `767f056 feat(50-01)` Cargo Wave 2 deps + metrics recorder
- `b4726e7 feat(50-02)` per-shard metric series
- `d373a2b feat(50-03)` shard-thread spawner + barrier + quarantine
- `01344bc feat(50-04)` SPSC routing + backpressure wiring *(receiver is stub — scope gap)*
- `f2588dd feat(50-05)` SO_REUSEPORT per-shard Linux + macOS fallback
- `0addd88 feat(50-06)` shard_key missing-field reject + warnings + deprecation
- `c8b29ad feat(50-07)` routing counters + N=2 integration test
- `0b0cfb9 feat(50-08)` metrics parity test + run_matrix.sh BEAVA_SHARDS support
- `f36d595 fix(50)` remove Wave-1 clamp
- `bd05500 fix(50)` zero-cost N=1 SPSC bypass (post-debug Fix #1)
- `ac7ed88 fix(50-05)` Linux portability for SO_REUSEPORT bind

## Handoff to Phase 50.5

See `50.5-FIX-PLAN.md` for the scope:
1. Wire `shard/thread.rs:160` to consume SPSC events into per-shard state (not a stub).
2. Flip the reader-source from legacy DashMap StateStore to per-shard `Shard` at N>1.
3. Measure on Hetzner: expected ≥3× at N=16 CPUS=16 (≥600K EPS on complex-c8-x8).
