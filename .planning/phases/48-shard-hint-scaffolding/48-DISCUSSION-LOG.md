# Phase 48: shard-hint-scaffolding - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-18
**Phase:** 48-shard-hint-scaffolding
**Areas discussed:** Where shard_hint is computed · SPSC roundtrip bench strategy · Micro-bench location and cadence

---

## Gray-area selection

| Gray area | Selected? |
|---|---|
| Hash function choice (ahash vs xxhash vs FxHasher) | — (not selected; default → ahash already in tree) |
| Where `shard_hint` is computed | ✓ |
| SPSC roundtrip bench strategy | ✓ |
| Micro-bench location and cadence | ✓ |

---

## Where `shard_hint` is computed

| Option | Description | Selected |
|--------|-------------|----------|
| Inside TCP/HTTP parsers | Parser produces `(event, shard_hint)` tuple; downstream reads the hint. Most efficient. | |
| Inside `handle_push_core_ex` | Central choke point; simpler initial scaffold. | |
| Both — parser computes, core_ex asserts | Belt-and-suspenders debug assert. | |

**User's choice:** Other (free text) — "Shard hint is not pushed down stream it just determined which shard input land at."

**Notes:** Important architectural clarification. `shard_hint` is a **routing function, not a propagated field.** Computed at dispatch, used to select shard inbox, then discarded. Event struct carries no shard_hint. Downstream code (shard threads, operators, logs) never reads it. Recorded as D-01 in CONTEXT.md.

---

## SPSC roundtrip bench strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Stub shard + real bench | Spawn pinned thread, crossbeam receiver, real roundtrip. | |
| Primitives-only bench (no shard) | Just channel send/recv timing. | |
| Defer SPSC bench to Wave 1 | Hash bench only in Wave 0; SPSC measured when real shard exists. | ✓ |

**User's choice:** Defer SPSC bench to Wave 1.
**Notes:** Recorded as D-08. TPC-INFRA-01 wording ("hash AND SPSC") is satisfied across Waves 0+1, not Wave 0 alone.

---

## Micro-bench location + cadence

| Option | Description | Selected |
|--------|-------------|----------|
| New `benches/shard_scaffold.rs` + CI nightly | Criterion under `benches/`, nightly reference-box run. | ✓ |
| Extend existing 9-cell harness | Add cells to `benchmark/`. | |
| Standalone `benches/` + per-PR CI | Run every PR, risk of criterion variance noise. | |

**User's choice:** New `benches/shard_scaffold.rs` + CI nightly.
**Notes:** Recorded as D-06 and D-07. 9-cell matrix remains per-PR hard regression gate; criterion bench is nightly-only to avoid noise-driven false alarms.

---

## Claude's Discretion

- Exact criterion bench harness shape (`Bencher::iter` vs `BenchmarkGroup`).
- Trait default-impl body (`ahash::AHasher` vs `std::hash::Hasher` shim — whichever meets <100 ns budget).

## Deferred Ideas

- SPSC roundtrip bench → Wave 1 (Phase 49).
- `BEAVA_SHARDS` env + `--shards` CLI → Wave 1.
- Hash-function swap if distribution problems surface → Wave 2 ship-gate revisit.
- Upstream `shard_hint` fast-path for fork/replica → Wave 4 (TPC-CORR-06).
- Python SDK `shard_key=` surface → Wave 1 (TPC-DX-01).
