# Phase 6: WAL + idempotency - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-23
**Phase:** 06-wal-idempotency
**Mode:** --auto (all gray areas auto-selected with recommended defaults)
**Areas discussed:** Crate layout, WAL file format, Group-commit strategy, Idempotency cache, /push endpoint, WAL rotation + truncation, Durability UAT, Perf microbench

---

## Crate layout

| Option | Description | Selected |
|--------|-------------|----------|
| New `beava-persistence` crate | Keep fs/sync code out of core; future home for snapshot + recovery | ✓ |
| Put WAL in `beava-server` | Closer to HTTP handler | |
| Put WAL in `beava-core` | Single-crate simplicity | |

**User's choice:** `beava-persistence` (auto)
**Notes:** `beava-core` WASM-portability invariant codified 2026-04-23 forbids syscalls in core; Phase 7 snapshot + recovery will land in the same crate.

---

## WAL file format

| Option | Description | Selected |
|--------|-------------|----------|
| Length+CRC32C framing, JSON payload | Simple, hardware-accelerated CRC, debug-friendly body | ✓ |
| postcard / bincode binary | Compact but hit v1 serialization ceiling | |
| flat-buffers / capnproto | Zero-copy read but big dep | |

**User's choice:** Length+CRC32C + JSON payload (auto)
**Notes:** MessagePack migration deferred; Phase 6 bench captures today's cost.

---

## Group-commit strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Background fsync worker + tokio watch watermark | Clean fanout; one fsync per batch | ✓ |
| Sync fsync per push | Simpler but poor throughput | |
| Dedicated OS thread (not tokio) | Ultimate fsync determinism | |

**User's choice:** Background worker + watch watermark (auto)
**Notes:** Spawn-blocking for the fsync syscall itself; 2ms default coalesce.

---

## Idempotency cache

| Option | Description | Selected |
|--------|-------------|----------|
| HashMap + lazy expiry + periodic sweep | Simple; bounded by dedupe_window × rate | ✓ |
| LRU with configurable cap | Bounded memory but may evict valid entries | |
| External store (redis) | Out of scope (single-process) | |

**User's choice:** HashMap + sweep (auto)
**Notes:** No LRU cap in v0 — matches "size your box" project stance.

---

## /push endpoint

| Option | Description | Selected |
|--------|-------------|----------|
| Ship `POST /push/{event}` with `{ack_lsn,idempotent_replay,registry_version}` response | Phase 6 scope — features deferred to /push-sync | ✓ |
| Also ship /push-sync with features | Bigger scope — Phase 12 covers this | |
| Only wire /push-batch | Not scoped this phase | |

**User's choice:** /push only (auto)
**Notes:** `/push-sync`, `/push-batch`, `push_many` all land in Phase 12.

---

## WAL rotation + truncation

| Option | Description | Selected |
|--------|-------------|----------|
| Size-based segments (128 MiB) + truncate_up_to(snapshot_lsn) API | Clean handoff to Phase 7; fixed-size segments are simple | ✓ |
| Time-based rotation (hourly) | Less predictable under load | |
| Single growing file with hole-punching | Complex; not portable | |

**User's choice:** Size-based (auto)
**Notes:** Phase 6 exposes `truncate_up_to`; Phase 7 wires it.

---

## Durability UAT harness

| Option | Description | Selected |
|--------|-------------|----------|
| Subprocess spawn + SIGKILL, read WAL via `WalReader` post-mortem | Matches cli_smoke.rs pattern; real fsync behavior | ✓ |
| In-process fault injection | Faster but doesn't prove on-disk invariant | |
| fio-level synthetic WAL test | Tests the fs, not our code | |

**User's choice:** Subprocess + SIGKILL (auto)
**Notes:** Phase 6 proves the disk-level invariant; Phase 7 adds the restart-and-replay half.

---

## Perf microbench

| Option | Description | Selected |
|--------|-------------|----------|
| criterion bench: append_nofsync + append_fsync_2ms + append_fsync_burst_1k | Covers serialize cost + fsync overhead + group-commit amortization | ✓ |
| Single large EPS-throughput bench | Closer to Phase 13 gate but too coarse for regression tripwire | |
| Only append_nofsync | Misses the actual fsync cost the phase is about | |

**User's choice:** Three-bench suite (auto)
**Notes:** Numbers land in `.planning/perf-baselines.md` under Apple-M4 hw-class row.

---

## Claude's Discretion

- BytesMut vs Vec<u8> for WAL staging buffer — planner picks.
- `std::fs::File + spawn_blocking` vs `tokio::fs::File` for fsync — planner benchmarks.
- `crc32c` crate vs `crc` crate — planner picks (leaning `crc32c`).
- Whether to introduce `AppState` struct now or extend `DevAggState` — planner picks.

## Deferred Ideas

- TCP `op=push` handler wiring — Phase 12
- `/push-sync` with features — Phase 12
- WAL replay — Phase 7
- MessagePack WAL payload — optimization pass post-v0
- LRU cap on idempotency cache — operator-reported need
</content>
</invoke>
