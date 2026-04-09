---
phase: 4
slug: persistence-and-operational-readiness
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-09
---

# Phase 4 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test framework (cargo test) |
| **Config file** | none — standard Cargo.toml test config |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~10 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib`
- **After every plan wave:** Run `cargo test`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Nyquist Compliance

**Strategy: TDD-inline.** All plans use `tdd="true"` on code-producing tasks. Tests are
created alongside implementation within each task's `<behavior>` block. No separate
Wave 0 plan is needed because:

1. Plan 01 Task 2 creates unit tests in `src/state/snapshot.rs`, `src/state/store.rs`,
   `src/state/eviction.rs`, and `src/engine/pipeline.rs` (inline `#[cfg(test)]` modules).
2. Plan 02 Task 2 creates `tests/test_snapshot.rs` integration tests covering snapshot
   round-trip, version mismatch, and eviction behavior.
3. Plan 03 Task 2 creates HTTP endpoint integration tests in `tests/test_server.rs`.

Every task with `tdd="true"` writes tests before or alongside implementation, satisfying
the Nyquist requirement that every verify has an automated command.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|------|--------|
| 4-01-01 | 01 | 1 | PERS-01 | — | OperatorState enum delegates push/read | unit | `cargo test --lib` | src/state/snapshot.rs (inline) | ⬜ pending |
| 4-01-02 | 01 | 1 | PERS-02, PERS-05 | T-04-01 | Snapshot round-trip, version mismatch, eviction | unit | `cargo test snapshot && cargo test evict` | src/state/snapshot.rs, src/state/eviction.rs, src/state/store.rs (inline) | ⬜ pending |
| 4-02-01 | 02 | 2 | PERS-01, PERS-03, PERS-04 | T-04-05 | Startup recovery, periodic timers | build | `cargo build` | src/main.rs | ⬜ pending |
| 4-02-02 | 02 | 2 | PERS-03, PERS-05 | T-04-06 | Snapshot + eviction integration | integration | `cargo test --test test_snapshot` | tests/test_snapshot.rs | ⬜ pending |
| 4-03-01 | 03 | 3 | SRV-08 | T-04-09 | HTTP endpoints, metrics with push_latency | unit+build | `cargo test --lib` | src/server/http.rs, src/server/tcp.rs | ⬜ pending |
| 4-03-02 | 03 | 3 | SRV-08 | — | HTTP endpoint integration tests | integration | `cargo test --test test_server` | tests/test_server.rs | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

No separate Wave 0 plan needed. All test files are created within their respective plan tasks:

- [x] `src/state/snapshot.rs` inline tests — created by Plan 01 Task 2
- [x] `src/state/eviction.rs` inline tests — created by Plan 01 Task 2
- [x] `src/state/store.rs` inline tests — created by Plan 01 Task 2
- [x] `src/engine/pipeline.rs` inline tests — created by Plan 01 Task 2
- [x] `tests/test_snapshot.rs` — created by Plan 02 Task 2
- [x] `tests/test_server.rs` (new HTTP tests appended) — created by Plan 03 Task 2

*Existing test infrastructure covers framework requirements.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Kill-restart recovery | PERS-01 | Requires process kill/restart cycle | Start server, push events, kill -9, restart, verify GET returns pre-crash features |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (TDD-inline strategy: no MISSING refs)
- [x] No watch-mode flags
- [x] Feedback latency < 15s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved (TDD-inline strategy)
