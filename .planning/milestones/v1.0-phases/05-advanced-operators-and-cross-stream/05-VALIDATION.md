---
phase: 5
slug: advanced-operators-and-cross-stream
status: approved
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-09
---

# Phase 5 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test framework (cargo test) |
| **Config file** | Cargo.toml (already configured) |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~5 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib`
- **After every plan wave:** Run `cargo test`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 5 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 5-01-01 | 01 | 1 | OPS-01, OPS-02, OPS-03, OPS-05 | T-5-01, T-5-03 | MinBucket/MaxBucket sentinels never returned to client; event_count check returns Missing | unit | `cargo test --lib engine::window::tests && cargo test --lib engine::operators::tests` | TDD inline | pending |
| 5-01-02 | 01 | 1 | OPS-03, OPS-05 | T-5-02, T-5-04 | Where expressions bounded AST; snapshot version rejects old format | unit | `cargo test` | TDD inline | pending |
| 5-02-01 | 02 | 1 | OPS-04 | T-5-05, T-5-07 | HLL memory bounded by design; ahash non-crypto accepted for cardinality | unit | `cargo test --lib engine::hll::tests` | TDD inline | pending |
| 5-02-02 | 02 | 1 | OPS-04 | T-5-06 | HLL merge O(30*16384) sub-microsecond | unit | `cargo test --lib engine::hll` | TDD inline | pending |
| 5-03-01 | 03 | 2 | OPS-04, XSTR-01, XSTR-02, XSTR-03 | T-5-08, T-5-10 | Lookup reads feature values only; view features validated at registration | unit+integration | `cargo test` | TDD inline | pending |
| 5-03-02 | 03 | 2 | XSTR-03 | T-5-09, T-5-11 | Fan-out bounded by stream count; qualified map O(features*2) | unit+integration | `cargo test` | TDD inline | pending |

*Status: pending -- all tasks use TDD (tests written before implementation)*

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements. All tasks use `tdd="true"` with inline `<behavior>` blocks that define tests before implementation. No separate Wave 0 needed.

---

## Manual-Only Verifications

All phase behaviors have automated verification.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (N/A -- TDD inline)
- [x] No watch-mode flags
- [x] Feedback latency < 5s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-04-09
