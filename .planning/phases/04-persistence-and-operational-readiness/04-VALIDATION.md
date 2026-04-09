---
phase: 4
slug: persistence-and-operational-readiness
status: draft
nyquist_compliant: false
wave_0_complete: false
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

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 4-01-01 | 01 | 1 | PERS-01 | — | Snapshot serialization round-trips | unit | `cargo test snapshot` | ❌ W0 | ⬜ pending |
| 4-01-02 | 01 | 1 | PERS-02 | — | Snapshot write is non-blocking | integration | `cargo test snapshot_nonblocking` | ❌ W0 | ⬜ pending |
| 4-01-03 | 01 | 1 | PERS-05 | — | Version mismatch → clean startup | unit | `cargo test snapshot_version` | ❌ W0 | ⬜ pending |
| 4-02-01 | 02 | 1 | PERS-03 | — | TTL eviction removes idle keys | unit | `cargo test eviction` | ❌ W0 | ⬜ pending |
| 4-03-01 | 03 | 2 | SRV-08 | — | HTTP endpoints return correct data | integration | `cargo test http` | ❌ W0 | ⬜ pending |
| 4-03-02 | 03 | 2 | PERS-04 | — | Metrics in Prometheus format | integration | `cargo test metrics` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_snapshot.rs` — stubs for PERS-01, PERS-02, PERS-05
- [ ] `tests/test_eviction.rs` — stubs for PERS-03
- [ ] `tests/test_http.rs` — stubs for SRV-08, PERS-04

*Existing test infrastructure covers framework requirements.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Kill-restart recovery | PERS-01 | Requires process kill/restart cycle | Start server, push events, kill -9, restart, verify GET returns pre-crash features |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
