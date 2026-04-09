---
phase: 6
slug: foundation
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-09
---

# Phase 6 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test framework (`cargo test`) |
| **Config file** | none — existing infrastructure |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~15 seconds |

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
| 06-01-01 | 01 | 1 | OPS-02 | — | N/A | unit | `cargo test --lib store` | ✅ | ⬜ pending |
| 06-01-02 | 01 | 1 | OPS-02 | — | N/A | unit | `cargo test --lib eviction` | ✅ | ⬜ pending |
| 06-02-01 | 02 | 1 | ELOG-01 | — | N/A | unit | `cargo test --lib event_log` | ❌ W0 | ⬜ pending |
| 06-02-02 | 02 | 1 | ELOG-03 | — | N/A | unit | `cargo test --lib event_log` | ❌ W0 | ⬜ pending |
| 06-02-03 | 02 | 1 | ELOG-04 | — | N/A | unit | `cargo test --lib event_log` | ❌ W0 | ⬜ pending |
| 06-02-04 | 02 | 1 | ELOG-05 | — | N/A | unit | `cargo test --lib event_log` | ❌ W0 | ⬜ pending |
| 06-03-01 | 03 | 2 | OPS-01 | — | N/A | unit | `cargo test --lib protocol` | ✅ | ⬜ pending |
| 06-03-02 | 03 | 2 | ELOG-02 | — | N/A | integration | `cargo test` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_event_log.rs` — stubs for ELOG-01 through ELOG-05
- [ ] Event log test fixtures — temp directory setup with stream log files

*Existing test infrastructure covers EntityState, eviction, and protocol tests.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Event log writes don't degrade PUSH p99 | ELOG-03 | Latency measurement requires benchmarking | Run `cargo bench -- throughput` before and after, compare p99 |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
