---
phase: 2
slug: tcp-server-and-binary-protocol
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-09
---

# Phase 2 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust cargo test (edition 2021) |
| **Config file** | Cargo.toml |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~10 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --lib`
- **After every plan wave:** Run `cargo test`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 10 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 02-01-01 | 01 | 1 | SRV-01 | — | N/A | unit | `cargo test protocol` | ❌ W0 | ⬜ pending |
| 02-01-02 | 01 | 1 | SRV-02 | — | N/A | unit | `cargo test protocol` | ❌ W0 | ⬜ pending |
| 02-02-01 | 02 | 1 | SRV-03 | — | N/A | integration | `cargo test --test tcp` | ❌ W0 | ⬜ pending |
| 02-02-02 | 02 | 1 | SRV-04 | — | N/A | integration | `cargo test --test tcp` | ❌ W0 | ⬜ pending |
| 02-03-01 | 03 | 2 | SRV-05 | — | N/A | integration | `cargo test --test tcp` | ❌ W0 | ⬜ pending |
| 02-03-02 | 03 | 2 | SRV-06 | — | N/A | integration | `cargo test --test tcp` | ❌ W0 | ⬜ pending |
| 02-04-01 | 04 | 2 | SRV-07 | — | N/A | integration | `cargo test --test tcp` | ❌ W0 | ⬜ pending |
| 02-04-02 | 04 | 2 | SRV-08 | — | N/A | integration | `cargo test --test http` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/tcp_integration.rs` — integration test stubs for TCP protocol commands
- [ ] `tests/http_integration.rs` — integration test stubs for HTTP management API

*If none: "Existing infrastructure covers all phase requirements."*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| MSET cooperative yielding observable | SRV-07 | Timing-dependent interleaving | Send MSET 10K + concurrent PUSH, verify PUSH responses arrive during MSET processing |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
