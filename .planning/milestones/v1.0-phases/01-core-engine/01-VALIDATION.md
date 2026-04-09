---
phase: 1
slug: core-engine
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-09
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test framework (`cargo test`) |
| **Config file** | Cargo.toml |
| **Quick run command** | `cargo test --lib` |
| **Full suite command** | `cargo test` |
| **Estimated runtime** | ~5 seconds |

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
| 1-01-01 | 01 | 1 | ENG-01 | — | N/A | unit | `cargo test state` | ❌ W0 | ⬜ pending |
| 1-01-02 | 01 | 1 | ENG-02 | — | N/A | unit | `cargo test window` | ❌ W0 | ⬜ pending |
| 1-02-01 | 02 | 1 | ENG-03 | — | N/A | unit | `cargo test operators::count` | ❌ W0 | ⬜ pending |
| 1-02-02 | 02 | 1 | ENG-04 | — | N/A | unit | `cargo test operators::sum` | ❌ W0 | ⬜ pending |
| 1-02-03 | 02 | 1 | ENG-05 | — | N/A | unit | `cargo test operators::avg` | ❌ W0 | ⬜ pending |
| 1-03-01 | 03 | 2 | ENG-06 | — | N/A | unit | `cargo test expression::parse` | ❌ W0 | ⬜ pending |
| 1-03-02 | 03 | 2 | ENG-07 | — | N/A | unit | `cargo test expression::eval` | ❌ W0 | ⬜ pending |
| 1-03-03 | 03 | 2 | ENG-08 | — | N/A | unit | `cargo test expression::missing` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `Cargo.toml` — project setup with dependencies (ahash, winnow, thiserror, serde, serde_json, postcard)
- [ ] `src/main.rs` — minimal entry point
- [ ] `src/types.rs` — FeatureValue, Timestamp, TallyError types

*If none: "Existing infrastructure covers all phase requirements."*

---

## Manual-Only Verifications

*All phase behaviors have automated verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
