---
phase: 28-client-engine-embedding
plan: 03
subsystem: client
tags: [client, engine, feature-flags, integration-test]
requires: [28-01]
provides:
  - tally::client::Session (stub)
  - tally::client::SessionMode (Historical only)
  - tally::client::OutOfScopeError
  - client-features engine round-trip test harness
affects:
  - src/lib.rs (unconditional pub mod client)
  - src/client/mod.rs (new)
  - tests/client_engine_roundtrip.rs (new)
tech-stack:
  added: []
  patterns:
    - "Type surface lands a phase before implementation (Session stub → Phase 29 swaps fields for Phase 27 Scope)"
    - "Round-trip integration test as anti-regression guard for `#[cfg(feature = \"server\")]` gating"
key-files:
  created:
    - src/client/mod.rs
    - tests/client_engine_roundtrip.rs
    - .planning/phases/28-client-engine-embedding/28-03-SUMMARY.md
  modified:
    - src/lib.rs
decisions:
  - "Client module is unconditional (not behind #[cfg(feature=\"client\")]): the module imports nothing server-gated, so cost is ~80 LOC of compiled types; benefit is that server-side tests can reference client types without dual gating."
  - "Stub Session uses plain String/Vec<String> — NOT Phase 27's Scope struct — so Phase 28 can land before Phase 27 without cross-phase dependency."
  - "No extra cfg-gating needed in src/engine/pipeline.rs: the only server-only call site (SubscriberRegistry::notify_subscribers) was already gated by 28-01."
metrics:
  duration_minutes: ~20
  tests_added: 4
  completed: 2026-04-15
---

# Phase 28 Plan 03: Shared client module (Session stub + OutOfScopeError) + engine round-trip lockdown Summary

One-liner: Landed `tally::client` with stub `Session` / `SessionMode::Historical` / `OutOfScopeError`, confirmed the engine's `push` hot path has zero ungated server side-effects, and added a 2-event integration test that passes under `--no-default-features --features client` to lock it in.

## Audit result — `src/engine/pipeline.rs`

Grep (`crate::server|tally::server|signals::|SignalRegistry|SubscriberRegistry|LatencyTracker|ThroughputTracker|emit_[a-z_]+`) found 5 hits:

| Line | Match                                                              | Status                                                             |
| ---- | ------------------------------------------------------------------ | ------------------------------------------------------------------ |
| 447  | `Option<Arc<crate::server::replica::SubscriberRegistry>>` (field)  | Already `#[cfg(feature = "server")]` gated by 28-01 (line 445).    |
| 686  | doc comment for `install_subscribers`                              | Doc comment — no code emission.                                    |
| 696  | `registry: Arc<crate::server::replica::SubscriberRegistry>` (fn)   | Already `#[cfg(feature = "server")]` gated by 28-01 (line 693).    |
| 995  | `if let Some(reg) = &self.subscriber_registry` (ingest hook)       | Already `#[cfg(feature = "server")]` gated by 28-01 (line 994).    |
| 1239 / 1259 / 1261 | local variable `emit_live` in stream-stream join logic | False positive — regex hit on identifier, not on a server function call. Local bool controlling retraction emission order within the engine. Not a server side-effect. |

**Verdict:** zero unganted server-only side effects in the engine. CONTEXT D1's requirement ("cfg-gate in-engine emission") is structurally satisfied. No edits to `src/engine/pipeline.rs` required.

**Anti-regression guard:** if anyone adds a new ungated `crate::server::*` call into `push_internal` or its callees, the new `tests/client_engine_roundtrip.rs` fails to compile under `--features client` (server symbols are not in scope) — that's the automated lock.

## What landed

### `src/client/mod.rs` (108 lines)

- `pub enum SessionMode { Historical }` — `Streaming` variant documented as Phase 31.
- `pub struct Session { remote, streams, keys, key_prefix, mode, token }` — all fields `pub`, plain String / Vec<String>. `#[derive(Debug, Clone)]`.
- `Session::new(remote, streams)` — defaults to `Historical` mode, no keys/prefix/token.
- `pub struct OutOfScopeError(pub String)` with `#[derive(Debug, thiserror::Error)]` + `#[error("query out of scope: {0}")]`.
- `OutOfScopeError::new(s)` constructor.
- 2 unit tests: `session_new_defaults`, `out_of_scope_display`.
- **Zero** imports from `tally::server::*` / `crate::server::*` (verified by grep).
- Module is unconditional in `src/lib.rs` (compiles under both feature sets).

### `tests/client_engine_roundtrip.rs` (99 lines)

Cribbed the minimal-pipeline pattern from `tests/test_pipeline.rs::test_push_single_event_returns_all_features` (lines 18–93): `StreamDefinition` with `Count` + `Sum` features, one `key_field`, no dependencies. Inlined the setup rather than extracting a helper — 40 lines of `StreamDefinition` literal is the minimum to exercise the engine, and factoring it out for a single test is premature abstraction. Left a comment pointing Phase 29 at a future `minimal_client_harness()` helper once it builds the session manager.

Two tests:

1. `engine_push_round_trip_under_client_features` — builds `PipelineEngine::new()` + empty `StateStore`, registers a single-stream `Count` + `Sum` pipeline, pushes `{entity_key:"u1", amt:10.0}` then `{entity_key:"u1", amt:2.5}`. Asserts feature values after each push: `count_1h: 1 → 2`, `sum_1h: 10.0 → 12.5`. Second push's advanced sum proves operator state actually mutated, not just that `push` returned Ok.
2. `client_types_usable_alongside_engine` — smoke-check that `tally::client::{Session, SessionMode, OutOfScopeError}` are importable in the same test binary that drives the engine. Zero server imports.

### `src/lib.rs`

Added `pub mod client;` as the first module (alphabetically; unconditional). Final ordering:

```rust
pub mod client;
pub mod duration;
pub mod engine;
pub mod error;
#[cfg(feature = "server")]
pub mod server;
pub mod state;
pub mod types;
```

## Verification

| Command                                                                           | Result                           |
| --------------------------------------------------------------------------------- | -------------------------------- |
| `cargo test client:: --lib`                                                       | 2 passed (client module unit)    |
| `cargo test --no-default-features --features client --test client_engine_roundtrip` | 2 passed (client-only features)  |
| `cargo test --test client_engine_roundtrip` (defaults)                            | 2 passed (regression guard)      |
| `cargo build --no-default-features --features client --lib`                       | green                            |
| `cargo build` (defaults)                                                          | green                            |
| `grep -rn "crate::server\|tally::server" src/client/ tests/client_engine_roundtrip.rs` | empty                            |
| Full `cargo test` default suite                                                   | all green on second run (see note) |

Test count added by this plan: **4** (2 unit + 2 integration).

### Test-suite note (non-deviation)

On the first full `cargo test` run, the probabilistic test `tests/test_count_distinct_hybrid.rs::hll_mode_within_2_percent_on_100k` reported `error 0.057 > 5%` (HLL accuracy assertion with a random-seeded generator). Isolating the test and re-running it passed immediately (0.021). This is a pre-existing flaky statistical test with no code path overlap with Phase 28 (sketch library + hybrid transition logic; unchanged by any 28-0x plan). Logged here for transparency; not filed as a deviation because it is neither caused by nor worsened by this plan's changes. A follow-up plan tightening the HLL tolerance or seeding the generator would be the right fix.

## Deviations from Plan

### Unexpected parallel-plan overlap (no code change)

**1. [Rule 3 — blocking issue, resolved upstream] Cargo.toml `[[bin]] tally` gating**
- **Found during:** Task 2, when `cargo test --no-default-features --features client` failed with `main function not found in crate tally` (because `src/main.rs` top-level `#![cfg(feature = "server")]` makes the auto-discovered `tally` bin empty under client-only).
- **Initial action:** Added an explicit `[[bin]] name = "tally" / path = "src/main.rs" / required-features = ["server"]` entry to `Cargo.toml` to resolve the compile error.
- **Resolution:** While I was running the post-fix verification, Plan 28-02 completed and committed the identical change (commit `1ccc94b feat(28-02): add tally_cli bin with clone/sync stubs + arg parsing`). My local edit was bit-identical to the committed version — `git diff` showed no delta, so no Cargo.toml change is attributed to this commit. Plan 28-02 correctly owns this file.
- **Impact:** None. My plan's deliverables are confined to `src/client/mod.rs`, `src/lib.rs`, and `tests/client_engine_roundtrip.rs`, which respects the user's "do not touch 28-02's files" constraint.

No other deviations.

## What Phase 29 inherits

- **`Session` field shape** — will be replaced field-by-field with Phase 27's real `Scope` struct (mapping: `streams` → `Scope::streams`, `keys` / `key_prefix` → `Scope::keys` / `Scope::key_prefix`, etc.). Stringly-typed fields disappear.
- **`OutOfScopeError` import path** — `tally::client::OutOfScopeError` is stable; Phase 29's scope validator raises it.
- **Round-trip test as smoke harness** — `tests/client_engine_roundtrip.rs` becomes the skeleton Phase 29 extends with session-manager setup; the inline `minimal_tx_stream()` builder should be promoted to a shared helper at that point.
- **Session lifecycle** — `connect` / `bootstrap` / `run` methods are Phase 29's job; the stub deliberately has none.

## Explicit deferral

- Phase 27's `Scope` struct replaces the stub `String` / `Vec<String>` scope fields in Phase 29.
- `SessionMode::Streaming` variant added in Phase 31.
- Real network transport wiring is Phase 28-04 (Option-K historical clone).
- On-disk client state is Phase 32.

## Self-Check: PASSED

- `src/client/mod.rs` exists — FOUND.
- `tests/client_engine_roundtrip.rs` exists — FOUND.
- `src/lib.rs` contains `pub mod client;` — FOUND.
- 4 tests added, all passing under both feature sets.
- Commit hash: recorded below in the completion message.
