---
phase: "02"
subsystem: registry
tags: [rust, registry, http, axum, validation, diff, schema, acceptance]
dependency_graph:
  requires: [phase-01-foundation]
  provides: [POST-register, GET-registry-dev, Registry-data-model, OpNode-enum, diff-engine, validation-pass, TestServer-HTTP-helpers]
  affects: [phase-03-python-sdk, phase-06-wal]
tech_stack:
  added:
    - parking_lot 0.12 (RwLock for Registry)
    - proptest 1 (property-based testing for diff engine)
  patterns:
    - internally-tagged serde enums (#[serde(tag = "kind")] / #[serde(tag = "op")])
    - equiv_ignoring_version() helper (not custom PartialEq) for diff without version-stamp noise
    - ValidatedPayload newtype enforcing validate-then-diff ordering
    - fail-soft validation (collect all errors, return Vec<ValidationError>)
    - first-error-wins on HTTP 400 (logs full Vec at WARN, returns first)
    - DFS three-color cycle detection for DAG acyclicity
    - Server::bind takes dev_endpoints: bool directly (avoids env-var race in tests)
key_files:
  created:
    - crates/beava-core/src/schema.rs
    - crates/beava-core/src/registry.rs
    - crates/beava-core/src/op_node.rs
    - crates/beava-core/src/registry_diff.rs
    - crates/beava-core/src/register_validate.rs
    - crates/beava-server/src/register.rs
    - crates/beava-server/src/registry_debug.rs
    - crates/beava-server/tests/phase2_smoke.rs
  modified:
    - crates/beava-core/src/lib.rs
    - crates/beava-server/src/http.rs
    - crates/beava-server/src/server.rs
    - crates/beava-server/src/lib.rs
    - crates/beava-server/src/testing.rs
    - crates/beava-server/src/main.rs
    - crates/beava-server/Cargo.toml
    - Cargo.toml (workspace)
decisions:
  - "apply_registration takes Vec<PayloadNode> and bumps version by 1 atomically under write lock; no-op decision is caller's responsibility (endpoint)"
  - "Server::bind accepts dev_endpoints: bool directly instead of reading env var inside async fn (avoids parking_lot MutexGuard held across await)"
  - "Parse errors return path='<body>' (v0 best-effort); richer JSON-pointer extraction is Phase 3+ work"
  - "OpNode derives PartialEq but NOT Eq (serde_json::Value is not Eq)"
  - "POST /register is additive-only; 409 on conflict with structured diff; 400 on validation failure"
  - "GET /registry gated on BEAVA_DEV_ENDPOINTS=1 at bind time, reads once per process"
metrics:
  duration_minutes: 90
  completed_date: "2026-04-23"
  plans_completed: 6
  tests_added: 129
  files_created: 8
  files_modified: 7
---

# Phase 2 Summary: Sources + Registry + Version Bumps

**One-liner:** In-memory Registry with `parking_lot::RwLock`, additive `POST /register` with validation + diff + 409 conflict, `GET /registry` dev endpoint, and end-to-end `phase2_smoke` acceptance gate over real HTTP.

## Plans Executed

| Plan | Name | Commit | Tests Added |
|------|------|--------|-------------|
| 02-01 | Schema types + descriptor structs + Registry wrapper | ed5e75e | 12 |
| 02-02 | OpNode enum; swap DerivationDescriptor.ops to Vec<OpNode> | 2dd8d64 | 15 |
| 02-03 | Registry diff engine with reason classification + proptests | f033c4a | 26 |
| 02-04 | Registration validation pass (9 rules, structured errors) | 61bb35e | 36 |
| 02-05 | POST /register endpoint — parse, validate, diff, install | 489646c | 15 |
| 02-06 | TestServer HTTP helpers + GET /registry + phase2_smoke | dc2320a | 25 |

## What Was Built

### beava-core additions

**schema.rs** — `FieldType` enum (6 variants), `EventSchema`, `TableSchema`, `DerivedSchema` (all with `BTreeMap<String, FieldType>` fields), `validate_descriptor_name` (hand-rolled, no regex, enforces `[A-Za-z_][A-Za-z0-9_]*`, `_beava_` reserved prefix, 128-char max).

**registry.rs** — `OutputKind`, `TableMode` enums; `EventDescriptor`, `TableDescriptor`, `DerivationDescriptor` with `#[serde(default)]` on optional fields and `registered_at_version`; `equiv_ignoring_version()` on each; `RegistryInner` (`BTreeMap`s + `version: u64`); `Registry` (`parking_lot::RwLock<RegistryInner>`) with `new()`, `version()`, `read()`, `snapshot()`, `install_descriptors()`, `apply_registration()`.

**op_node.rs** — `AggSpec { op, params }`, `JoinType`, `OpNode` enum (11 variants: Filter/Select/Drop/Rename/WithColumns/Map/Cast/Fillna/GroupBy/Join/Union) with `#[serde(tag = "op", rename_all = "snake_case")]`. Derives `PartialEq` only (not `Eq`) because `serde_json::Value` in `AggSpec` is not `Eq`.

**registry_diff.rs** — `PayloadNode` enum (internally tagged by `kind`); `RegistryDiff { added, already_present, changed }`; `ConflictDetail { name, reason, details }`; `DiffReason` (12 variants); `compute_diff(current, payload) -> RegistryDiff` (pure, order-preserving); 3 classify helpers for event/table/derivation diffs; `describe_schema_diff` (deterministic via `BTreeMap` order); 18 unit tests + 8 proptests.

**register_validate.rs** — `ErrorCode` (19 variants), `ValidationError { code, path, reason }`, `ValidatedPayload` newtype; `validate_payload` enforcing all 9 rules: uniqueness, name format, event schema (event_time_field + type + non-empty), table primary_key (1–4 fields, all in schema), derivation upstreams resolution, derivation schema non-empty, DAG acyclicity (DFS three-color), topological order, idempotency key. Fail-soft (collects all errors). 36 unit tests.

### beava-server additions

**register.rs** — Full 8-step `post_register` handler: CT check (415), JSON parse (400), snapshot, `validate_payload` (400), `compute_diff`, conflict (409 with `ResponseDiff`), no-op (200 same version), `apply_registration` (200 new version). `RegisterAppState { registry: Arc<Registry> }`; `register_router` with `DefaultBodyLimit::max(1 MiB)`. 15 integration tests via `tower::ServiceExt::oneshot`.

**registry_debug.rs** — `GET /registry` returning `RegistryDump { version, events, tables, derivations, _dev_only: true }`. Only mounted when `dev_endpoints_enabled = true`. 4 unit tests.

**http.rs** — `router(readiness, registry, dev_endpoints_enabled)` now 3-arg; conditionally merges `registry_debug_router`.

**testing.rs** — `TestServer::post_json`, `get_json`, `get_raw` methods; `TestServerBuilder::dev_endpoints(bool)` toggle; `dev_endpoints` passed directly to `Server::bind` (no env-var mutex).

**phase2_smoke.rs** — 7 acceptance tests over real HTTP: 5 ROADMAP success criteria + 2 dev-endpoint tests. Runs in ~130ms.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Proptest `input_order_preserved` with duplicate names**
- **Found during:** Plan 02-06 final workspace run
- **Issue:** `arb_event_descriptor()` could generate descriptors with duplicate names (e.g., two events named `"_"`). The ordering assertion used `Vec::position()` (first-match), which returned lower indices for later duplicates, causing false failures.
- **Fix:** Deduplicate payload by name (first-wins) in the proptest body using a `HashSet` filter + `prop_assume!(!payload.is_empty())`.
- **Files modified:** `crates/beava-core/src/registry_diff.rs`
- **Commit:** dc2320a

**2. [Rule 3 - Blocking] `await_holding_lock` clippy error in testing.rs**
- **Issue:** Original plan used `parking_lot::MutexGuard` held across `Server::bind(...).await` to serialize env-var mutation. Clippy `-D warnings` rejected this.
- **Fix:** Changed `Server::bind` to accept `dev_endpoints: bool` directly. Env var read moved to `main.rs`. Tests pass the flag directly — no mutex, no env mutation, no clippy issue.
- **Files modified:** `crates/beava-server/src/server.rs`, `crates/beava-server/src/testing.rs`, `crates/beava-server/src/main.rs`
- **Commit:** dc2320a

**3. [Rule 1 - Bug] Tracing test couldn't `block_on` inside async context**
- **Found during:** Plan 02-05 Task 2
- **Issue:** `test_success_emits_info_log` tried to use `tokio::runtime::Handle::current().block_on(...)` inside a `#[tokio::test]` (cannot nest `block_on` within an active runtime).
- **Fix:** Rewrote to use `tokio::task::spawn_blocking` with a dedicated `current_thread` runtime inside the blocking task, dispatching tracing events via `tracing::dispatcher::with_default`.
- **Files modified:** `crates/beava-server/src/register.rs`
- **Commit:** 489646c

## Known Stubs

None. All wire endpoints return real data. `registered_at_version` serializes in GET /registry responses. The registry is in-memory only (no WAL yet — that is Plan 06-06, not a stub).

## Threat Flags

No new network surfaces beyond what was planned in the threat model (`T-02-05-*` and `T-02-06-*` in the PLAN.md files).

## Phase 2 Acceptance Gate

All 5 ROADMAP success criteria proven by `phase2_smoke.rs`:

1. Valid JSON DAG → 200 with `registry_version: 1` and `registered_descriptors`
2. Identical re-post is no-op, version unchanged
3. Additive DAG (new nodes) → 200 with version bump
4. Conflicting DAG → 409 with `{error: {code: "registration_conflict", diff: {added, removed, changed}}}`
5. Malformed payload → 400 with `{error: {code: "invalid_registration", path, reason}}`

## Self-Check: PASSED

- `/Users/petrpan26/work/tally/crates/beava-core/src/schema.rs` — exists
- `/Users/petrpan26/work/tally/crates/beava-core/src/registry.rs` — exists
- `/Users/petrpan26/work/tally/crates/beava-core/src/op_node.rs` — exists
- `/Users/petrpan26/work/tally/crates/beava-core/src/registry_diff.rs` — exists
- `/Users/petrpan26/work/tally/crates/beava-core/src/register_validate.rs` — exists
- `/Users/petrpan26/work/tally/crates/beava-server/src/register.rs` — exists
- `/Users/petrpan26/work/tally/crates/beava-server/src/registry_debug.rs` — exists
- `/Users/petrpan26/work/tally/crates/beava-server/tests/phase2_smoke.rs` — exists
- Commits ed5e75e, 2dd8d64, f033c4a, 61bb35e, 489646c, dc2320a all present
- 165 workspace tests pass, 0 failures
- `cargo clippy --workspace --all-targets -- -D warnings` clean
- `cargo fmt --check` clean
