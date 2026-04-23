---
phase: 2
phase_name: Sources + registry + version bumps
status: passed
date: 2026-04-22
verifier: inline (executor self-report + gate re-run)
---

# Phase 2 Verification — Sources + registry + version bumps

## Goal statement

`POST /register` accepts a JSON DAG of event/table/derivation nodes; validates; persists in-memory; assigns monotonic `registry_version`. Additive-only — removals/changes return 409 with structured diff.

## Success criteria status

| # | Criterion | Status | Evidence |
|---|-----------|--------|----------|
| 1 | Valid DAG → 200 with `registry_version: 1` + `registered_descriptors` list | ✅ | `phase2_smoke.rs` test 1 passes |
| 2 | Re-posting identical DAG = no-op; version unchanged | ✅ | `phase2_smoke.rs` test 2 passes |
| 3 | Additive DAG → 200 with version bump | ✅ | `phase2_smoke.rs` test 3 passes |
| 4 | Conflicting DAG (changed/removed) → 409 with structured `diff` | ✅ | `phase2_smoke.rs` tests 4 + 5 pass |
| 5 | Malformed payload → 400 with `{error: {code, path, reason}}` | ✅ | `phase2_smoke.rs` tests 6 + 7 pass + 30+ unit tests in `register_validate` |

## Gate results

- `cargo build --release`: PASS
- `cargo fmt --check`: PASS
- `cargo clippy --workspace --all-targets -- -D warnings`: PASS (0 warnings)
- `cargo test --workspace`: PASS (165 tests — up from 36 in Phase 1)
- `cargo test --features testing --test phase2_smoke`: PASS (7/7 acceptance tests)
- `cargo test --features testing --test foundation_smoke`: PASS (2/2 — no regression)

## Ships

- `crates/beava-core/src/schema.rs` (FieldType, EventSchema, TableSchema, DerivedSchema, validate_descriptor_name)
- `crates/beava-core/src/registry.rs` (Registry + parking_lot::RwLock<RegistryInner>, apply_registration)
- `crates/beava-core/src/op_node.rs` (OpNode 11 variants, AggSpec, JoinType)
- `crates/beava-core/src/registry_diff.rs` (compute_diff pure function, RegistryDiff, DiffReason)
- `crates/beava-core/src/register_validate.rs` (validate_payload, 9 rules, ValidatedPayload newtype)
- `crates/beava-server/src/register.rs` (POST /register handler, 15 integration tests)
- `crates/beava-server/src/registry_debug.rs` (GET /registry dev endpoint, 4 tests)
- `crates/beava-server/tests/phase2_smoke.rs` (7 acceptance tests)

## Commits (on `v2/greenfield`)

- 02-01 schema + descriptors + Registry
- 02-02 OpNode enum
- 02-03 diff engine + proptests
- 02-04 validation pass
- 02-05 POST /register endpoint
- 02-06 TestServer helpers + GET /registry + phase2_smoke
- 02 SUMMARY

## Requirements delivered (12/12)

SRV-API-01, SRV-API-02, SRV-API-11, SRV-API-12, SRV-REG-01, SRV-REG-02, SRV-REG-03, SRV-REG-05, SRV-REG-06, SDK-DEC-06, SDK-DEC-08, SDK-DEC-09 — all confirmed via tests.

## Handoff notes for Phase 3 (Python SDK)

- REGISTER JSON payload shape is stable. SDK compiles decorators → this JSON → POST /register.
- Validation errors have format `path: "nodes[X].upstreams[Y]"` — SDK parses these for readable ValidationError display.
- Version semantics: SDK stores `registry_version` from the last register response; future phases will use it for optimistic concurrency.
- OpNode JSON shapes locked — SDK serializes stateless ops / agg / join / union as these exact shapes.
- `GET /registry` behind `BEAVA_DEV_ENDPOINTS=1` is available for SDK round-trip testing.

## Verdict

PASS. Phase 2 delivered per all 5 ROADMAP success criteria. No gaps. No human-verification items. All 12 REQ-IDs covered. 165 tests green (up from 36).
