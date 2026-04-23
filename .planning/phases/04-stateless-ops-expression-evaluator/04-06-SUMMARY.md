---
phase: "04"
plan: "06"
subsystem: "server/dev-endpoints + acceptance"
tags: [acceptance, smoke, phase-gate, e2e, http, tcp, rust, dev-endpoint]
depends_on: ["04-05"]
dependency_graph:
  requires: ["04-05 (compiled_chain accessor + Rule 10 wiring)"]
  provides: ["POST /dev/apply_ops (dev-gated)", "phase4_smoke.rs Rust acceptance gate"]
  affects: ["04-07 (Python SC4 proptest needs this endpoint)", "Phase 5 (apply loop pattern proven here)"]
tech_stack:
  added: []
  patterns:
    - "Dev endpoint gating: mount sub-router only when dev_endpoints=true (same pattern as GET /registry)"
    - "JSON <-> Row conversion: json_to_value + value_to_json helpers with documented conversion tables"
    - "TestServer.registry() accessor for in-process compiled_chain assertions without HTTP round-trips"
key_files:
  created:
    - "crates/beava-server/tests/phase4_smoke.rs"
  modified:
    - "crates/beava-server/src/registry_debug.rs"
    - "crates/beava-server/src/http.rs"
    - "crates/beava-server/src/server.rs"
    - "crates/beava-server/src/testing.rs"
    - "crates/beava-server/Cargo.toml"
decisions:
  - "POST /dev/apply_ops returns 404 (not 400) for unknown derivation names — mirrors REST convention for missing resource"
  - "json_to_value: integers that fit i64 are I64; floating-point numbers are F64; arrays/objects become Null (no nested types in v0)"
  - "value_to_json: Value::F64(NaN/Inf) becomes Null (serde_json::Number::from_f64 returns None for non-finite floats)"
  - "TestServer stores registry Arc captured before serve() consumes Server — gives tests direct access without env-var mutation"
  - "dev_apply_ops_not_mounted_without_flag passes at red-stub time (route not mounted → 404); remains green after impl (route mounted only when flag=true)"
metrics:
  duration: "~30min"
  completed_at: "2026-04-23T14:12:00Z"
  tasks_completed: 2
  files_changed: 6
---

# Phase 4 Plan 06: Rust-side Phase 4 Acceptance Gate Summary

**One-liner:** `POST /dev/apply_ops` dev endpoint (BEAVA_DEV_ENDPOINTS=1 gated) + Rust SC1/SC2/SC3/SC5 acceptance smoke tests over HTTP and TCP against a live TestServer.

---

## 1. POST /dev/apply_ops Endpoint

### Spec

```
POST /dev/apply_ops
Content-Type: application/json

Request:  {"derivation": "<name>", "row": {<field>: <json value>, ...}}
Response: {"kept": true,  "row": {...}}   // filter passed + transforms applied
        | {"kept": false}                 // a Filter op dropped the row
        | HTTP 404 {"error": "derivation_not_found"}   // derivation name unknown
```

**Gating:** Mounted on the axum Router ONLY when `dev_endpoints_enabled = true` (same condition as `GET /registry`). When `BEAVA_DEV_ENDPOINTS` is unset or not `"1"`, the route is not wired → axum returns 404.

### JSON <-> Value conversion rules

| JSON type | Value |
|-----------|-------|
| `null` | `Value::Null` |
| `bool` | `Value::Bool` |
| integer-fitting Number | `Value::I64` |
| other Number | `Value::F64` |
| string | `Value::Str` |
| array / object | `Value::Null` (no nested support in v0) |

| Value | JSON |
|-------|------|
| `Value::Null` | `null` |
| `Value::Bool(b)` | `bool` |
| `Value::I64(n)` | Number (i64) |
| `Value::F64(f)` | Number (f64); NaN/Inf → `null` |
| `Value::Str(s)` | string |
| `Value::Bytes(_)` | `null` (binary not JSON-representable) |
| `Value::Datetime(ms)` | Number (i64 ms since epoch) |

### 4 Unit Tests

| Test | Assertion |
|------|-----------|
| `dev_apply_ops_endpoint_returns_404_without_derivation` | Unknown derivation name → HTTP 404 |
| `dev_apply_ops_endpoint_filters_drops_row` | `amount=50 < 100` with filter `(amount > 100)` → `{"kept": false}` |
| `dev_apply_ops_endpoint_filter_keeps_row_and_returns_transformed` | `amount=1000` with filter+with_columns → `{"kept": true, "row": {..., "is_big": true}}` |
| `dev_apply_ops_not_mounted_without_flag` | Router built with `dev_endpoints=false` → HTTP 404 for any POST /dev/apply_ops |

---

## 2. Rust SC Coverage Matrix

| SC | Test(s) | What it proves |
|----|---------|----------------|
| SC1 | `sc1_http_filter_rejects_failing_events` | Filter registered over HTTP; amount=50 dropped, amount=150 kept via /dev/apply_ops |
| SC1 | `sc1_tcp_filter_rejects_failing_events` | Same derivation registered over TCP; /dev/apply_ops agrees (HTTP-only dev endpoint) |
| SC2 | `sc2_with_columns_adds_derived_field_visible_downstream` | `with_columns` adds `is_big:bool` to propagated schema; downstream `OnlyBig` registers against it; /dev/apply_ops confirms field in output row |
| SC3 | `sc3_chained_ops_filter_select_with_columns_cast_schema_propagates` | 4-op chain (filter→select→with_columns→cast); GET /registry shows `is_big:i64`; apply_ops round-trip agrees |
| SC5 | `sc5_malformed_predicate_returns_400_with_path_http` | Unterminated expr at register → 400, `code="invalid_expression"`, `path` contains `"ops[0]"` |
| SC5 | `sc5_malformed_predicate_returns_error_frame_tcp` | Same over TCP → `OP_ERROR_RESPONSE` frame with same error shape |

SC4 (client/server eval equivalence proptest) lives in Plan 04-07.

---

## 3. phase4_compiled_chain_is_retrievable_post_register

This contract test exercises `Registry::compiled_chain(name)` directly via `TestServer::registry()`:

1. Registers `Transaction + BigTx(filter: amount > 100)` via HTTP.
2. Calls `ts.registry().compiled_chain("BigTx")` → asserts `Some(Arc<OpChain>)`.
3. Builds `Row` values in Rust and calls `chain.apply(row)` directly in-process.
4. Compares in-process results to what `/dev/apply_ops` returns for the same rows — they must agree.

**Why it matters for Phase 5:** Phase 5's push-path event handler will call exactly this pattern (`Registry::compiled_chain(name)` → `chain.apply(incoming_row)`) per event. This test proves the chain is always cached post-registration and gives Plan 04-07's Python proptest a trustworthy HTTP witness.

---

## 4. Follow-ups for Plan 04-07

Plan 04-07 (Python side) inherits:

- `/dev/apply_ops` as its HTTP witness for the SC4 client/server equivalence proptest (256 hypothesis cases: random op-chain + random row → SDK eval == server eval via this endpoint)
- The 8 Python SDK op methods (`filter`, `select`, `drop`, `rename`, `with_columns`, `map`, `cast`, `fillna`) whose Python reference evaluator outputs must match what `chain.apply` computes
- SC4: Python reference evaluator agrees with Rust server for random expressions and random rows (the "no drift" guarantee)
- SC1..SC3 + SC5 on the Python side (SDK-level smoke over HTTP)

---

## 5. Seams for Phase 5 Apply Loop

Phase 5's push-path should use exactly:

```rust
// After WAL-ack, for each derivation downstream of the pushed event:
if let Some(chain) = registry.compiled_chain(&derivation_name) {
    match chain.apply(incoming_row) {
        None => { /* filter dropped it — skip aggregations for this derivation */ }
        Some(transformed_row) => { /* pass to aggregation layer */ }
    }
}
```

The `Registry::compiled_chain` accessor acquires only a read-lock and clones an `Arc` — no write contention on the hot push path. The `OpChain::apply` call is pure (no locks, no allocation for small rows) and safe to call from the async push handler.

---

## Deviations from Plan

### None — plan executed exactly as written.

The two "gating tests that pass at red-stub time" (`dev_apply_ops_not_mounted_without_flag`, `dev_apply_ops_endpoint_returns_404_without_derivation`) are correct behavior: at stub time neither condition is distinguishable from "route not mounted"; after implementation they continue to pass because the implementation is correct.

---

## Self-Check: PASSED

Files created/modified:

- `/Users/petrpan26/work/tally/crates/beava-server/tests/phase4_smoke.rs` — FOUND
- `/Users/petrpan26/work/tally/crates/beava-server/src/registry_debug.rs` — FOUND
- `/Users/petrpan26/work/tally/crates/beava-server/src/http.rs` — FOUND
- `/Users/petrpan26/work/tally/crates/beava-server/src/server.rs` — FOUND
- `/Users/petrpan26/work/tally/crates/beava-server/src/testing.rs` — FOUND

Commits:

- `11f27e0` test(04-06): red stubs — FOUND
- `b0ecb0d` feat(04-06): green implementation — FOUND

`cargo test --workspace`: 384 passed, 0 failed.
`cargo clippy --workspace --all-targets --all-features -- -D warnings`: clean.
`cargo fmt --all --check`: clean.
