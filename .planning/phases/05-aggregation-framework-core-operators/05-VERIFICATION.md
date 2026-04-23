---
status: passed
phase: 05-aggregation-framework-core-operators
verified: 2026-04-23
must_haves_total: 6
must_haves_verified: 6
human_verification: []
gaps: []
---

# Phase 05 Verification — PASSED

**Phase Goal:** `group_by(keys).agg(name=bv.<op>(...), ...)` produces a Table in the DAG; server's apply loop updates per-entity aggregation state for every registered feature touching the event's source. Core 8 operators land (count, sum, avg, min, max, variance, stddev, ratio). `Windowed<Op>` bucket infra.

## Success Criteria (6/6)

| SC | Verified |
|---|---|
| SC1: `Event.group_by("user_id").agg(cnt=bv.count(window="5m"))` registers → Table with `cnt` | ✅ phase5_smoke + test_phase5_smoke |
| SC2: push event updates aggregation; GET /get returns current value | ✅ phase5_smoke + Python smoke |
| SC3: all 8 core operators pass table-driven tests | ✅ per-op unit tests in `agg_state.rs`, `agg_windowed.rs` + phase5_smoke |
| SC4: uniform event-time bucketing cap 64 proven replay-deterministic | ✅ layered: `windowed_replay_determinism` (internal-state gate, Plan 05-01) + `sc4_replay_determinism` observable-layer gate (Plan 05-08) |
| SC5: lifetime/windowless mode for count and ratio | ✅ `WindowlessOp` branch covered in unit tests + phase5_smoke |
| SC6: unknown field in op.field rejected at registration | ✅ Rule 11 validation in `register_validate.rs`; HTTP+TCP parity in phase5_smoke |

## Requirements Coverage (15/15)

SDK-AGG-01..06 + AGG-CORE-01..09 all delivered by code + tests.

## CONTEXT.md Decision Compliance (D-01..D-08)

| Decision | Status |
|---|---|
| D-01 enum dispatch (no Box<dyn>) | ✅ AggOp enum + match arms in agg_op.rs |
| D-02 {value} envelope only | ✅ grep guard — no "meta" in feature_query.rs |
| D-03 where= reuses Phase 4 eval; three-valued null drop | ✅ agg_where.rs delegates to eval::eval_with_depth |
| D-04 64-bucket event-time tumbling | ✅ agg_windowed.rs div_euclid mod 64 |
| D-05 aggregation → TableDerivation; aggregation-on-Table rejected | ✅ Rule 11 `AggregationOnTableNotSupported` |
| D-06 no SystemTime::now / rand::; BTreeMap for determinism | ✅ grep guards + apply_accepts_event_id_and_ignores_it test |
| D-07 Rule 11 register-time validation; HTTP+TCP parity | ✅ 7 new ErrorCode variants; wire tests on both transports |
| D-08 event_id threaded through apply for Phase 6 WAL | ✅ apply_event_to_aggregations signature |

## Fix Chain Summary

Code review found 1 critical + 3 warnings:
- CR-01 (zero-window div_euclid panic) → fixed commit `21ca582`: reject ms==0 in `parse_duration_to_ms` + tighten Python `_WINDOW_PATTERN` regex
- WR-01 (dead duplicate-feature check) → fixed commit `c3f2e7b`: removed unreachable HashSet check + comment documenting BTreeMap last-writer-wins
- WR-02 (| entity-key corruption) → fixed commit `667c2a1`: `parse_entity_key` returns Option; 400 `key_parse_failure` on mismatch; pipe-in-values documented as requiring %7C
- WR-03 (O(N) feature_index rebuild) → fixed commit `3af055d`: scope index updates to newly-inserted nodes only
- Post-fix fmt drift → fixed commit `c6fcf27`

## Gates

- `cargo test -p beava-core --lib`: 395 pass
- `cargo test -p beava-server --lib`: 114 pass
- `cargo test --test phase5_smoke`: 10 pass
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: clean
- `cargo fmt --all --check`: clean
- `python -m pytest -q`: 222 pass
- `python -m ruff check .`: clean
- `python -m mypy beava/`: clean

## Pre-existing (not Phase 5 regression)

- `cli_smoke` tests `loads_valid_config_starts_and_prints_banner` + `env_var_overrides_listen_addr` fail with timing-race on banner pipe (main binary + SIGTERM). Pre-existing since Phase 2.5 era; no Phase 5 code involved. Tracked separately for Phase 13 observability work.

## Architectural Carry-Forward to Phase 5.5 + Phase 6

Per memory/feedback_perf_regression_per_phase.md + memory/project_stateful_architecture.md:
- Phase 5.5 will add retroactive microbenches for AggOp::update, WindowedOp fold, apply_event_to_aggregations
- Phase 6 WAL will populate the `event_id` parameter already threaded through `apply_event_to_aggregations` (D-08)
- Stream retraction remains deferred to v1; event_id in WAL is the architectural handle
