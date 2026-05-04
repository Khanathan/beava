# Phase 13.4.1 — Deferred Items

Per CLAUDE.md scope-boundary rule: only auto-fix issues directly caused by the
current task's changes. The following pre-existing failures and concerns were
discovered during Plan 13.4.1-05 closure and are documented here for the
v0.0.x cleanup window.

## Pre-existing test flakes (NOT introduced by Phase 13.4.1)

Verified by `git stash` + targeted re-run on the plain Plan 13.4.1-04 head
(commit `526c9963`) during Plan 13.4.1-05 closure.

### 1. `phase2_5_smoke::criterion_6_pipelined_registers_return_in_order`

**Symptom:** Returns `OP_ERROR_RESPONSE (0xFFFF)` on the first pipelined
register, expected `OP_REGISTER (0x0001)`.

**Provenance:** First documented as a pre-existing flake in the Phase 13.4
perf-baselines and again in the Phase 13.5 perf-baselines:
> "1 pre-existing flake (`phase2_5_smoke::criterion_6_pipelined_registers_return_in_order`); not introduced by Phase 13.4 (verified by `git stash` + re-run during Plan 09 execution; pre-Phase-13.4 history at `7ad84c5` is the file's last touch)."

**Confirmed pre-existing on Plan 13.4.1-04 head** (Plan 13.4.1-05 closure
verification, 2026-05-04): fails 5/5 invocations on the plain head; identical
failure mode under Plan 13.4.1-05 changes.

**Resolution:** Defer to v0.0.x test-stability sweep. Not a Plan 13.4.1 issue.

### 2. `phase2_smoke::success_criterion_4_conflict_returns_409_with_diff`

**Symptom:** Returns `registration_conflict` error code; the test expects a
different code/value structure on the diff payload.

**Confirmed pre-existing on Plan 13.4.1-04 head** (Plan 13.4.1-05 closure,
2026-05-04): fails identically with my changes stashed; reproduces under
`--test-threads=1`.

**Resolution:** Defer to v0.0.x test-stability sweep. Not a Plan 13.4.1 issue.

### 3. Workspace-parallel tempdir / port-binding races

**Symptom:** Various beava-server tests intermittently fail with
`spawn: Server(WalSpawn("io: File exists (os error 17)"))` or port-binding
errors when run in parallel via `cargo test --workspace`. They all pass when
run individually or with reduced parallelism.

**Affected tests observed during Plan 13.4.1-05 closure:**
- `phase13_4_1_verb_style_batch_get` (3 of 6 tests under `--test-threads=4`)
- `phase12_07_main_uses_v18_test::test_release_binary_responds_to_post_get`
- `phase12_8_metrics_endpoint::test_bytes_per_entity_p99_reports_static_v0_estimate`
- `cli_smoke` test cases
- `testing::tests::post_json_404_on_unknown_path` (lib unit test)

**Root cause:** Test infrastructure uses ephemeral ports + `tempfile::tempdir()`
across processes. Under high parallelism, port-binding TOCTOU races and
tempdir-creation collisions occur. The Phase 12.6 / 12.8 SUMMARYs already note
similar parallel-run flakiness on Apple-M4 dev boxes.

**Resolution:** Defer to v0.0.x test-infrastructure hardening. Each affected
test passes deterministically when run individually or under `--test-threads=1`
within the affected file. Not a Plan 13.4.1 regression.

## v0.0.x cleanup carry-forwards (already documented)

These were noted in the Plan 13.4.1-04 SUMMARY and Plan 13.4.1-05 SUMMARY;
listed here for completeness:

- **`entity_id` serde alias on `BatchGetReqEntry`** — drop the alias + its
  detection custom-Deserialize after one v0.0.x release cycle. Tracked in
  `.planning/ideas/v0.1-deferrals.md` per Plan 13.4.1-04 commit `1d5c241f`.

- **`OP_MGET (0x0021)` and `OP_GET_MULTI (0x0022)` opcodes** — these legacy
  multi-key-single-feature and multi-feature-multi-key TCP opcodes still exist
  in `crates/beava-runtime-core/src/wire_request.rs` and route through the
  legacy `dispatch_get_batch` path. They are NOT in the locked v0 wire-spec
  (`docs/wire-spec.md` lists only OP_GET, OP_BATCH_GET, OP_GET_RESPONSE).
  v0.0.x cleanup should remove them along with the legacy `dispatch_get_batch`
  function in `runtime_core_glue.rs`.

- **`WireRequest::HttpGetSingle` (`GET /get/:feature/:key` route)** — the
  legacy path-encoded single-feature route survives intact. It feeds the
  Phase 12.7 / 12.9 test fixtures (`{"value": <val>}` envelope). Removal also
  belongs to v0.0.x cleanup; it is NOT in the locked verb-style wire-spec.

## Audit trail

Migration follow-throughs landed during Plan 13.4.1-05 closure (Rule 1
deviation per CLAUDE.md scope-boundary — Plan 04 flattened
`phase13_4_op_batch_get.rs` Tests 1/2/5 but missed several sibling test
files that also asserted against the legacy shape):

| Test file | Tests migrated | Reason |
|---|---|---|
| `phase13_4_get_row_shape.rs` | 3 of 5 | Test 1, 2, 3 sent legacy `{keys, features}` to POST /get; rejected by D-05 post-13.4.1. Migrated to verb-style `{table, key, features?}` + FLAT-row response shape. |
| `phase12_07_get_via_mio_test.rs` | 2 of 5 | `test_http_get_batch_via_mio_returns_result_map` (legacy `{keys, features}` POST /get) + `test_tcp_op_get_single_returns_op_get_response` (legacy `{feature, key}` OP_GET). Both migrated to verb-style. OP_MGET / OP_GET_MULTI tests untouched — those opcodes still route through legacy. |
| `phase12_07_main_uses_v18_test.rs` | 1 of 3 | `test_release_binary_responds_to_post_get_without_dev_endpoints_env`. |
| `phase12_07_read_your_writes_test.rs` | 1 of 2 | TCP read-your-writes via legacy `{feature, key}`; HTTP path untouched (uses `GET /get/:feature/:key` legacy route). |
| `phase12_07_tcp_get_test.rs` | 2 of 4 | `test_apply_shard_dispatches_tcp_get_single` + `test_apply_shard_tcp_get_unknown_feature_returns_query_not_found`. |
| `phase12_09_http_get_json_only_test.rs` | 1 of 2 | `test_http_post_get_returns_json_response`. |
| `phase12_09_tcp_get_json_unchanged_test.rs` | 1 of 3 | `test_tcp_get_single_json_unchanged`. |
| `phase12_09_tcp_get_msgpack_test.rs` | 1 of 3 | `test_tcp_get_single_msgpack_round_trip`. |
| `phase12_6_path_x_windowed_arrival_time.rs` | 1 of 1 | `path_x_bucketing_is_on_arrival_not_event_time`. |
| `phase13_4_global_table_routing.rs` | 3 of 6 | Tests asserting against `{table, entity_id, features}` envelope shape. Flattened to FLAT-row form. Tests 3, 4 (register validation) untouched. |

Total: 16 follow-through test migrations across 10 sibling files. All pass
under serialized execution post-migration.
