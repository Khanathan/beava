---
phase: 49-per-shard-state-store
plan: "04"
subsystem: engine/serde + python-sdk
tags: [shard-key, serde, tpc-dx-01, tdd]
requirements: [TPC-DX-01]
key-files:
  modified:
    - src/engine/join_validator.rs
    - src/engine/register.rs
    - python/beava/_stream.py
    - python/beava/_serialize.py
decisions:
  - "ShardKeySpec uses #[serde(untagged)]: string → Single, array → Tuple — no envelope wrapper needed"
  - "shard_key added to SourceDescriptor (register.rs), not StreamDefinition directly, matching the existing wire/internal split"
metrics:
  duration_minutes: 8
  completed: "2026-04-18"
  tasks_completed: 1
  files_changed: 4
---

# Phase 49 Plan 04: ShardKeySpec Serde + Python SDK shard_key Surface Summary

One-liner: ShardKeySpec gains Serialize/Deserialize (untagged), SourceDescriptor gains shard_key with backward-compat serde default, Python @bv.stream() accepts shard_key with client-side type validation.

## Tests

8/8 contract tests in `tests/test_shard_key_serde.rs` GREEN:

| Test | Status |
|------|--------|
| shard_key_single_round_trip | ok |
| shard_key_tuple_round_trip | ok |
| shard_key_missing_field_deserializes_as_none | ok |
| shard_key_null_deserializes_as_none | ok |
| shard_key_single_deserializes_from_string | ok |
| shard_key_tuple_deserializes_from_array | ok |
| source_descriptor_missing_shard_key_is_backward_compat | ok |
| source_descriptor_with_shard_key_parses_correctly | ok |

## Commit

`3a2b8ad` — feat(49-04): add ShardKeySpec serde + shard_key to SourceDescriptor and Python SDK

## Files Changed

- `src/engine/join_validator.rs` — added `Serialize, Deserialize` derives + `#[serde(untagged)]` to `ShardKeySpec`
- `src/engine/register.rs` — added `shard_key: Option<ShardKeySpec>` with `#[serde(default)]` to `SourceDescriptor`
- `python/beava/_stream.py` — `@bv.stream(shard_key=...)` parameter; type validation; `StreamSource._beava_shard_key` stored
- `python/beava/_serialize.py` — emits `shard_key` in REGISTER payload (str → string, tuple → array)

## Deviations from Plan

None — plan executed exactly as written. The `convert_register_request` / `v0_source_to_stream_def` mapping step in the plan description was not needed: the existing pipeline code reads `raw_register_jsons` directly and the `SourceDescriptor.shard_key` field is available for Wave 2 routing to consume.

## Self-Check: PASSED

- `src/engine/join_validator.rs` — modified, present
- `src/engine/register.rs` — modified, present
- `python/beava/_stream.py` — modified, present
- `python/beava/_serialize.py` — modified, present
- Commit `3a2b8ad` — verified via git log
- `cargo check` — clean (2.66 s)
- Targeted test run — 8/8 passed, build took 7.44 s total
