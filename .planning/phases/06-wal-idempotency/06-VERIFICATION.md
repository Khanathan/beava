# Phase 6: WAL + Idempotency — Verification

**Verified:** 2026-04-23
**Branch:** v2/greenfield
**Commit range:** `5771ff1..bd99d67` (4 new commits this session — RED, two
GREENs, summaries)

## Gate results

| Gate | Result |
|---|---|
| `cargo test --workspace` | 395 + subsuites all PASS |
| `cargo test --workspace --features beava-server/testing` | **590 / 590 PASS** |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | clean |
| `cargo fmt --all --check` | clean |

## Success-criterion verification

### #1 Durability invariant — PASS

Evidence: `cargo test -p beava-server --test phase6_crash --features testing
-- --test-threads=1` → 2/2 pass:

- `wal_kill_before_fsync_drops_event`: asserts `count_wal_event_records(wal_dir) == 0`
- `wal_kill_after_ack_preserves_event`: asserts `count_wal_event_records(wal_dir) >= 1`

### #2 Byte-identical dedupe replay — PASS

Evidence: `phase6_push.rs::push_with_dedupe_key_replays_byte_identical`
asserts `b1_bytes == b2_bytes`, `X-Beava-Idempotent-Replay: 1` header
present on replay, and `cnt/alice == 1` after the duplicate. Also verified
by `phase6_smoke.rs::phase6_criterion_2_dedupe_replay_byte_identical`.

### #3 P50 fsync < 2 ms — WARNING

Evidence: `.planning/perf-baselines.md` records
`wal/append_fsync_default_coalesce = 7.40 ms` on Apple-M4/Darwin. Documented
as hw-class-limited (macOS `F_FULLSYNC`). Linux CI baseline is the final
gate at Phase 13.

### #4 WAL rotation + truncate — PASS

Evidence: `rotation.rs` unit tests (3/3 pass) + `phase6_smoke.rs::phase6_criterion_4_rotation_truncates`.

## Test count trace

| Session | Count |
|---|---:|
| Phase 6 start of session | 531 |
| Phase 6 close of session | **590** |
| Delta | **+59** |

New tests this session:
- `idem_cache` unit (4)
- `phase6_push.rs` (10)
- `phase6_crash.rs` (2)
- `phase6_smoke.rs` (4)
- bump in baselines smoke + pre-existing suites (+39 via feature flag
  activation — these were compiled but not running without the
  `testing` feature flag in prior aggregate counts)

## Open WARNINGs

1. **macOS P50 fsync 7.40 ms vs 2 ms target.** Hw-class-limited; Linux
   baseline pending. Not a blocker per CLAUDE.md §Performance Discipline
   (+25% = blocker; this is hw-class-inherent, not a regression).

## Gaps / human needed

None identified. Plans 03 + 04 both closed; all mandated gates pass.
