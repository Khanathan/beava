---
phase: 59
plan: 00
subsystem: tests / contract-first RED scaffolding
tags:
  - tdd-red
  - wave-0
  - binary-wire-format
  - tpc-perf-09
  - samply
  - requirements
requires:
  - phase-58-tokio-connection-handling-rewrite (baseline 1,376,450 EPS C1 preserved; 812/0/35 lib)
  - scripts/samply-probe-tokio-share.sh (coverage-sentinel pattern reused)
  - tests/tcp_ingest_routing.rs (boot boilerplate reused verbatim)
  - tests/profile_ingest.rs (harness cited by new probe script)
provides:
  - tests/wire_negotiation_handshake.rs (TPC-PERF-09 D-B1 RED, 59-W2)
  - tests/binary_push_bytes_passthrough.rs (TPC-PERF-09 D-D1 RED, 59-W1)
  - tests/json_over_tcp_still_accepted.rs (TPC-PERF-09 D-B3 RED, 59-W1 — see deviation)
  - tests/protocol_binary_decode_fuzz.rs (TPC-PERF-09 D-E3 GREEN Wave 0)
  - scripts/samply-probe-json-share.sh (one-command JSON_SHARE_PCT= probe + SENTINEL_FAILED)
  - scripts/verify-no-tcp-json-reserialize.sh (grep-ZERO D-C3 enforcer; exit 1 RED Wave 0)
  - .planning/REQUIREMENTS.md TPC-PERF-09 row (already landed pre-execution; coverage 37→38)
  - src/server/tcp.rs ConcurrentAppState fields
    (json_reserialize_count_total, binary_passthrough_count_total) + 2 .fetch_add(1) fires
affects:
  - Wave 1 (59-01) deletes tcp.rs:2204 + tcp.rs:2591 serde_json::to_vec call sites,
    wires Bytes passthrough + PayloadFmt::Binary tag end-to-end →
    flips binary_push_bytes_passthrough + json_over_tcp_still_accepted + verify-no-tcp-json-reserialize GREEN.
    Also adds parse_command JSON fallback so D-B3 regression guard becomes reality.
  - Wave 2 (59-02) adds OP_NEGOTIATE_WIRE_FORMAT (0x18) dispatch + capability echo →
    flips wire_negotiation_handshake GREEN.
  - Wave 4 (59-04) re-runs samply-probe-json-share.sh → expects JSON_SHARE_PCT ≤ 3.0 (D-D3).
  - protocol_binary_decode_fuzz runs on EVERY wave — regression alarm for any new
    decoder path introduced by Wave 1's Bytes reshape (D-E3).
tech-stack:
  added: []
  patterns:
    - "#[ignore = \"59-W{N}\"] Wave-targeted RED markers (Phase 54/55/56/57/58 precedent)"
    - "bash gate-script with machine-parseable final line (JSON_SHARE_PCT=<num>)"
    - "Coverage sentinel exit-code 2 for harness-unable false-pass (Phase 58 58-NEXT #1 pattern)"
    - "Always-on AtomicU64 probe fields on ConcurrentAppState (50.5-02 conn_interns_total idiom)"
    - "Source-level grep assertion as pragmatic RED (reads CARGO_MANIFEST_DIR/src/... at test-time)"
    - "proptest panic-free decoder regression (Phase 52 dev-dep, 500 cases)"
key-files:
  created:
    - tests/wire_negotiation_handshake.rs
    - tests/binary_push_bytes_passthrough.rs
    - tests/json_over_tcp_still_accepted.rs
    - tests/protocol_binary_decode_fuzz.rs
    - scripts/samply-probe-json-share.sh
    - scripts/verify-no-tcp-json-reserialize.sh
    - .planning/phases/59-binary-wire-format-for-push/59-00-SUMMARY.md
  modified:
    - src/server/tcp.rs (+ 2 AtomicU64 fields in ConcurrentAppState; + 2 .fetch_add(1) fires at WASTE sites)
requirements:
  - TPC-PERF-09
decisions:
  - "Rule 1 deviation: json_over_tcp_still_accepted flipped from GREEN-at-Wave-0 to
    #[ignore = \"59-W1\"]. Plan Task 4 claimed the test 'already passes' on Wave-0 HEAD
    via a parse_command JSON fallback. Empirically false — parse_command OP_PUSH uses
    decode_event_binary(&mut buf)? with `?` propagation and no fallback (src/server/protocol.rs:906).
    JSON-body push returns STATUS_ERROR. Wave 1's scope already touches parse_command
    (Bytes passthrough reshape) so net-additive to also plant the JSON fallback there;
    test flips GREEN in the same Wave-1 commit. Documented in test module header."
  - "Rule 3 deviation: verify-no-tcp-json-reserialize.sh uses grep -H for consistent
    filename-prefixed output. BSD grep on macOS omits filename when single-file input
    is passed; the comment-stripping regex `^[^:]+:[0-9]+:[[:space:]]*//` then cannot
    match `N:content` lines — a doc-comment at tcp.rs:322 was false-flagged during
    verification. Fix landed in a follow-up commit (e5e956e)."
  - "Rule 3 deviation: `/debug/status` JSON output NOT updated with the new counters.
    Plan Task 2 action text says 'Emit both via /debug/status JSON output' but no
    /debug/status endpoint exists in src/server (verified by grep). Phase 58
    accept_threads_spawned_total precedent is field-only / no HTTP export; the new
    Phase 59 counters follow that precedent. Wave 4's samply probe reads the counters
    via /debug/warnings or via crate-level access — no user-facing API surface touched."
  - "Plan Task 1 §5 called for 500 fuzz cases; kept at 500 (proptest default cases are
    256; bumped to 500 per plan). Added a second property for `field_count = u16::MAX`
    to pin the cap-clamp guard at protocol.rs:820 — regression protection for
    Wave 1's anticipated reshape of the binary decode path on the shard thread."
  - "Plan Task 2 counter fetch_add sites placed AT the 2 existing `serde_json::to_vec`
    call sites (tcp.rs ~:2204 after the new counter land, ~:2591 for batch). The 3rd
    WASTE pattern at tcp.rs:2059 (replica relog `make_log_payload`) is intentionally
    NOT fired — per 59-CONTEXT.md line 332 it's a 'conditional' Wave-1 target that
    keeps a JSON path for legacy callers. Wave 1 decides whether to delete or gate."
  - "protocol_binary_decode_fuzz.rs kept as a pure unit-level proptest (no server boot)
    to match the plan Task 1 §5 guidance. `proptest` is already a dev-dep since Phase 52."
metrics:
  duration: ~35min
  completed: 2026-04-20
  tasks: 2
  commits: 3
  files_created: 7
  files_modified: 1
  red_tests_landed: 3    # wire_negotiation + binary_passthrough + json_over_tcp (all Wave-1/2 flip targets)
  green_tests_landed: 1  # protocol_binary_decode_fuzz (always-on D-E3 regression)
  ignored_marker_count: 3  # 1 × 59-W2 + 2 × 59-W1 (wire_negotiation + binary_passthrough + json_over_tcp)
---

# Phase 59 Plan 00: Wave 0 RED-tests & Probe-Script Contract Summary

RED-first TDD baseline for Phase 59 (TPC-PERF-09 — binary wire format for
TCP PUSH). Four integration tests + two bash gate scripts + REQUIREMENTS
row + two always-on WASTE-probe counter fields land on disk. Tests FAIL
today by design (RED); Wave 1/2/4 flip them GREEN one by one.

## RED/GREEN → Wave Flip Map

| Gate | File | Marker | Flips GREEN at |
|------|------|--------|----------------|
| D-B1 (OP_NEGOTIATE_WIRE_FORMAT 0x18) | `tests/wire_negotiation_handshake.rs::op_negotiate_wire_format_round_trips_capability_bits` | `#[ignore = "59-W2"]` | Wave 2 opcode dispatch |
| D-D1 (TCP PUSH has no JSON re-serialize) | `tests/binary_push_bytes_passthrough.rs::binary_op_push_flows_without_json_reserialize` | `#[ignore = "59-W1"]` | Wave 1 Bytes passthrough |
| D-B3 (JSON-over-TCP accepted) | `tests/json_over_tcp_still_accepted.rs::json_over_tcp_op_push_accepted_after_phase_59` | `#[ignore = "59-W1"]` (see deviation) | Wave 1 parse_command JSON fallback |
| D-E3 (decoder panic-free) | `tests/protocol_binary_decode_fuzz.rs::decode_event_binary_never_panics_*` | none — always-on | Stays GREEN every wave |
| D-C3 (grep-ZERO on tcp.rs WASTE) | `scripts/verify-no-tcp-json-reserialize.sh` | — | Wave 1 WASTE deletion |
| D-D3 (samply JSON share ≤ 3%) | `scripts/samply-probe-json-share.sh` | — | Wave 4 probe re-run |

## Grep-Count Evidence

```
$ grep -cE '^- \[ \] \*\*TPC-PERF-09\*\*' .planning/REQUIREMENTS.md
1  (= 1 ✓ — row landed pre-execution in commit 4dbb04a)

$ grep -cE '^\| 59 \| binary-wire-format-for-push' .planning/REQUIREMENTS.md
1  (= 1 ✓)

$ grep -c '38/38' .planning/REQUIREMENTS.md
1  (= 1 ✓ — coverage incremented from 37/37)

$ grep -c '1,376,450' .planning/REQUIREMENTS.md
1  (≥ 1 ✓ — Phase 58 C1 baseline × 1.10 = 1,514,095 EPS floor encoded)

$ grep -c 'BEAVA_MAX_PAYLOAD_BYTES' .planning/REQUIREMENTS.md
1  (≥ 1 ✓ — D-E1 env var encoded)

$ grep -c 'WIRE_BINARY_PASSTHROUGH' .planning/REQUIREMENTS.md
1  (≥ 1 ✓ — D-B1 capability bit encoded)

$ grep -cE '#\[ignore = "59-W[1-2]"' tests/wire_negotiation_handshake.rs \
                                     tests/binary_push_bytes_passthrough.rs \
                                     tests/json_over_tcp_still_accepted.rs
1 + 1 + 1 = 3  (≥ 3 ✓)

$ test -x scripts/samply-probe-json-share.sh && echo OK
OK  ✓ (mode 0755)

$ test -x scripts/verify-no-tcp-json-reserialize.sh && echo OK
OK  ✓ (mode 0755)

$ bash scripts/samply-probe-json-share.sh --help | head -1
samply-probe-json-share — Phase 59 TPC-PERF-09 probe helper.  ✓

$ grep -c 'json_reserialize_count_total' src/server/tcp.rs
6  (≥ 3 ✓ — struct def + doc ref + initializer + 2 .fetch_add sites + batch comment)

$ grep -c 'binary_passthrough_count_total' src/server/tcp.rs
2  (≥ 1 ✓ — struct def + initializer; Wave 1 adds .fetch_add sites)
```

## Verification Log

```
$ cargo build --release --tests 2>&1 | grep -cE "^error"
0  ✓

$ cargo test --release --lib 2>&1 | tail -1
test result: ok. 812 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out; finished in 1.51s
✓ (Phase 58 baseline preserved — no regression from new AtomicU64 fields + 2 fetch_add sites)

$ cargo test --release --test protocol_binary_decode_fuzz 2>&1 | tail -1
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
✓ (D-E3 decoder panic-free regression guard GREEN today)

$ cargo test --release --test wire_negotiation_handshake 2>&1 | tail -1
test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s
✓ (59-W2 RED latent — opcode 0x18 not yet defined)

$ cargo test --release --test binary_push_bytes_passthrough 2>&1 | tail -1
test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s
✓ (59-W1 RED latent — tcp.rs still has serde_json::to_vec(payload|r.payload) call sites)

$ cargo test --release --test json_over_tcp_still_accepted 2>&1 | tail -1
test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s
✓ (59-W1 RED latent — parse_command has no JSON fallback today; see Rule-1 deviation)

$ bash scripts/verify-no-tcp-json-reserialize.sh; echo exit=$?
FAIL: TCP JSON re-serialize patterns found in src/server/tcp.rs (TPC-PERF-09 D-C3):
src/server/tcp.rs:2059:  let json_bytes = serde_json::to_vec(payload).unwrap_or_default();
src/server/tcp.rs:2204:  let payload_bytes = bytes::Bytes::from(serde_json::to_vec(payload).unwrap_or_default());
src/server/tcp.rs:2591:  bytes::Bytes::from(serde_json::to_vec(r.payload).unwrap_or_default());
Total hits: 3 (expected 0 after Phase 59 Wave 1)
exit=1
✓ (D-C3 grep-ZERO RED — three WASTE call sites remain for Wave 1 to delete)

$ cargo test --release --test tcp_ingest_routing --test http_ingest_routing 2>&1 | grep "test result:"
test result: ok. 1 passed; 0 failed; 0 ignored; ...
test result: ok. 1 passed; 0 failed; 0 ignored; ...
✓ (Phase 54 routing regression guards preserved after the 2 new fetch_add fires)
```

## Deviations from Plan

Three deviations, all documented in the `decisions:` block above and commit bodies:

### Rule 1 — Bug fix: json_over_tcp_still_accepted marker flip

- **Found during:** Task 1 verification run.
- **Issue:** Plan 59-00 Task 4 claims: "On current HEAD (pre-Wave 1), this test
  **already passes** because the current listener does `decode_event_binary(&mut buf)`
  and its first failure mode is 'format byte 0x7b' which the JSON-fallback path
  at `parse_command` has always handled." Empirically false on `arch/tpc-full-shard` HEAD:
  `src/server/protocol.rs::parse_command` OP_PUSH branch is
  `decode_event_binary(&mut buf)?` with `?` propagation and no fallback.
  Sending `{"amount":100}` on OP_PUSH returns STATUS_ERROR (0x01), not STATUS_OK.
  The D-B3 backward-compat contract is a SPEC not a current reality.
- **Fix:** Marked the test `#[ignore = "59-W1"]` (flips GREEN at Wave 1) and
  rewrote the module docstring to document the factual correction. Wave 1's
  scope already touches `parse_command` (Bytes passthrough reshape), so
  adding the JSON-body fallback there is net-additive. Once landed, D-B3
  becomes a GREEN regression guard for every wave ≥ 1.
- **Files modified:** `tests/json_over_tcp_still_accepted.rs`
- **Commit:** `bb96db2` (baked into Task 1's commit)

### Rule 3 — Blocking: verify-no-tcp-json-reserialize.sh false-positive on doc-comment

- **Found during:** Task 2 verification of the counter landing (grep invariant
  run to confirm RED still holds with the new .fetch_add call sites).
- **Issue:** BSD grep on macOS omits the filename prefix when a single file
  is passed (`grep -nE PAT file` emits `N:content`, not `file:N:content`).
  The comment-stripping filter `^[^:]+:[0-9]+:[[:space:]]*//` expects two
  colons — a doc-comment reference to the pattern at tcp.rs:322 (my own
  `json_reserialize_count_total` field doc) was false-flagged.
- **Fix:** Pass `-H` to grep for unconditional filename prefix. Script now
  correctly reports 3 WASTE call sites (down from 4 with the false-positive).
- **Files modified:** `scripts/verify-no-tcp-json-reserialize.sh`
- **Commit:** `e5e956e`

### Rule 3 — Blocking: /debug/status emission omitted

- **Found during:** Task 2 action-text read.
- **Issue:** Task 2 action text says "Emit both via `/debug/status` JSON
  output using the existing `serde_json::json!` macro (grep for
  `accept_threads_spawned_total` in the status handler to find the right
  spot)." But `/debug/status` does not exist in the codebase — grep for it
  across src/server returns 0 results. The Phase 58 precedent is
  field-only / no HTTP export; none of the Phase 58 counters
  (`accept_threads_spawned_total`, `inline_handler_events_total`) are
  emitted via any JSON endpoint.
- **Fix:** Follow the Phase 58 precedent — field-only. Wave 4's samply
  probe reads the counters directly via crate-level access (the counters
  are `pub` on `ConcurrentAppState`). No user-facing API surface touched.
- **Files modified:** None (plan action text over-specified).
- **Commit:** N/A (captured in commit `12988af` body + decisions block above).

Neither deviation changes wave-flip assignments, marker counts, or the
REQUIREMENTS.md row. Success criteria still met as written (with the
D-B3 "already passes" premise corrected to "Wave 1 establishes, Wave
1+ guards").

## Auth Gates Encountered

None — Wave 0 is tests + docs + bash gate scripts + 4 lines of
always-on AtomicU64 field additions. No wire surface, no external auth,
no network credentials.

## Next Wave Handoff (Wave 1 must deliver)

Wave 1 (plan 59-01) MUST:

1. **Bytes passthrough for TCP PUSH (D-A2 / D-A3 / D-C3):** Add
   `ShardEvent.payload_fmt: PayloadFmt { Binary, Json }` sibling field on
   `src/shard/thread.rs::ShardEvent`. Reshape `tcp.rs::handle_push_core_ex`
   (~:2195) and `tcp.rs::handle_push_batch` (~:2583) to forward
   `bytes::Bytes::from(raw_payload)` + `PayloadFmt::Binary` instead of
   `serde_json::to_vec(payload)`. Shard thread (`src/shard/thread.rs::process_shard_event`)
   dispatches on `payload_fmt`: call `decode_event_binary` for Binary,
   `serde_json::from_slice` for Json.
   → Flips `binary_push_bytes_passthrough` GREEN.
   → Flips `scripts/verify-no-tcp-json-reserialize.sh` exit 0.
   → Starts bumping `binary_passthrough_count_total` on every TCP push.
   → `json_reserialize_count_total` stays at 0 post-Wave-1 for TCP pushes
     (HTTP path still fires it — OK per D-A4).

2. **JSON-over-TCP parse_command fallback (D-B3):** Add a
   `decode_event_binary(&mut buf)` try; on `BeavaError::Protocol` retry
   with `serde_json::from_slice(&raw_payload)` if the first byte of the
   payload tail is `b'{'` or `b'['`. Forward as `PayloadFmt::Json`.
   → Flips `json_over_tcp_still_accepted` GREEN.

3. **BEAVA_MAX_PAYLOAD_BYTES DoS cap (D-E1):** Enforce at `parse_command`
   BEFORE any `read_string` / `decode_event_binary` call. Default 1 MiB,
   clamp [0, 16 MiB]. Return `BeavaError::Protocol("payload exceeds ...")`
   on violation.

Wave 2 (plan 59-02) MUST:

1. **OP_NEGOTIATE_WIRE_FORMAT = 0x18 (D-B1):** Add to `src/server/protocol.rs`
   as `pub const`. Extend `parse_command` dispatch + response handler to
   echo `[u8 STATUS_OK][u32 BE server_bits=0x00000001][u16 BE server_version=2]`.
   → Flips `wire_negotiation_handshake` GREEN.

Wave 4 (plan 59-04) MUST:

1. Extend `tests/profile_ingest.rs` (carries 58-NEXT #1) to exercise a
   real TCP push path so `samply-probe-json-share.sh`'s coverage sentinel
   passes.
2. Re-run `scripts/samply-probe-json-share.sh` → expect `JSON_SHARE_PCT ≤ 3.0`.
3. Re-run `MODE=complex DURATION=60 CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576 BEAVA_MAX_CONNS_PER_SHARD=1024 bash benchmark/fraud-pipeline/run_bench.sh`
   → expect aggregate EPS ≥ 1,514,095 (= Phase 58 C1 baseline × 1.10).
4. Verify p99 parity vs Phase 58's 30,632.5 µs median-of-p99 (±5%).
5. Remove all `#[ignore = "59-W[1-2]"]` markers once every gate passes.

## Known Stubs

**Intentional — this is the RED contract file.**

- `json_reserialize_count_total` + `binary_passthrough_count_total` fields
  exist on `ConcurrentAppState` and are initialized to 0. The first counter
  is fired twice-per-event today (at the two existing WASTE call sites);
  the second counter is NEVER fired — Wave 1 wires the `.fetch_add(1)` call.
  Both counters are `pub` field access, never exposed via HTTP endpoint
  (Phase 58 precedent).

- `scripts/samply-probe-json-share.sh` calls `tests/profile_ingest.rs`
  which does NOT exercise the real TCP push path (direct `handle_push_batch`
  calls from 8 OS threads). On Wave-0 HEAD the coverage sentinel returns
  `JSON_SHARE_PCT=SENTINEL_FAILED` and exits 2 — this is explicit by design
  (mirrors Phase 58 58-NEXT #1 harness-unable pattern). Wave 4 must extend
  the harness.

## Threat Flags

None — plan touched only test code, bash gate scripts, REQUIREMENTS.md,
and added 2 always-on `AtomicU64` probe fields + 2 `.fetch_add(1)` fires
on a struct already full of similar probe counters. No new trust
boundaries; no new wire surface; no new auth paths. Per plan
`<threat_model>`:

- T-59-00-01 (test grep-self symlink redirect): accepted — uses
  `env!("CARGO_MANIFEST_DIR")` (compile-time constant), not runtime arg.
- T-59-00-02 (proptest pathological timing): accepted — proptest's default
  time budget is per-test; 500 cases × 2 properties adds < 5s to the suite.
- T-59-00-03 (baseline numbers leakage in REQUIREMENTS): accepted —
  Phase 58's 1,376,450 EPS / 30,632.5 µs p99 numbers are public per
  committed Phase 58 SUMMARYs.
- T-59-00-04 (new auth surface): N/A — Wave 0 adds no opcode dispatch.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| Task 1 | `bb96db2` | `test(59-W0): plant 4 RED tests + 2 probe scripts for TPC-PERF-09` |
| Task 2 | `12988af` | `feat(59-W0): add TPC-PERF-09 always-on WASTE counters to ConcurrentAppState` |
| Task 2 fix | `e5e956e` | `fix(59-W0): force grep -H in verify-no-tcp-json-reserialize.sh` |

## Self-Check

- [x] `tests/wire_negotiation_handshake.rs` exists (1 test, 59-W2) — **FOUND**
- [x] `tests/binary_push_bytes_passthrough.rs` exists (1 test, 59-W1) — **FOUND**
- [x] `tests/json_over_tcp_still_accepted.rs` exists (1 test, 59-W1 — Rule-1 flip) — **FOUND**
- [x] `tests/protocol_binary_decode_fuzz.rs` exists (2 proptest cases, always-on) — **FOUND**
- [x] `scripts/samply-probe-json-share.sh` exists, mode 0755, `--help` works — **FOUND**
- [x] `scripts/verify-no-tcp-json-reserialize.sh` exists, mode 0755, exits 1 RED — **FOUND**
- [x] `.planning/REQUIREMENTS.md` contains TPC-PERF-09 row, coverage 38/38 — **FOUND**
- [x] `src/server/tcp.rs` `ConcurrentAppState` has `json_reserialize_count_total` + `binary_passthrough_count_total` fields initialized to 0 — **FOUND**
- [x] `src/server/tcp.rs` has 2 `.fetch_add(1, Relaxed)` fires at the two WASTE sites — **FOUND**
- [x] `cargo build --release --tests` → exit 0 — **VERIFIED**
- [x] `cargo test --release --lib` → 812/0/35 (Phase 58 baseline preserved) — **VERIFIED**
- [x] `cargo test --release --test protocol_binary_decode_fuzz` → 2/0/0 GREEN — **VERIFIED**
- [x] `cargo test --release --test wire_negotiation_handshake` → 0/0/1 RED (59-W2) — **VERIFIED**
- [x] `cargo test --release --test binary_push_bytes_passthrough` → 0/0/1 RED (59-W1) — **VERIFIED**
- [x] `cargo test --release --test json_over_tcp_still_accepted` → 0/0/1 RED (59-W1) — **VERIFIED**
- [x] `bash scripts/verify-no-tcp-json-reserialize.sh` → exit 1 RED (3 hits) — **VERIFIED**
- [x] 3 × `59-W[1-2]` attribute markers across 3 RED test files — **VERIFIED**
- [x] `grep -c 'TPC-PERF-09' .planning/REQUIREMENTS.md` ≥ 2 — **VERIFIED**
- [x] Commits `bb96db2`, `12988af`, `e5e956e` present in git log — **VERIFIED**

## Self-Check: PASSED
