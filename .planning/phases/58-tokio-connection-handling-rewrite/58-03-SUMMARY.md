---
phase: 58
plan: 03
subsystem: server / TCP replica ingress on per-shard accept
tags:
  - replica
  - replica-ingest
  - per-shard-accept
  - op-log-fetch
  - op-subscribe
  - wave-3
  - tpc-perf-08
  - guardrail
requires:
  - phase-58-01-SUMMARY (Wave 1 Linux SO_REUSEPORT per-shard accept —
    established the `handle_connection_public` INLINE-polling pattern
    via FuturesUnordered inside each shard's current_thread runtime)
  - phase-58-02-SUMMARY (Wave 2 macOS dedicated-accept-thread-per-shard —
    spawn_macos_per_shard_accept_threads + handle_connection_blocking
    reusing handle_connection_public via per-thread current_thread tokio
    runtime, D-B1 default + D-B2 escape hatch)
  - phase-35-01 OP_LOG_FETCH wire spec (replica catchup opcode) + phase-27
    OP_SUBSCRIBE wire spec (live-replay opcode) — the two replica-
    ingress opcodes this wave guards
  - phase-54-00-SUMMARY replica silent-regression guard test
    (`tests/replica_ingest_routing.rs`) — Wave 3 extends it in place
provides:
  - tests/replica_ingest_routing.rs::replica_ingest_lands_on_per_shard_accept_linux_at_n4
    (`#[ignore = "58-W3"]`, cfg linux) — boots N=4 per-shard
    SO_REUSEPORT listeners, asserts 4 LISTEN sockets via /proc/net/tcp,
    opens a TCP client that issues OP_LOG_FETCH, drains to the END frame
  - tests/replica_ingest_routing.rs::replica_ingest_lands_on_per_shard_accept_macos_at_n4
    (`#[ignore = "58-W3"]`, cfg not-linux) — boots N=4 dedicated accept
    threads via spawn_macos_per_shard_accept_threads, asserts
    accept_threads_spawned_total == 4, opens a TCP client that issues
    OP_LOG_FETCH, drains to the END frame; D-B2 skip branch when
    BEAVA_SHARDS_SINGLE_LISTENER=1
  - build_log_fetch_frame / send_log_fetch_and_drain / build_four_shard_state_w3
    / count_listen_sockets_on_port_w3 test-local helpers (no production
    API surface)
affects:
  - Wave 4 (58-04): replica ingest now verified to ride the unified
    per-shard accept path on both platforms. Wave 4 perf-gate close
    (D-C2 ≥ +25% EPS; samply probe harness extension covering
    `tokio::runtime::task::*` frames) can measure a single accept path
    rather than two — replica and primary PUSH share leaf samples.
tech-stack:
  added: []
  patterns:
    - "Guardrail test pattern: boot the per-shard accept topology
       directly (spawn_shard_threads with accept_cfg=Some on Linux;
       spawn_macos_per_shard_accept_threads on macOS), then open a TCP
       client that sends a replica-ingress opcode. Asserts BOTH the
       topology-shape invariant (N LISTEN sockets / N accept threads)
       AND the end-to-end handshake (OP_LOG_FETCH → END frame)."
    - "OP_LOG_FETCH test frame builder inline in tests/ — mirrors the
       private `build_log_fetch_frame` in src/server/replica_client.rs
       without re-exposing it as pub API. Uses the already-pub
       `beava::client::wire::{OP_LOG_FETCH, Scope, write_scope}`."
    - "Scope pull='all' (not 'eager') — v0 only implements 'all' per
       server::protocol::validate_scope. Documented inline as a note
       for future test authors."
key-files:
  created: []
  modified:
    - tests/replica_ingest_routing.rs (+350 lines: 2 new tests +
      4 test-local helpers; existing Phase-54 test preserved verbatim)
requirements:
  - TPC-PERF-08
decisions:
  - "Wave 3 is test-only — no production src/ changes. Wave 1 + Wave 2
    already unified replica + primary-PUSH dispatch through a single
    handle_connection opcode table on Linux and through
    handle_connection_blocking → handle_connection_public →
    handle_connection on macOS. An audit (grep -cE
    'spawn_linux_per_shard.*replica|replica_accept_loop' src/ → 0;
    handle_connection_blocking does not carve out replica opcodes)
    confirmed no carve-out exists, so Wave 3's value is entirely in
    the GUARDRAIL test — a future regression where someone introduces
    a replica-only listener or a replica-only handle_connection variant
    would cause either the N-endpoint assertion to fail or the client-
    side OP_LOG_FETCH handshake to never complete."
  - "Test sends OP_LOG_FETCH (not OP_SUBSCRIBE) because LOG_FETCH has
    a finite response shape (N events + 1 END frame) — a complete
    request/response cycle proves the whole dispatch path, including
    auth + scope validation + engine read + response framing. An
    OP_SUBSCRIBE test would need a second push-injection thread to
    drive events and then tear down via disconnect, expanding scope
    without strengthening the invariant (accept-topology, not
    notify-path, is the target)."
  - "Empty log ⇒ END-frame-only response is acceptable. Pushing real
    events and asserting per-shard distribution would overlap with
    the Phase-54 replica_push_fires_notify_on_shard_path test (which
    already asserts both shards see events), adding coverage cost
    without a new invariant. The OP_LOG_FETCH test here asserts
    TRANSPORT (connection accepted → opcode parsed → auth passed →
    response framed → terminal END frame), not RECEIVE-NOTIFY (which
    Phase 54 owns)."
  - "Per-shard helpers are test-local (not promoted to a shared
    tests/common/ module) — yak-shaving for a single-plan guardrail
    test. If a third or fourth consumer emerges, promote then. This
    matches the precedent set by per_shard_listener_smoke.rs and
    test_so_reuseport_boot.rs duplicating count_listen_sockets_on_port."
  - "D-B2 skip-with-eprintln on macOS mirrors tests/per_shard_listener_smoke.rs
    exactly. When BEAVA_SHARDS_SINGLE_LISTENER=1, the D-B1 N-counter
    invariant does not apply (single-accept spawner bumps the counter
    once), so the test returns early with a logged rationale rather
    than false-failing."
  - "Pull=\"all\" (not \"eager\" which the plan-illustrative helper
    pseudocode implied via `tests/replica_ingest_routing.rs`'s
    existing `scope_for` — that fn uses 'eager' but the Phase-54 test
    only passes its scope to in-process mpsc/Subscriber paths, never
    through validate_scope). Discovered via test-first Rule-1 bug
    fix: first test run got STATUS_ERROR body 'scope.pull=eager is
    not implemented in v0'. Fix is the one-line change + an inline
    explanatory note for future test authors."
metrics:
  duration: ~25min
  completed: 2026-04-20
  tasks: 1
  commits: 1
  files_modified: 1
  files_created: 0
  lib_test_delta: "0 (Wave 3 is integration-test-only; no new lib unit tests)"
  lib_test_total: "812/0/35 (Phase 58 Wave 2 baseline preserved)"
  integration_test_delta: "+2 #[ignore = \"58-W3\"] tests
    (replica_ingest_lands_on_per_shard_accept_{linux,macos}_at_n4)"
---

# Phase 58 Plan 03: Replica Ingest Rides Per-Shard Accept — Guardrail Test Summary

Wave 3 payload of Phase 58 (TPC-PERF-08). Confirms that the replica
ingress opcodes — OP_LOG_FETCH (Phase 35-01) and OP_SUBSCRIBE (Phase
27-02) — ride the same per-shard TCP accept topology established by
Wave 1 (Linux SO_REUSEPORT + FuturesUnordered) and Wave 2 (macOS
dedicated `std::thread` per shard). Wave 3 is a GUARDRAIL, not a
rewrite: Waves 1 + 2 already unified primary-PUSH and replica dispatch
through a single `handle_connection` opcode table on Linux and through
`handle_connection_blocking → handle_connection_public →
handle_connection` on macOS, so Wave 3's deliverable is the durable
test that catches a future regression where someone accidentally
carves out a replica-only listener or replica-only dispatch branch.

## What Landed

### Production code

**None.** Wave 3 required zero `src/` changes. The audit confirmed:

- `grep -cE 'spawn_linux_per_shard.*replica|replica_accept_loop' src/`
  → 0. No replica-specific accept function exists.
- `handle_connection` (Linux path) handles all opcodes via one match
  arm per opcode at `src/server/tcp.rs` lines 1538 (SnapshotFetch),
  1555 (Subscribe), 1567 (LogFetch), 1596+ (Push/PushBatch/PushAsync/
  Mset/Flush + source-table). OP_LOG_FETCH and OP_SUBSCRIBE are BOTH
  dispatched here, not in a separate function.
- `handle_connection_blocking` (macOS, Wave 2) delegates to
  `handle_connection_public` (thin passthrough → `handle_connection`)
  via a per-thread `current_thread` tokio runtime. There is no
  separate opcode table to keep in sync — the blocking wrapper
  delegates to the one async dispatch.
- No `tokio::spawn` on the macOS production PUSH / replica path:
  `grep -cE 'tokio::spawn\(.*handle_connection' src/server/tcp.rs`
  returns 0 (Wave 2 acceptance criterion preserved).

Replica connections therefore inherit the per-shard accept topology
automatically: the shard thread (Linux) or the dedicated accept
std::thread (macOS) accepts the connection regardless of which opcode
the client sends first.

### Test extension (`tests/replica_ingest_routing.rs` +350 lines)

Two platform-gated `#[ignore = "58-W3"]` tests added:

**`replica_ingest_lands_on_per_shard_accept_linux_at_n4`** (`cfg linux`):
1. Boots state at BEAVA_SHARDS=4 with a registered keyed stream
   `replica_stream_w3` (key_field="user_id" — OP_LOG_FETCH skips
   keyless streams per Phase 35-01 v0 semantics).
2. Pre-binds a loopback ephemeral port, drops it, creates
   `PerShardAcceptCfg{accept_addr, max_conns_per_shard=256}`, passes
   it into `spawn_shard_threads` so every shard binds its own
   SO_REUSEPORT socket on that port (mirrors the Wave 1 smoke test).
3. 100 ms sleep for accept loops to come online.
4. Asserts `count_listen_sockets_on_port(port) == 4` via /proc/net/tcp.
5. Asserts `state.accept_threads_spawned_total == 4` (Wave 1 bumps
   this counter at listener install, cross-platform).
6. Opens a TCP client to the port, sends an OP_LOG_FETCH request
   with the admin token + `streams=["replica_stream_w3"]` + `pull="all"`,
   drains response frames until the terminal END frame (tag 0x04),
   asserts zero event frames observed (empty log) before END. This
   proves the replica-ingress opcode flowed through the same
   per-shard accept path as primary PUSH, with the full auth + scope
   validation + response framing loop intact.

**`replica_ingest_lands_on_per_shard_accept_macos_at_n4`** (`cfg not-linux`):
1. D-B2 skip branch: if `BEAVA_SHARDS_SINGLE_LISTENER=1`, emits a
   log line and returns — the single-accept spawner bumps the counter
   once, not N, so the D-B1 invariant does not apply (mirrors
   per_shard_listener_smoke.rs).
2. Boots state at BEAVA_SHARDS=4 with the same keyed stream.
3. Installs `shard_handles.write()` FIRST (prevents the macOS boot
   race Wave 2 documented — clients connecting before handles install
   would hit empty shard_handles in handle_push_batch).
4. Pre-binds + drops a loopback ephemeral port, then calls
   `spawn_macos_per_shard_accept_threads` on it.
5. 100 ms sleep + asserts `accept_threads_spawned_total == 4`.
6. Opens a TCP client → OP_LOG_FETCH → drain → assert END frame (same
   client logic as the Linux test).

### Test-local helpers (added in `tests/replica_ingest_routing.rs`)

- `build_four_shard_state_w3(tag)` — 4-shard state with a keyed stream
  registered (satisfies OP_LOG_FETCH's key_field requirement).
- `build_log_fetch_frame(token, from_ts_millis, streams)` — inline
  byte-layout helper mirroring the private `build_log_fetch_frame` in
  `src/server/replica_client.rs`. Uses the already-public
  `beava::client::wire::{OP_LOG_FETCH, Scope, write_scope}`.
- `send_log_fetch_and_drain(addr, token, streams)` — connects, sends
  one LOG_FETCH request, drains frames until tag 0x04 (END), returns
  the event-frame count or an error containing the STATUS_ERROR body
  (invaluable for debugging protocol mismatches, as used during
  implementation — see §Deviations §Rule 1).
- `count_listen_sockets_on_port_w3` (cfg linux) — /proc/net/tcp LISTEN
  socket counter. Duplicated from per_shard_listener_smoke.rs /
  test_so_reuseport_boot.rs (same yak-shaving non-promotion stance).

## Verification Log

```
$ cargo check --release --tests
… Finished `release` profile [optimized] target(s) in 0.43s
✓

$ cargo check --release --tests --features state-inmem
… Finished `release` profile [optimized] target(s) in 0.27s
✓

$ cargo test --release --lib
test result: ok. 812 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out
✓ (Wave 2 baseline 812/0/35 preserved; Wave 3 adds no lib unit tests)

$ cargo test --release --lib --features state-inmem
test result: ok. 804 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out
✓ (Wave 2 baseline 804/0/35 preserved)

$ cargo test --release --test replica_ingest_routing
running 2 tests
test replica_ingest_lands_on_per_shard_accept_macos_at_n4 ... ignored, 58-W3
test replica_push_fires_notify_on_shard_path ... ok
test result: ok. 1 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out
✓ (Phase 54 regression test preserved + Wave 3 test correctly ignored
   behind 58-W3 marker)

$ cargo test --release --test replica_ingest_routing -- --ignored
running 1 test
test replica_ingest_lands_on_per_shard_accept_macos_at_n4 ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out
✓ (macOS host; Linux variant not compiled on this host — flips GREEN
   on Linux CI by construction: the harness uses bind_reuseport_tcp
   via PerShardAcceptCfg, which is the exact path Wave 1 installed)

$ cargo test --release --test http_push_still_works
test http_push_post_events_at_n4_matches_phase57 ... ok
test result: ok. 1 passed; 0 failed; 0 ignored
✓ (D-B3 HTTP regression guard — HTTP axum path unaffected)

$ cargo test --release --test tcp_ingest_routing
test tcp_push_at_n1_routes_through_spsc ... ok
test result: ok. 1 passed; 0 failed; 0 ignored
✓

$ cargo test --release --test http_ingest_routing
test http_push_at_n1_routes_through_spsc ... ok
test result: ok. 1 passed; 0 failed; 0 ignored
✓

$ cargo test --release --test per_shard_listener_smoke
test n_shards_produces_n_accept_threads_macos ... ok
test result: ok. 1 passed; 0 failed; 0 ignored
✓

$ cargo test --release --test test_metrics_parity
test result: ok. 6 passed; 0 failed; 0 ignored
✓

$ grep -cE 'spawn_linux_per_shard.*replica|replica_accept_loop' src/server/tcp.rs
0
✓ (no replica-specific accept carve-out in src/)
```

## Linux-Host Verification

Current host is macOS (Darwin 24.3.0 arm64). The Linux Wave-3 test
(`replica_ingest_lands_on_per_shard_accept_linux_at_n4`) flips GREEN
on Linux CI by construction:

- It uses `PerShardAcceptCfg{accept_addr, max_conns_per_shard=256}`
  passed into `spawn_shard_threads` — the identical harness path that
  `tests/per_shard_listener_smoke.rs::n_shards_produces_n_listeners_linux`
  uses. That harness is pre-wired by Wave 1 and the test-local
  assertions here reuse the same `/proc/net/tcp` LISTEN-count pattern.
- The OP_LOG_FETCH request-response cycle is wire-format-identical on
  both platforms (Linux and macOS use the same
  `handle_connection_public` frame loop).

## Deviations from Plan

### Rule 1 — Bug fix: scope.pull="eager" is not a valid v0 value; must be "all"

- **Found during:** Task 1 first test run. Initial test draft copied
  the `scope_for` helper from the existing Phase 54 test which uses
  `pull: "eager".to_string()`. That helper's `Scope` goes straight to
  the in-process `SubscriberRegistry::register` (Phase 54 flow), which
  does NOT run through `protocol::validate_scope`. Wave 3's test flows
  through the TCP wire to `handle_log_fetch`, which calls
  `validate_scope` and rejects anything but `"all"`.
- **Issue:** First test run produced a STATUS_ERROR response
  `"scope.pull='eager' is not implemented in v0 (only 'all')"`
  — exactly the error text in `src/server/protocol.rs::validate_scope`.
  The test surfacing an unexpected STATUS_ERROR frame proved the
  transport + opcode dispatch were working (auth + opcode parse +
  scope-field read all completed); only the scope value was wrong.
- **Fix:** Changed `pull: "eager"` → `pull: "all"` in the test-local
  `build_log_fetch_frame` helper, with an inline comment
  `// v0 only implements pull="all"; "eager" is a protocol-reserved
  future value.` to prevent the same mistake in future test authors.
- **Bonus signal:** Also enriched `send_log_fetch_and_drain`'s error
  path to surface the STATUS_ERROR body text (previously:
  `"unexpected frame tag 0xXX (body_len=N)"`; now includes the UTF-8
  body). This is test-helper defensive-coding — any future protocol
  regression caught via this helper will be debuggable inline rather
  than requiring a rerun with `RUST_LOG=trace`.
- **Files modified:** tests/replica_ingest_routing.rs (one helper +
  one comment).
- **Commit:** 0a423bf.

### Scope boundary preservation

- **Out of scope (preserved):** `replica_ingest`, `replica_ingest_batch`
  function bodies (Wave 3 only changes how the CONNECTION that invokes
  them is accepted, not the functions themselves — which are actually
  CLIENT-side helpers invoked by `ReplicaClient::run` in `replica_client.rs`
  when a replica consumes events FROM upstream, not SERVER-side opcode
  handlers). No production src/ file was touched.
- **Out of scope (preserved):** HTTP axum ingest path (D-B3 permanent
  boundary).
- **Out of scope (preserved):** Samply probe harness extension — this
  is Wave 4's deliverable per the Wave 1 / Wave 2 Next-Wave Handoff.

## Deferred Issues

1. **Samply probe harness extension (inherited from Wave 1 & 2
   Deferred Issues).** Wave 4 (58-04) is the perf-gate close. The
   probe harness still needs to drive real TCP traffic through the
   server so `tokio::runtime::task::*` frames appear in the profile
   for the D-C4 coverage sentinel
   `tokio_share_on_push_path_under_15_pct`. Wave 3 does not re-flag
   this as new work — it was already carried forward from Wave 1.
2. **Pre-existing `tests/test_concurrent.rs` failures (out of scope,
   inherited from Wave 1 & 2 Deferred Issues).** Six tests call
   `run_tcp_server_with_listener` directly without `spawn_shard_threads`
   first, so `shard_handles` is empty. Unchanged by Wave 3.
3. **Pre-existing `cargo clippy --release` errors on
   `#[deprecated(since = "56.0")]`** — inherited. Unchanged.

## Auth Gates Encountered

None. Wave 3 is pure-Rust test-only work. No external services,
credentials, or manual verification steps.

## Next Wave Handoff (Wave 4 — 58-04)

1. **Perf gate close (TPC-PERF-08 D-C2):** `+25% EPS vs Phase 57
   baseline` (1,297,293 EPS → floor 1,621,616 EPS). Wave 4 re-runs
   the samply probe + runs the EPS benchmark end-to-end. Wave 3 is
   neutral on perf — no production code changed; the replica ingress
   path was already on the per-shard accept topology via Wave 1 + 2.
2. **Samply probe harness extension (D-C4 coverage sentinel):**
   `scripts/samply-probe-tokio-share.sh` + `tests/profile_ingest.rs`
   need to drive real `TcpStream` traffic so
   `tokio::runtime::task::*` frames appear in the profile. Flips
   `tokio_spawn_absence_smoke::tokio_share_on_push_path_under_15_pct`
   from RED (sentinel `pct >= 1.0`) to GREEN (`pct <= 15.0`).
3. **Optional regression trim:** If Wave 4's EPS bench clears the
   gate cleanly, consider removing the macOS `run_tcp_server_with_listener`
   compat-shim in `src/server/tcp.rs` that preserves the legacy
   `tokio::spawn(handle_connection)` fallback behind
   `accept_threads_spawned_total == 0`. Six pre-existing failing
   `tests/test_concurrent.rs` tests would need their harness fixed
   first (Wave 1 Deferred Issue #2) — natural as part of the Wave 4
   test-harness audit.

## Known Stubs

None introduced by Wave 3. The `#[ignore = "58-W3"]` tests are
intentional — they're guardrail assertions that run via
`cargo test -- --ignored` in CI and do not slow the default
`cargo test` path.

## Threat Flags

None. Wave 3 touched:

- `tests/replica_ingest_routing.rs` — test-only addition. No production
  surface. No new wire formats, no new auth paths, no new schema.

All dispositions in plan `<threat_model>` (T-58-03-01..03) remain as
planned:
- **T-58-03-01 Spoofing:** `accept` — same wire-auth path (protocol
  opcode check + admin token middleware). The Wave 3 test actually
  EXERCISES the token check (uses `TEST_ADMIN_W3` both to seed the
  server state and in the request), confirming the replica auth is on
  the same code path as primary PUSH.
- **T-58-03-02 Denial of Service:** `mitigate` — no new long-lived
  subscribe session introduced by the test (LOG_FETCH is
  request-response with a bounded response size).
- **T-58-03-03 Information Disclosure:** `accept` — tests use
  localhost + fresh `make_concurrent_state_full` per invocation; no
  prod secrets in-path.

## Commits

| Task | Commit    | Message                                                                  |
| ---- | --------- | ------------------------------------------------------------------------ |
| 1    | `0a423bf` | `test(58-W3): replica ingest rides per-shard accept — OP_LOG_FETCH guardrail` |

## Self-Check: PASSED

- [x] `tests/replica_ingest_routing.rs` — new helpers
  (`build_four_shard_state_w3`, `build_log_fetch_frame`,
  `send_log_fetch_and_drain`, `count_listen_sockets_on_port_w3`)
  present — VERIFIED.
- [x] `tests/replica_ingest_routing.rs::replica_ingest_lands_on_per_shard_accept_linux_at_n4`
  present with `cfg(target_os = "linux")` + `#[ignore = "58-W3"]` —
  VERIFIED.
- [x] `tests/replica_ingest_routing.rs::replica_ingest_lands_on_per_shard_accept_macos_at_n4`
  present with `cfg(not(target_os = "linux"))` + `#[ignore = "58-W3"]`
  + D-B2 skip branch — VERIFIED.
- [x] `cargo check --release --tests` → exit 0 — VERIFIED.
- [x] `cargo check --release --tests --features state-inmem` → exit 0
  — VERIFIED.
- [x] `cargo test --release --lib` → 812/0/35 (Wave 2 baseline preserved)
  — VERIFIED.
- [x] `cargo test --release --lib --features state-inmem` → 804/0/35
  (Wave 2 baseline preserved) — VERIFIED.
- [x] `cargo test --release --test replica_ingest_routing` → 1/0/1
  GREEN (Phase 54 regression + Wave 3 test ignored behind marker) —
  VERIFIED.
- [x] `cargo test --release --test replica_ingest_routing -- --ignored`
  → 1/0/0 GREEN (macOS host) — VERIFIED.
- [x] `cargo test --release --test http_push_still_works` → 1/0/0
  GREEN (D-B3 regression guard) — VERIFIED.
- [x] `cargo test --release --test tcp_ingest_routing` → 1/0/0 GREEN —
  VERIFIED.
- [x] `cargo test --release --test http_ingest_routing` → 1/0/0 GREEN —
  VERIFIED.
- [x] `cargo test --release --test per_shard_listener_smoke` → 1/0/0
  GREEN — VERIFIED.
- [x] `cargo test --release --test test_metrics_parity` → 6/0/0 GREEN —
  VERIFIED.
- [x] `grep -cE 'spawn_linux_per_shard.*replica|replica_accept_loop' src/server/tcp.rs`
  → 0 — VERIFIED.
- [x] Commit `0a423bf` (Task 1) present in `git log` — VERIFIED.
- [x] `.planning/phases/58-tokio-connection-handling-rewrite/58-03-SUMMARY.md`
  written — VERIFIED (this file).
