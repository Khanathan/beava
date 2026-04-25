---
phase: 18-redis-hand-roll
artifact: risks
date: 2026-04-24
companion: 18-CONTEXT.md
---

# Phase 18 — Risks register with mitigations

Cross-stage risks called out at planning time. Each plan (18-01..18-06)
also has a stage-local "Risks & mitigations" section; this file is the
phase-wide view, owned by the phase author and reviewed before the
hard-gate stages (18-04.5+).

## R-01 — HTTP parsing edge cases (chunked, trailers, pipelining)

**Description:** Hand-rolling HTTP/1.1 in `httparse` re-introduces all the
spec edge cases that hyper has been hardening against for years. Chunked
transfer encoding, trailers, header continuation lines, multi-value cookies,
keep-alive pipelining state-machine reset, malformed Content-Length headers,
HTTP/0.9 absolute-URI parsing — any of these can hard-crash the apply thread
or leak across requests on a keep-alive connection.

**Stages affected:** 18-01 (initial implementation), 18-04 (write phase
re-introduces serialization edge cases), 18-06 (static-response correctness)

**Likelihood:** medium — `httparse` itself is mature, but the wrapping logic
(body framing, keep-alive reset, pipelining) is hand-rolled

**Impact:** high — incorrect parsing on a public-facing endpoint can
exfiltrate data or crash the server

**Mitigation:**
- Use `httparse` (the same crate hyper uses) for header parse — battle-tested
- Cover the cases that matter with explicit tests (Stage 18-01 §1.3 already
  enumerates: chunked, keep-alive pipelining, malformed boundary,
  `Connection: close`)
- Add a fuzzing target `crates/beava-runtime-core/fuzz/fuzz_targets/http_parse.rs`
  using `cargo-fuzz` — run it for at least 10M iterations before Stage 18.5
  hard gate
- Sanity check: drop a real-world HTTP corpus (e.g. recorded curl traffic)
  through the parser; assert no crashes
- Document in `18-SUMMARY.md`: edge cases NOT supported (e.g., HTTP/0.9 is
  acceptable to refuse with 505)

**Owner:** Stage 18-01 author; reviewed by phase author at 18-04.5 gate

## R-02 — Integration test rewrites (~200 LoC churn)

**Description:** Existing integration tests in `crates/beava-server/tests/`
use `#[tokio::test]` + `tokio::spawn` for client-side concurrency. With the
data-plane no longer running on tokio, those tests need to either:
(a) be rewritten using `std::thread::spawn` + blocking `TcpStream::connect`, or
(b) keep tokio but only on the test-client side (data plane is exercised via
real socket regardless)

**Stages affected:** 18-01 (first stage where tests start failing because the
serve loop is not on tokio anymore), 18-06 (final cleanup)

**Likelihood:** high — this is a definite cost, not a maybe

**Impact:** medium — pure churn, no functional change, but easy to introduce
test-only bugs

**Mitigation:**
- Keep tokio in `[dev-dependencies]` of `beava-server` for test clients only
- Test clients connect to the running `Server` over real sockets; whether
  the server's reactor is tokio or hand-rolled is irrelevant to the test
  client
- Concentrate the rewrites into one or two clearly-tagged commits
  (`test(18-redis-hand-roll-XX): rewrite integration suite for hand-rolled runtime`)
- Phase 18-01 gate: `cargo test --workspace` clean; if it's not, the rewrites
  block 18-02 progression

**Owner:** Stage 18-01 author

## R-03 — I/O threads spin-wait CPU burn at idle

**Description:** Per D-04, the spin-wait barrier between apply and I/O
threads uses `std::hint::spin_loop()` for low wake latency. If the system is
idle (no traffic), naive spinning would peg every I/O thread at 100% CPU,
which is unacceptable for an OSS deployable.

**Stages affected:** 18-03 (spin-wait introduced), 18-04 (extends to write
phase)

**Likelihood:** high — naive spin would absolutely do this

**Impact:** high — we'd ship an OSS daemon that idles at 100% CPU. Deal-
breaker for adoption.

**Mitigation:**
- Per D-04 + Stage 18-03 §3.4: exponential backoff:
  - 0..1024 idle iters: `std::hint::spin_loop()` (cheap, instruction-level)
  - 1024..65536 idle iters: `std::thread::yield_now()` (lets other tasks run)
  - 65536+ idle iters: `parker.park_timeout(100µs)` (true OS sleep until
    `unpark()`)
- `IoPool::publish` calls `slot.parker.unpark()` to wake parked workers
  immediately when work arrives (sub-microsecond wake latency)
- Stage 18-03 has a dedicated test (`test_io_threads_park_when_idle_no_cpu_burn`)
  asserting < 50ms CPU consumed over 500ms of zero-traffic idle
- Document in `18-SUMMARY.md`: idle CPU usage profile + how to tune
  (`io_threads = 0` for fully embedded / single-tenant deployments)

**Owner:** Stage 18-03 author

## R-04 — fsync coordination across pthreads (durability invariants)

**Description:** WAL fsync runs on a dedicated `std::thread` (D-05). The
apply thread appends to a buffered writer via `Rc<RefCell<WalWriter>>`; the
fsync thread periodically takes a snapshot of the buffer and calls
`File::sync_data()`. PerEvent durability (`/push-sync`) requires the apply
thread to wait for fsync's durable-LSN to advance past the request's LSN
before responding. Any bug in the LSN watermark or the
oneshot-pending-list mechanism breaks the read-your-writes invariant —
which is THE core fraud-detection guarantee.

**Stages affected:** 18-02 (introduced)

**Likelihood:** medium — atomic ordering bugs are subtle

**Impact:** very high — silent data loss or false-positive durability acks
would invalidate Beava's fraud-detection use case entirely

**Mitigation:**
- Single shared `durable_lsn: AtomicU64` between fsync worker (writer) and
  apply thread (reader); ordering: writer uses `Release`, reader uses `Acquire`
  (per `18-rust-translation.md` §"Atomics — ordering rules")
- All Phase 6.1 crash tests stay in the suite and run on every Stage 18-02
  commit (per Stage 18-02 verification list)
- Add `tests/durable_lsn_invariants_test.rs` (Stage 18-02 §2.3) covering:
  - Periodic mode: returns BEFORE durable_lsn advances (acceptable)
  - PerEvent mode: returns AFTER durable_lsn ≥ request's LSN (REQUIRED)
  - Concurrent appends: durable_lsn monotonically non-decreasing
  - Crash + restart: events ≤ durable_lsn at crash time are recovered;
    events > durable_lsn are missing (Periodic) or present (PerEvent — fsync
    completed before ack)
- Senior-review checkpoint before Stage 18.3: atomic ordering + LSN
  watermark logic reviewed by user (or trusted second pair of eyes)

**Owner:** Stage 18-02 author; reviewed by phase author + user at 18-03 gate

## R-05 — Cross-runtime handoff for admin endpoints (axum tokio ↔ apply std::thread)

**Description:** Admin endpoints (`/metrics`, `/health`, `/ready`,
`/registry`) stay on tokio/axum on port 8081 (D-01, D-13). Reads from these
endpoints to data-plane state require a cross-runtime bridge. If the bridge
mechanism (shared atomics for `/metrics`, `Arc<RwLock<RegistrySnapshot>>` for
`/registry`) blocks the tokio task, it stalls the admin runtime; if it
blocks the apply thread, it stalls the data plane.

**Stages affected:** 18-01 (introduced)

**Likelihood:** low (cold path; not under load), but easy to mess up

**Impact:** medium — admin endpoints are observability surface; if they
hang, alerting breaks

**Mitigation:**
- Admin endpoints are STRICTLY READ-ONLY from data-plane state. No write-back
  path (D-13).
- `/metrics`, `/health`: access shared `AtomicU64` counters directly — no
  bridge needed; reads are wait-free
- `/registry`: `Arc<RwLock<RegistrySnapshot>>` updated by apply thread on
  every `register` call; admin tokio task reads via `.read()` lock — brief
  contention only at register time (rare)
- For any future write-back from admin (NONE planned for v0): use
  `std::sync::mpsc::SyncSender<AdminCommand>` with bounded capacity; tokio
  task does `tx.try_send(...)` non-blocking; on full → 503 Service Unavailable
- Smoke test in Stage 18-01 §1.5 covers all 4 admin endpoints

**Owner:** Stage 18-01 author

## R-06 — macOS kqueue gets no io_uring benefit (M4 ceiling is real)

**Description:** Apple-M4 is the dev-loop platform but `io_uring` is Linux-
only. macOS retains `mio` over `kqueue` permanently. Per-stage perf gates
on M4 are INFORMATIONAL only (D-14, D-16). The risk: if we get attached to
M4 numbers as a proxy for shipping, we may over-optimize for kqueue
characteristics that don't match Linux io_uring (different sweet spots for
batching, different syscall costs).

**Stages affected:** all stages with M4 perf gates (18-01..18-04)

**Likelihood:** low — Stage 18-04.5 forces the Linux baseline early

**Impact:** medium — could lead to wasted optimization effort or wrong
defaults

**Mitigation:**
- Treat M4 numbers as a cheap regression tripwire only — they tell us if
  something catastrophic broke, but not whether we're on track to hit the
  3M EPS/core target
- Stage 18-04.5 (Linux baseline) lands BEFORE any io_uring work, so by
  Stage 18.5 we already have Linux numbers for mio backend on the same
  hardware as the io_uring numbers — apples-to-apples comparison
- HARD GATE in Stage 18.5 is on Linux Xeon; Stage 18.6 final matrix is on
  Linux Xeon
- Default `io_threads` value chosen from Linux benchmarking, not M4 (M4 has
  4 perf cores → ceiling at 4 threads; Linux Xeon has 24+ → ceiling much
  higher)
- Document in `18-SUMMARY.md` that M4 is a dev platform, not a target
  platform

**Owner:** phase author at 18-04.5 + 18-05 gates

## R-07 — Axum dependency drag for admin endpoints

**Description:** Keeping axum + tokio for admin endpoints adds compile time,
dependency surface, and cross-runtime complexity. The risk: this drags the
binary size up and adds CVE exposure surface for a feature most users won't
hit.

**Stages affected:** 18-01 (introduced), 18-06 (could revisit)

**Likelihood:** low (admin path is narrow)

**Impact:** low (a few MB binary size, ~30s extra compile time)

**Mitigation:**
- Per Stage 18-06 §6.5: split `beava-server` into feature flags:
  - `default = ["hand-rolled-runtime", "admin-axum"]`
  - `admin-axum = ["axum", "tokio/rt-multi-thread"]`
  - Embedded deployments can `--no-default-features --features hand-rolled-runtime`
    to drop axum entirely (no admin endpoints in that build)
- Admin endpoint listener is bound to `127.0.0.1:8081` by default — no
  external attack surface unless explicitly opened
- Document tradeoff in `18-SUMMARY.md`: "axum retained for admin to keep
  tower-middleware ergonomics for /metrics, /health, /ready, /registry; if
  shipping a slim binary, disable `admin-axum` feature and rely on external
  monitoring agent reading `/proc` or netstat"
- Periodically re-evaluate (Phase 19+): if we never get value from axum on
  admin, switch admin to a tiny hand-rolled HTTP/1.1 responder too

**Owner:** Stage 18-01 author for initial; phase author at 18-06 for the
feature-flag split

## R-08 — Senior review needed before Stage 18.5 hard gate (atomic correctness)

**Description:** The combination of atomic counters (`io_pending`,
`durable_lsn`, `stop_flag`), spin-wait barriers, and `unsafe impl Send` for
`ClientRef` per-tick exclusive access is the highest-risk concentration of
correctness logic in the phase. A single wrong `Ordering::` or a missed wake
on the parker can produce intermittent test failures, deadlocks, or worse,
silently-wrong results that look fine in dev and ruin in production.

**Stages affected:** 18-03 (introduces spin-wait + atomic barrier),
18-04 (extends to write phase), 18-05 (io_uring submission lifetimes)

**Likelihood:** medium — these patterns are well-established but the project
hasn't used them before at this scale

**Impact:** very high — concurrency bugs are the worst kind to hit in
production

**Mitigation:**
- Senior-review checkpoint AFTER Stage 18-03 (read I/O threads) and BEFORE
  Stage 18-04 (write I/O threads) — catches the foundational atomic patterns
  before they're cloned into the write path
- Reviewer asks: are all atomic publication pairs Acq/Rel? Is every
  `unsafe impl Send` justified by a per-tick exclusive-access invariant
  documented in a `// SAFETY:` comment? Does every `parker.park` have a
  matching `parker.unpark` somewhere on the wake path?
- Run `cargo test --release` AND `cargo test --debug` for the atomic-coord
  tests — ARM debug builds are stricter about ordering
- Optionally: `loom` integration for the spin-barrier pattern (high
  ceremony — defer unless the review surfaces a smell)
- Surface in `18-RESUME.md` after Stage 18-03 lands: "Senior review needed
  before Stage 18-04 starts"
- Stage 18-05 `io_uring` brings additional unsafe (buffer lifetime in flight
  SQEs); same review applies for that commit

**Owner:** phase author + user (senior reviewer) at 18-03/04 boundary and
at 18-05

## Cross-cutting risk mitigations

These apply across the whole phase, not to any one stage:

1. **Rollback plan**: each stage is behind `--features hand-rolled-runtime`
   until Stage 18-06 cutover. If the hard gate fails irrecoverably, we ship
   v0 on the tokio path and keep `hand-rolled-runtime` as opt-in for v0.1.
2. **Bench tooling debt**: any bench that proves a perf gate must be
   committed; CI should be able to re-run any stage's gate from
   `.github/workflows/perf-linux.yml`. Stage 18-04.5 lands the workflow.
3. **Documentation lag**: every plan author MUST update `18-SUMMARY.md`'s
   running notes after their stage's perf gate run, even if the SUMMARY's
   final form is authored at 18-06. This avoids "what did stage 18-03
   actually achieve?" archaeology at the end.
4. **Phase 18 SCOPE creep**: explicit non-goals (per `18-CONTEXT.md` and
   `18-rust-translation.md` §"What we do NOT translate"): no cluster bus,
   no replication, no Lua, no MULTI/EXEC, no slow log. Reject scope
   additions to Phase 18 — they belong in Phase 19+.

## Reference

- `.planning/phases/18-redis-hand-roll/18-CONTEXT.md` — locked decisions
- `.planning/phases/18-redis-hand-roll/18-redis-research.md` — Redis pattern
  source
- `.planning/phases/18-redis-hand-roll/18-rust-translation.md` — Rust
  mapping + Send/Sync rules + atomic ordering rules
