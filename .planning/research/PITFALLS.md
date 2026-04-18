# Pitfalls: Migrating Beava (DashMap-backed) to TPC + Full Key-Shard

**Scope:** Beava-specific pitfalls for the TPC + full key-shard migration (design doc
`.planning/arch/TPC-SHARD-DESIGN.md`). Does NOT duplicate Iggy's already-catalogued pitfalls
(RefCell/await panic, non-deterministic broadcasts, io_uring completion order, per-event uring
submit). Complements TPC-RESEARCH.md §"Pitfalls they hit."

**Evidence basis:** TPC-SHARD-DESIGN.md, TPC-RESEARCH.md, `src/state/`, `src/engine/`,
prior CORR-series commits (CORR-01, CORR-02, CORR-05, CORR-06, CORR-10).

---

## 1. Determinism Pitfalls When Porting Existing Operators

### 1.1 Inter-Shard Join Ordering: The Phantom Total Order

**Scenario:** A join operator joins stream A (high-traffic, shard 3) and stream B
(low-traffic, shard 7). Shard 3 processes 50K events/s; shard 7 processes 2K events/s.
An event at t=100 on shard 3 joins against the B-side state that shard 7 had at t=90 — the
join sees stale state as if time went backwards.

**Evidence:** Today Beava joins run inside `push_with_cascade_internal` against the shared
`StateStore` (DashMap) — every join read sees the latest state regardless of which worker
thread wrote it. Under TPC, shard N only owns keys where `hash(key) mod N_SHARDS == N`.
A join between streams on different shards requires a cross-shard read; the design doc §3
labels this "dangerous" and requires co-located shard keys. The join's per-shard ordering
is not captured by wall-clock or event-time alone; it depends on message queue depth and
reactor scheduling, which varies run-to-run (precisely the non-determinism Iggy flagged with
background broadcasts).

**Prevention:** Design doc §3 requires `shard_key=` co-location at register time. Wave 3
must add a hard register-time guard: if stream A and stream B are joined and their shard
keys hash differently for the same logical entity, reject with an error. The guard must also
verify that the *hash function* (ahash) and *N_SHARDS* are identical at registration — a
stream registered at N=4 and a stream registered at N=8 are incompatible even if both
declare `shard_key="user_id"`. No existing test covers this; add a property test: push
events to two co-located streams at different rates, assert join results are identical to
N=1 replay.

**Severity:** LAUNCH-GATE — joins are a first-class Beava feature; silent reordering is a
correctness regression against crash-replay determinism (Goal 7).

---

### 1.2 Watermark Asymmetry and TTL Eviction Skew

**Scenario:** Shards advance their watermarks at different rates. Shard 3 (high-traffic)
has `observed_max` at t=3600; shard 7 (low-traffic) is at t=2000. The global watermark
is `min(all shards) = t=2000`. Keys on shard 3 whose last event was at t=2500 — inside
the global watermark — will NOT be evicted by shard 3 (its local `observed_max` is t=3600,
so `age = 3600 - 2500 = 1100s`, which might exceed TTL). But identical-logical-time keys
on shard 7 may survive or be evicted differently, breaking the invariant that TTL is
event-time-uniform across keys.

**Evidence:** `src/state/eviction.rs` line 67: `let scan_clock = engine.watermarks.observed_max(stream_name).unwrap_or(now)` — this is the per-stream (not per-shard) observed max. Under TPC, if watermarks become per-shard, each shard's eviction clock diverges. The design doc §5 says "per-entity TTL eviction uses the shard-local watermark (no cross-shard needed)" — this is the correct approach, but it means two keys with the same `last_event_at` can be evicted at different wall-clock times depending on which shard owns them. This is observable: a user querying features for two entities at the same event-time may see one evicted and one alive.

**Prevention:** Document this as an intentional divergence from current behavior at Wave 3.
Add a test: create two keys on different shards, drive shard 3's watermark to T+2h while
holding shard 7 at T; assert that the shard-3 key evicts while the shard-7 key survives,
and that this matches the documented per-shard-clock semantics. The alternative
(evicting by the global watermark) would re-introduce cross-shard coordination on every
eviction scan — do not go there. The doc gap is that §5 does not explicitly state the TTL
divergence is intended behavior.

**Severity:** SHIP-GATE — must be documented and tested before Wave 3 merge. Not a
correctness regression (eviction is best-effort) but it breaks user mental models if silent.

---

### 1.3 Per-Shard HashMap Cold-Start: Registration-Before-Push Ordering

**Scenario:** DashMap in v1.0 lazily creates per-key entries on first event. A per-shard
`HashMap<String, EntityState>` has the same lazy-insert behavior, but stream registration
also allocates per-stream watermark trackers and log writers. Under TPC with N=8, if a
stream is registered on the control-plane thread (HTTP /register handler) and then the
first event arrives on shard 3, shard 3 must have already received the stream definition
— otherwise it drops the event or errors.

**Evidence:** `src/state/event_log.rs` `register_stream()` inserts into a `DashMap<String,
LockFreeStreamLog>` keyed by stream name — global today. Under Wave 1, each `Shard` struct
gets its own `HashMap` and event log. The registration broadcast from the control-plane
thread to all N shard threads must complete before any PUSH for that stream arrives. If
a client registers and immediately pushes (common in SDK integration tests), a race exists
between the registration broadcast landing in all shard inboxes and the first push being
routed to shard 3.

**Prevention:** Wave 1 must define a registration protocol: the control-plane sends
`RegisterStream(def)` to all N shard inboxes synchronously (waiting for N acks) before
returning HTTP 200 to the registrant. Alternatively, shards treat unknown streams as
"lazy-register from the global registry" — but that requires a global registry, defeating
part of the TPC isolation. The synchronous broadcast with acks is the safe choice; it adds
one round-trip of latency to registration, which is fine (registration is rare). No existing
test covers this; the current test harness calls `engine.register()` synchronously before
any push, so the race never surfaces.

**Severity:** SHIP-GATE — Wave 1 must resolve the registration-before-push ordering contract
or integration tests will be flaky at N>1.

---

## 2. Test-Suite Fragility

### 2.1 Synchronous Push-Then-Query Pattern Will Race at N>1

**Scenario:** A large fraction of engine tests call `engine.push(...)` and then immediately
call `store.get_all_features(key, now)` or inspect the returned `FeatureMap`. At N=1 the
push is synchronous — state is mutated before the function returns. At N>1, the push routes
to a shard via SPSC channel; the calling thread gets a future/response back only after the
shard has processed the event. Any test that pushes via the HTTP or TCP layer without
awaiting the shard-side completion will race.

**Evidence:** `src/engine/pipeline.rs` test at line 2537: `engine.push("Transactions",
&event, &store, now)` followed by `store.get_all_features("u123", now)` at line 2552 — the
push and the subsequent read are synchronous calls to the same `&mut PipelineEngine`. Under
TPC, `push` becomes "enqueue to shard inbox and wait for response" — still synchronous if
the caller blocks on the channel receive. The fragility is in tests that go *through* the
HTTP/TCP server layer without enforcing synchronous round-trip completion (e.g., fire-and-
forget pushes then sleep and read). These will become racy at N>1 even if the unit-test
engine layer stays synchronous at N=1.

**Prevention:** Wave 2 integration tests must use the response channel contract: every PUSH
response must confirm the shard has written the state before HTTP 200 is returned. Mark
all existing `push` + immediate `get_features` unit tests as "N=1 only" with a compile-
time assertion (`#[cfg(not(multi_shard_integration))]`) until Wave 2 validates the
full-round-trip behavior. The unit-test harness itself may remain synchronous at N=1 (this
is safe — N=1 is current behavior per migration compat §7), so most unit tests continue
to pass unmodified.

**Severity:** SHIP-GATE for Wave 2 — undetected races at N>1 will produce intermittent
CI failures that are hard to diagnose.

---

### 2.2 HashMap Iteration Order Assertions

**Scenario:** Tests that assert feature order, stream listing order, or entity key order
across multiple events will break when the underlying iteration is per-shard HashMap (not
DashMap) and shards return results in an unspecified order.

**Evidence:** `src/state/event_log.rs` `registered_streams()` returns `writers.iter().map(|e| e.key().clone()).collect()` — the test at line 885 explicitly `.sort()`s before asserting. If any integration test does NOT sort before comparing stream lists, it will be non-deterministic at N>1. The scatter-gather path for `GET /streams` (Wave 3) will concatenate per-shard results in shard-index order, which is deterministic but differs from DashMap's arbitrary shard ordering. Any test comparing a `GET /streams` result without sorting will break.

**Prevention:** Audit all tests that assert on collections of stream names, entity keys, or
feature maps: require `.sort()` or convert to `HashSet` before asserting equality.
The `registered_streams()` function must document that its output order is unspecified at
N>1. This is a Wave 1 hygiene task: run `cargo test` at N=1 with randomized HashMap seed
(set `RUSTFLAGS=-C randomize-layout`) to surface any hidden order dependencies before Wave
2 introduces actual per-shard non-determinism.

**Severity:** SHIP-GATE for Wave 2 — order-sensitive assertions are silent correctness bugs
that pass at N=1 and fail randomly at N>1.

---

### 2.3 Event-Log Mock Breakage at Wave 4

**Scenario:** Tests that mock or inspect the event log file directly (`data/<stream>.log`)
will break when Wave 4 moves the layout to `data/shard-N/streams/<stream>/log.bin`. Any
test that hardcodes the log file path or reads the log directory listing to verify event
persistence will fail.

**Evidence:** `src/state/event_log.rs` `EventLog::new(log_dir)` takes a path; tests
pass `tmp.path().to_path_buf()` and then directly read `tmp.path().join("Transactions.log")`.
At Wave 4, the path becomes `tmp.path().join("shard-0/streams/Transactions/log.bin")` (or
similar). Any integration test that knows the old path structure is broken. The compaction
test at line 793 (`let log_file = tmp.path().join("S.log")`) and the rename test
(`let tmp_file = tmp.path().join("S.log.tmp")`) are both path-sensitive.

**Prevention:** Introduce a `EventLog::stream_log_path(stream_name) -> PathBuf` accessor
at Wave 1, and update all path-aware tests to use it rather than constructing paths
manually. When Wave 4 changes the layout, only the accessor implementation changes. This
is a one-day refactor that pays dividends across the full test suite.

**Severity:** SHIP-GATE for Wave 4 — test failures here are expected but must be planned,
not discovered mid-sprint.

---

### 2.4 Snapshot Round-Trip Tests Break on Format Extension

**Scenario:** Snapshot tests that save and reload state (verifying round-trip fidelity) will
fail if the snapshot format gains a `shard_count: u16` field (design doc §7) and the test
fixture uses a v7 snapshot blob with no shard count. The test will either load stale fixtures
or produce a false "format unknown" error.

**Evidence:** `src/state/snapshot.rs` already has version migration logic for v5→v6→v7
(`SNAPSHOT_FORMAT_VERSION = 7`, `LEGACY_V5_FORMAT`, `LEGACY_V6_FORMAT`). The test at
`src/state/store.rs` line 1484 (`test_restore_from_snapshot_v4`) demonstrates this
pattern: it hardcodes a v4 payload and asserts migration succeeds. Any test with a hardcoded
v7 payload will need to be updated to v8 (or whatever the TPC snapshot version is). More
critically, the re-sharding tool (Wave 4) rewrites snapshot state; any test that snapshots,
runs the tool, and reloads must use the new format.

**Prevention:** Define `SNAPSHOT_FORMAT_VERSION = 8` at Wave 4 with a migration path for
v7 (add `shard_count: u16` defaulting to 1). Add a dedicated migration test: save v7
snapshot, run the upgrade path, verify the loaded state matches at N=1. Keep the v7
fixture file in `tests/fixtures/` under version control so regressions are detectable.

**Severity:** SHIP-GATE for Wave 4 — snapshot regression equals data loss on upgrade.

---

## 3. Hot-Shard Footguns

### 3.1 Benchmark Hygiene: Uniform Hash Conceals Real Imbalance

**Scenario:** The 9-cell matrix benchmark uses synthetic workloads with uniformly random
keys. `shard_probe` reports `cross_shard_fraction < 40%` and `beava_shard_keys_owned` is
balanced across all shards. The team ships. In production, a SaaS customer's 10 largest
merchants account for 40% of all transactions; those 10 keys land on at most 3 shards,
saturating those shards while the other 13 sit idle. The performance regression is invisible
until a customer complains.

**Evidence:** ScyllaDB issue #7797 "Hot partitions for a specific shard" — cited in
TPC-RESEARCH.md §2.1 — documents exactly this failure mode. The shard_probe in
`src/server/shard_probe.rs` measures cross-shard fraction but does not measure key-density
imbalance (max keys per shard / mean keys per shard). The design doc's Q6 adds
`beava_shard_keys_owned{shard="N"}` gauge, which surfaces this — but only after shipping.

**Prevention:** Before Wave 5 load test, add a workload scenario to the 9-cell matrix that
simulates a Pareto (80/20) key distribution: 20% of keys generate 80% of events. The
target: `beava_shard_reactor_utilization` variance across shards must be <2× at steady
state under the Pareto workload. If it exceeds 2×, the architecture gate (ship gate #3)
must flag this explicitly. Document in `docs/operations.md`: "for workloads with key Pareto
ratio >80/20, pre-warm shard awareness by running shard_probe on a representative sample
before enabling N>1."

**Severity:** SHIP-GATE — shipping clean bench numbers against a production workload that
immediately invalidates them is a v1.2 credibility risk.

---

### 3.2 Single Hot Key Saturates One Shard: No Mitigation Path

**Scenario:** A single entity key (e.g., the primary "global" merchant in a multi-tenant
pipeline) receives 90% of all events. Its shard's reactor utilization hits 100%; the shard
inbox (`beava_shard_inbox_depth`) grows unboundedly. Other shards at 5% utilization. The
operator has no control-plane knob to re-distribute or salt the key.

**Evidence:** Design doc §Non-goals explicitly excludes key salting. The design doc §Risks
does not list single-hot-key saturation as a risk — only "shard imbalance" generally. The
metrics in Q6 expose the symptom (`beava_shard_keys_owned`, `beava_shard_reactor_utilization`)
but provide no remediation path. The crossbeam-channel SPSC channel is bounded
(`bounded()` in design doc §6); once the inbox is full, the listener thread blocks
(backpressure). This propagates upstream: the TCP or HTTP listener stalls, increasing
per-request latency for ALL connections on that listener, not just the hot-key connections.
CORR-10 addressed the dirty-set race using arc-swap rather than dropping, but inbox
backpressure behavior under shard saturation is not covered by any existing CORR work.

**Prevention:** Wave 2 must define and test the backpressure contract: when a shard inbox
is full, does the listener block (at-least-once, backpressure propagated upstream) or drop
(best-throughput, at-most-once)? The design doc says `crossbeam-channel::bounded()`, which
blocks on full — this is the right default for at-least-once semantics. Document it
explicitly and add a test: saturate shard 3's inbox, assert that the listener returns a
503 or blocks rather than silently dropping events. Add a `beava_shard_inbox_full_total`
counter (not in current Q6 metrics list) to distinguish backpressure events from processing
drops. Note the design-doc gap: §6 and the Q6 metrics table do not include a counter for
inbox-full events.

**Severity:** LAUNCH-GATE — backpressure behavior is a correctness contract; undefined
behavior under saturation is a data-integrity risk.

---

### 3.3 Cascading Overload: Hot Shard Stalls Listener Threads Globally

**Scenario:** Shard 3 is saturated. The listener thread trying to send to shard 3's SPSC
channel blocks. That listener thread is also responsible for dispatching events to shards
4, 5, 6, etc. All events destined for any shard are delayed until shard 3 drains.

**Evidence:** Design doc §6 describes a "listener → shard SPSC channel" architecture where
listener threads route to shards by shard_hint. If listener threads are shared across
multiple shards (the dispatcher pattern, used on macOS and as the Wave 0 default before
SO_REUSEPORT), a blocked send to shard 3 stalls the entire listener. Even with SO_REUSEPORT
(Linux), if a single connection has events for both shard 3 and shard 5, the connection
handler blocks on shard 3 before it can serve shard 5's events. CORR-10's ring-buffer
drop behavior (`beava_late_events_dropped_total`) addresses late events, not inbox
saturation; there is no existing Beava mechanism for non-blocking shard dispatch.

**Prevention:** Use non-blocking send (`try_send`) on the SPSC channel with a per-shard
drop counter rather than blocking the listener. Return HTTP 503 / TCP error code for events
that can't be enqueued within a configurable timeout (`BEAVA_SHARD_DISPATCH_TIMEOUT_MS`,
default 50ms). This changes the delivery model from "block until processed" to "best-effort
with backpressure error," which must be documented as the v1.2 contract. Add
`beava_shard_dispatch_timeout_total{shard="N"}` to the Q6 metrics table — currently absent.

**Severity:** LAUNCH-GATE — uncontrolled listener stall is a global availability failure,
not a single-shard failure.

---

## 4. Python SDK Migration Footguns

### 4.1 Silent Wrong-Shard-Key: Joins That Don't Exist Yet

**Scenario:** A user deploys a pipeline with `@bv.stream` and no `shard_key=`. The server
falls back to the primary-key field (first dataclass field). The primary key is a
high-cardinality UUID (`event_id`), not the business entity key (`user_id`). The pipeline
has no joins, so no register-time error is raised. Six months later, the user adds a join
to an `AccountProfile` stream keyed on `user_id`. The join registration errors because the
shard keys disagree — but all the historical data is already partitioned incorrectly, and
no in-place re-sharding path exists for live data (only the offline re-sharding tool from
Wave 4 covers N_SHARDS changes, not shard_key changes).

**Evidence:** Design doc Q5: "Omitted `shard_key=`: fall back to the stream's primary-key
field (first dataclass field)." The join registration guard (Wave 3) only fires when a join
is declared. The user's data is permanently mis-sharded because the SDK has no mechanism
to validate the shard key's semantic correctness (low vs high cardinality) at stream
creation time.

**Prevention:** At stream registration, if `shard_key=` is omitted, emit a warning in the
server log: "stream 'Transactions' is using default shard key 'event_id'; if you plan to
join this stream with another, declare an explicit shard_key." This is a non-breaking
warning. Additionally, the Python SDK `@bv.stream` decorator should display a deprecation-
style warning in the Python terminal if no `shard_key=` is provided and the first field
appears to be a UUID (heuristic: field name contains "id" and type is `str`). Neither
warning is currently planned. This is a design-doc gap: Q5 does not mention any feedback
mechanism for the fallback path.

**Severity:** POST-RELEASE POLISH for the warning; SHIP-GATE for the join guard (already
planned in Wave 3).

---

### 4.2 Tuple Shard Key with Missing Fields: Hash-of-None or Panic?

**Scenario:** A user declares `shard_key=("region", "user_id")`. An event arrives at
runtime with only `user_id` (no `region` field — e.g., a legacy client or a schema
migration). The server must compute `ahash(None, user_id)` or `ahash("", user_id)`, or
drop the event, or panic. If the behavior is hash-of-None, two events with different
regions hash to the same shard; if it drops, the user loses data silently; if it panics,
the shard thread crashes and takes its queue with it.

**Evidence:** Design doc Q5: "Multi-field shard keys supported as tuple (`shard_key=(
"region", "user_id")`)." No specification for what happens when a field is missing from the
event at runtime. The Python dataclass validation (`pydantic` or manual) happens client-
side; the server receives arbitrary JSON. `shard_hint` computation in the Rust server will
attempt to extract fields from the JSON Value; missing fields produce `serde_json::Value::Null`.

**Prevention:** Define the contract explicitly in Wave 0: missing shard-key fields are
treated as empty string for hashing purposes (`""` not `None`), and a counter
`beava_shard_key_field_missing_total{stream="...",field="..."}` is incremented. Never panic
on missing fields — the design doc §2 fallback already says "never panic on malformed
routing." Add a unit test covering this exact case. This is a design-doc gap: Q5 does not
specify the missing-field behavior.

**Severity:** LAUNCH-GATE — undefined behavior on malformed input can crash shard threads
(the Iggy `RefCell` panic lesson applies here: any panic inside a pinned shard thread kills
that shard's reactor).

---

### 4.3 SDK Version Mismatch: Do v1.1 Clients Need Redeploy?

**Scenario:** An existing production user is running a v1.1 pipeline with no `shard_key=`
parameter (not yet part of the wire protocol). They upgrade the server to v1.2 (TPC
enabled, N>1). Their client SDK still sends v1.1 register packets without `shard_key`. The
server must either (a) accept v1.1 packets and apply the fallback rule silently, or (b)
reject them with a protocol error, forcing client redeployment.

**Evidence:** Design doc §7 "Migration compatibility" states "Wire format: unchanged. TCP
opcodes unchanged. HTTP endpoints unchanged." The `shard_key` field in `@bv.stream` is a
client-side SDK concept only; on the wire it would appear as an additional JSON field in
the `REGISTER_STREAM` packet. If the v1.1 SDK omits `shard_key`, the v1.2 server must
treat it as the fallback (first field) — identical to the Q5 fallback behavior. This is
forward-compatible. However, if the v1.2 server emits a `shard_key` in its stream-metadata
response (for replica/fork use), a v1.1 replica reading the response will encounter an
unexpected field and may error.

**Prevention:** The `REGISTER_STREAM` protocol must treat `shard_key` as an optional field
with a stable default (primary key). Verify that existing v1.1 TCP protocol tests pass
against the v1.2 server at N=1 with no code changes. The design doc's compatibility
guarantee covers N=1 behavior; explicitly test N>1 with a v1.1-format register packet to
confirm the fallback fires correctly and the server does not error.

**Severity:** SHIP-GATE — silent breakage for existing paying users is not acceptable.

---

## 5. Operational Deployment Pitfalls

### 5.1 Rolling Restart with BEAVA_SHARDS Mismatch: Silent Empty-State Start

**Scenario:** A production operator upgrades a running instance from `BEAVA_SHARDS=1` (v1.1)
to `BEAVA_SHARDS=8` (v1.2) via a rolling restart without running the re-sharding tool first.
The new process reads the old `data/` layout (v7 snapshot, no shard subdirectories), fails
to find per-shard data, and starts with empty state — effectively dropping all in-memory
features.

**Evidence:** Design doc §7: "Adding `shard_count` to snapshot header is forward-compatible,
but N=1→N=K rewrites require the re-sharding tool and downtime." The re-sharding tool is
Wave 4 scope; no automated guard prevents a BEAVA_SHARDS change without running it. A
v7 snapshot with no `shard_count` field will be read by the Wave 4 loader as either a
migration trigger or an error; the behavior is unspecified in the current design doc.

**Prevention:** At Wave 4, the startup sequence must:
(1) Read `shard_count` from the snapshot header (default=1 if absent).
(2) If `shard_count != BEAVA_SHARDS`, refuse to start with a clear error message:
"Snapshot has shard_count=1, server configured for BEAVA_SHARDS=8. Run `beava reshard
--to 8` before starting." Do NOT silently start with empty state.
(3) The re-sharding tool must be a documented, mandatory pre-upgrade step whenever
BEAVA_SHARDS changes. This is a design-doc gap: §7 says "document the migration clearly"
but does not specify what happens if the tool is not run — the server currently could
silently start empty.

**Severity:** LAUNCH-GATE — silent data loss on upgrade is the worst possible production
failure mode.

---

### 5.2 Fork/Replica Double-Emit Window During Upstream Rolling Restart

**Scenario:** Upstream is on v1.1 (N=1). A replica is deployed on v1.2 (N=8). The replica
subscribes to the upstream's `OP_LOG_FETCH` stream. Per design doc Q4, the replica
re-hashes on ingest by its own N. However, during the window where the upstream is mid-
upgrade from N=1 to N=8, both the old and new upstream processes may be running
simultaneously (rolling restart). If both processes emit `OP_LOG_FETCH` entries for the
same event range, the replica may ingest duplicates.

**Evidence:** Design doc Q4 resolves fork re-sharding correctly ("always re-hash on
ingest; upstream N is irrelevant"). But the double-emit window is not addressed: the design
doc §7 says "upstream's `shard_hint` in `OP_LOG_FETCH` metadata is a *fast-path hint*" and
does not specify how the replica deduplicates if two upstreams emit the same log range.
The at-least-once delivery guarantee (design doc §Non-goals) permits duplicates at the wire
level; client dedup is the expected mitigation. But for fork/replica, where the client IS
the Beava replica, there is no explicit dedup mechanism.

**Prevention:** The `OP_LOG_FETCH` protocol must include a monotonic log sequence number
(LSN) per shard. The replica deduplicates by LSN: if it sees the same LSN twice (from two
upstream instances), it discards the duplicate. This is a Wave 4 design sub-question not
currently specified in the design doc. The doc gap: Q4 and Wave 4's fork plan do not
address the double-emit window during upstream rolling restarts.

**Severity:** SHIP-GATE for Wave 4 fork/replica work — double-counting corrupts feature
values silently (CORR-01 class of bug).

---

### 5.3 Metric Label Migration Blind Spot: Legacy Unlabeled Counters Go Dark

**Scenario:** An operator runs Beava v1.1 with alerts on `beava_events_total` (unlabeled
global counter). v1.2 with N>1 emits only `beava_shard_events_total{shard="N",outcome="..."}`.
The global unlabeled counter is not emitted. The alert fires (counter goes to zero). The
operator concludes the server is down or not processing events. They page at 2am.

**Evidence:** Design doc Q6 lists per-shard labeled metrics and states "the global
`beava_watermark_lag_seconds` stays; it becomes a derived `min(...)`." But there is no
equivalent statement for `beava_events_total`. The transition from a single global counter
to N per-shard counters creates a label-cardinality change that breaks existing PromQL
queries and alert expressions without warning.

**Prevention:** At Wave 2, when per-shard metrics are introduced, also emit a global
synthetic counter computed as `sum(beava_shard_events_total)` under the original name
`beava_events_total`. This "double-emit" pattern (labeled + unlabeled sum) is the standard
Prometheus migration path. In the deployment runbook, list every metric whose name or
labels change from v1.1 to v1.2. The design-doc gap: Q6 states the unlabeled watermark
lag metric survives but says nothing about the event counter or other unlabeled metrics
that operators may have built alerts on.

**Severity:** POST-RELEASE POLISH for the metric migration docs; SHIP-GATE for the
double-emit pattern — shipping without it is guaranteed to cause false-alarm pages for
early adopters.

---

## Phase-Specific Warning Matrix

| Wave | Topic | Likely Pitfall | Mitigation |
|------|-------|---------------|------------|
| Wave 0 | Shard key missing fields | §4.2 tuple shard key hash-of-None | Define empty-string contract; unit test |
| Wave 1 | Per-shard HashMap | §1.3 registration-before-push ordering race | Synchronous broadcast-and-ack on register |
| Wave 1 | Test suite | §2.2 iteration order assertions | Sort before assert; randomize-layout CI |
| Wave 1 | Test suite | §2.3 path-hardcoded log tests | `stream_log_path()` accessor |
| Wave 2 | Multi-shard routing | §2.1 push-then-query race at N>1 | Mark N=1-only; gate integration tests |
| Wave 2 | Multi-shard routing | §3.3 SPSC inbox full stalls listener globally | Non-blocking try_send with 503 |
| Wave 2 | Multi-shard routing | §3.2 backpressure contract undefined | Document and test block-vs-drop |
| Wave 2 | Metrics | §5.3 legacy unlabeled metrics go dark | Double-emit global sum |
| Wave 3 | Joins | §1.1 inter-shard ordering non-determinism | Hard co-location guard at register |
| Wave 3 | Watermarks | §1.2 TTL eviction diverges across shards | Document and test explicitly |
| Wave 4 | Snapshots | §2.4 format v7→v8 migration | Versioned migration + fixture file |
| Wave 4 | Deploy | §5.1 rolling restart drops state | Startup guard: refuse if shard_count mismatch |
| Wave 4 | Fork/replica | §5.2 double-emit window | LSN-based dedup on replica ingest |
| Wave 5 | Benchmarks | §3.1 uniform hash conceals Pareto imbalance | Add Pareto workload to 9-cell matrix |
| SDK | Python | §4.1 silent wrong shard key, no joins yet | Server-side warning on fallback |
| SDK | Python | §4.3 v1.1 client against v1.2 server | Optional field + compat test |

---

## Design-Doc Gaps Summary

The following pitfalls surface gaps in TPC-SHARD-DESIGN.md not addressed by any existing
wave plan:

| Gap | Location in Doc | Pitfall |
|-----|----------------|---------|
| Inbox-full behavior undefined | §6 (SPSC channel) | §3.2, §3.3: block vs drop contract; missing `beava_shard_inbox_full_total` metric |
| Cascading stall under hot shard not analyzed | §6 | §3.3: dispatcher blocks all shards if one is saturated |
| Tuple shard key missing-field behavior unspecified | Q5 | §4.2: hash-of-None vs empty-string vs drop vs panic |
| No registration feedback for fallback shard key | Q5 | §4.1: no warning when defaulting to primary-key field |
| Startup guard for shard_count mismatch unspecified | §7 | §5.1: silent empty-state start if reshard tool not run |
| Double-emit window during upstream rolling restart | Q4 / Wave 4 | §5.2: requires LSN-based dedup on replica |
| No metric migration plan for unlabeled→labeled counters | Q6 | §5.3: operators get false-alarm pages on upgrade |

---

*Researched: 2026-04-18. Evidence basis: TPC-SHARD-DESIGN.md, TPC-RESEARCH.md,
src/state/event_log.rs, src/state/eviction.rs, src/state/snapshot.rs, src/state/store.rs,
src/engine/pipeline.rs, src/server/shard_probe.rs,
CORR commits (01/02/05/06/10).*
