# Phase 59: 59-binary-wire-format-for-push — Context

**Gathered:** 2026-04-20 (auto)
**Status:** Ready for planning
**Mode:** Auto (combined CONTEXT + plans in one pass — plan-phase orchestrator could not pre-write CONTEXT due to context budget)

<domain>
## Phase Boundary

Phase 59 eliminates **server-side JSON re-serialization on the TCP PUSH hot
path**. The ~11% CPU cost that Phase 58's handoff notes (51-04-SUMMARY /
58-04-SUMMARY) attributes to `serde_json::*` is NOT the wire parse — it is
the **redundant round-trip inside the server**:

1. Wire parse (already-binary): `decode_event_binary` converts wire bytes →
   `serde_json::Value` — ONE necessary decode per event. Kept.
2. **WASTE ①** `tcp.rs::handle_push_core_ex` (line 2159): `serde_json::to_vec(payload)`
   re-serializes the Value back into JSON-shaped `Bytes` to hand to the shard
   inbox, because `ShardEvent.payload: bytes::Bytes` expects JSON bytes.
3. **WASTE ②** `tcp.rs::handle_push_batch` (line 2538): same pattern per
   event — `serde_json::to_vec(r.payload).unwrap_or_default()`.
4. **WASTE ③** `shard/thread.rs::process_shard_event` (line 724):
   `serde_json::from_slice(&event.payload)` re-parses the JSON bytes back to
   `serde_json::Value` to feed `engine.push_with_cascade_on_shard`.

**Phase 59 changes this:**

- Preserve the TCP wire bytes **verbatim** from `parse_command` →
  `ShardEvent.payload: bytes::Bytes` → shard thread → `push_with_cascade_on_shard`.
- The shard thread decodes ONCE (either via `decode_event_binary` for
  binary payload or `serde_json::from_slice` for legacy JSON) using a new
  `ShardEvent.payload_fmt: PayloadFmt { Binary, Json }` tag.
- Listener never re-serializes. No `serde_json::to_vec` on the hot path.
- Replica ingest and `handle_push_core_ex` carry the format tag through.

**The "new binary codec" is not new.** Beava's binary event encoding
(TYPE_NULL=0x00 / TYPE_BOOL=0x01 / TYPE_I64=0x02 / TYPE_F64=0x03 / TYPE_STR=0x04
with u16-BE field count header) ALREADY EXISTS in `src/server/protocol.rs`
and has been the default TCP OP_PUSH shape since Phase 11. The Python SDK's
`_encode_event_body` (`python/beava/_protocol.py:114`) already emits this
shape. So the phase's "binary codec" scope is: preserve-through-the-pipe,
not invent-a-new-codec.

**Handshake negotiation:** Add `OP_NEGOTIATE_WIRE_FORMAT` (0x18) so a
future binary-only evolution can be advertised. For Phase 59 the server's
capability bit is `WIRE_BINARY_PASSTHROUGH=1`, declaring "I accept
binary-tagged OP_PUSH and will skip the Value round-trip." JSON-over-TCP
(legacy) remains accepted for ≥ 1 release cycle.

**Out of scope (explicit):**
- Changing `decode_event_binary` tag set (already sufficient for all
  observed SDK shapes) — no new TYPE_* tags.
- HTTP PUSH path — stays JSON (user-facing compat; Phase 59 explicitly
  leaves `axum`/`http_ingest.rs` alone).
- A NEW binary codec (postcard, bincode, rkyv, flatbuffers, etc.) —
  deferred. Current `decode_event_binary` already hits the ≤ 3% samply
  gate per the simple-pass-through arithmetic (see §code_context below).
- Hot-key salting (Phase 60), metrics hoist (Phase 61), allocator
  pooling (Phase 62), fjall tuning (Phase 63), Rust bench client (Phase 64).

</domain>

<decisions>
## Implementation Decisions (ALL LOCKED — no grey areas carried into plans)

### Area A — Wire Format Continuity

- **D-A1 (codec choice):** REUSE `decode_event_binary` + TYPE_* tag set.
  Do NOT add `postcard` / `bincode` / `rkyv`. Rationale: postcard is
  already a dep (Cargo.toml:47) for fjall's on-disk encoding but adding
  it to the wire path means a new codec, new test surface, new Python
  emitter. The existing TYPE_* shape is smaller than postcard's struct
  envelope for flat-field events (which ALL Beava events are — no
  nested structs on the wire). Phase 59 wins the 11% by eliminating
  the round-trip, not by picking a new codec.
- **D-A2 (payload format tag):** Add `ShardEvent.payload_fmt: PayloadFmt`
  enum (Binary = 0, Json = 1). Default `Binary` when the listener parsed
  via `decode_event_binary`; `Json` when parsed via `serde_json::from_slice`
  (HTTP path, replica LOG_FMT_JSON path). The shard-thread dispatch on
  this tag to skip the parse when the engine can consume bytes directly —
  see D-C1 for the engine surface.
- **D-A3 (raw_payload passthrough):** `ShardEvent.payload` carries the
  post-opcode, post-stream_name TCP wire tail (after `parse_command`'s
  `read_string(stream_name)`). No re-assembly, no re-wrapping. The
  listener in `parse_command` ALREADY captures `raw_payload = buf.to_vec()`
  (protocol.rs:905) — we now forward that via `bytes::Bytes::from(raw)`
  into ShardEvent and STOP re-serializing.
- **D-A4 (HTTP path untouched):** HTTP POST ingest (`axum::routing` →
  `http_ingest.rs`) continues to receive JSON bytes, parse to Value,
  and pass `PayloadFmt::Json + bytes::Bytes::from(original_json_bytes)`
  to the shard. The HTTP hot path is slower than TCP's anyway (axum
  middleware + serde_json alloc dominate) — Phase 59 does not touch it.

### Area B — Handshake Opcode

- **D-B1 (opcode number):** `OP_NEGOTIATE_WIRE_FORMAT = 0x18` (next free
  after 0x17 = OP_DELETE_TABLE_BATCH). Request wire:
  `[u8 0x18][u32 BE client_capability_bits][u16 BE client_version_tag]`.
  Response wire: `[u8 STATUS_OK][u32 BE server_capability_bits][u16 BE server_version_tag]`.
  `client_capability_bits` / `server_capability_bits` use bit 0 =
  `WIRE_BINARY_PASSTHROUGH` (Phase 59 delivers this bit). All other bits
  reserved — server treats unknown client bits as no-op; future phases
  add more bits.
- **D-B2 (negotiation semantics):** Optional. If client never sends
  OP_NEGOTIATE_WIRE_FORMAT, server still accepts OP_PUSH with either
  JSON or binary payload bodies (format detection is: try binary-parse
  first, fall back to JSON-parse on failure — zero-cost discriminator
  since binary's first 2 bytes are `u16 BE field_count` and a valid
  JSON object starts with `0x7b` = `{` which would decode as
  `field_count = 0x7b??` — far from typical event field counts but
  the discriminator is `buf[0] == b'{' || buf[0] == b'['` tested first).
  Clients that DO negotiate get: server echoes its actual supported
  bits, client can stop sending JSON fallback heuristics.
- **D-B3 (backward compat window):** Server accepts JSON-over-TCP
  OP_PUSH for ≥ 1 release cycle AFTER Phase 59 ships (i.e. through the
  next minor beava version ≥ 0.Z+1). Enforced via:
  (a) `tests/json_over_tcp_still_accepted.rs` RED-to-GREEN integration
  guard at Wave 0, (b) a dated `#[deprecated = "v0.Z+1 removes JSON
  over TCP OP_PUSH; use binary per TPC-PERF-09"]` on the JSON-fallback
  branch. Removal is NOT in Phase 59 scope; filed as 59-NEXT #1.
- **D-B4 (Python SDK emit):** Python SDK's `_encode_event_body` already
  emits binary. Bump `python/beava/_protocol.py` version tag constant
  (`WIRE_VERSION_TAG = 2`) and add `BEAVA_WIRE_NEGOTIATE=1` opt-in env
  that triggers the new OP_NEGOTIATE_WIRE_FORMAT handshake on connect.
  Default remains "just emit binary, don't negotiate" — backward-compat
  for users on older Beava servers.

### Area C — Shard-Thread Payload Consumption

- **D-C1 (engine.push_with_cascade_on_shard signature extension):** Add
  an overload (or a new method `push_with_cascade_on_shard_bytes`) that
  accepts `&[u8] + PayloadFmt` instead of `&serde_json::Value`. The
  implementation's first step is: if `Binary` → `decode_event_binary`;
  if `Json` → `serde_json::from_slice`; then proceed as today. The
  existing method stays (some code paths are Value-in-hand and can't
  gain from this; keep them working). Net effect: the WASTE ③ parse
  in thread.rs:724 moves INSIDE the engine call and happens only ONCE
  (the listener's parse vs the current listener-serialize + shard-parse).
  Actually — we eliminate the listener's WASTE ① + ② AND preserve
  WASTE ③ as a single necessary parse.
- **D-C2 (ShardEvent.payload_fmt default):** Default is `Binary` (the
  TCP-and-Python primary path). HTTP path explicitly sets `Json`.
  Replica ingest reads `LOG_FMT_*` byte from the event log header and
  sets accordingly. No silent-JSON for binary-labeled payloads — a
  mislabeled payload returns `ShardDispatchError::ProcessingError`
  exactly like today's JSON parse error path.
- **D-C3 (Bytes end-to-end invariant):** After Phase 59 lands, this
  grep MUST return zero:
  ```
  grep -rnE 'serde_json::to_vec\(payload\)|serde_json::to_vec\(r\.payload\)' src/server/tcp.rs | wc -l
  ```
  Equivalent grep for `src/server/http_ingest.rs` is EXPECTED TO STAY
  NON-ZERO (Json path is preserved). Enforced via
  `scripts/verify-no-tcp-json-reserialize.sh` (new, Wave 0).

### Area D — Test Scope + Perf Gate

- **D-D1 (RED-first TDD):** Wave 0 plants:
  - `tests/wire_negotiation_handshake.rs` — OP_NEGOTIATE_WIRE_FORMAT
    round-trips the capability bits; RED until Wave 2.
  - `tests/binary_push_bytes_passthrough.rs` — sends a binary-tagged
    OP_PUSH, asserts `shard.events_total` advances AND no `serde_json::to_vec`
    frame appears in a scoped samply probe (via the Wave 0 probe
    script — NOT the general samply-probe). RED until Wave 1.
  - `tests/json_over_tcp_still_accepted.rs` — sends JSON-body OP_PUSH,
    asserts STATUS_OK and entity state correct. RED Wave 0, MUST STAY
    GREEN after every later wave (regression guard for D-B3 ≥ 1
    release cycle).
  - `scripts/samply-probe-json-share.sh` (new, Wave 0) — mirror of
    58's `samply-probe-tokio-share.sh`. Parses pprof top.txt for
    `serde_json::` + `from_utf8` leaf share; emits
    `JSON_SHARE_PCT=` line. Coverage sentinel floor: PCT ≥ 1.0 to
    prevent false-pass on harness-unable (SAME pattern as 58-NEXT #1
    probe-coverage sentinel).
- **D-D2 (perf gate):** ≥ +10% EPS vs Phase 58 C1 baseline on macOS
  dev host (matches 58-PERF-GATE.md§Hardware context — same reference
  laptop). Phase 58 C1 baseline 1,376,450 EPS → **floor ≥ 1,514,095 EPS**.
  Matches ROADMAP D-4. Same harness as Phase 58:
  `MODE=complex DURATION=60 CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576 BEAVA_MAX_CONNS_PER_SHARD=1024 bash benchmark/fraud-pipeline/run_bench.sh`.
- **D-D3 (samply gate):** `serde_json::*` + `from_utf8` combined ≤ 3%
  of leaf samples in re-run samply probe. RED: Phase 58 snapshot
  (pre-59) expected to show ≥ 8% given `to_vec(payload)` + re-parse;
  GREEN: Phase 59 post-close expected to show 1-2% (decode_event_binary
  remains as single necessary parse).
- **D-D4 (p99 parity guard):** p99 per-event push latency must NOT
  regress vs Phase 58 C1's 30,632.5 µs median-of-p99. Within
  ±5% noise floor accepted as parity. Reuses 58's p99 measurement
  convention.

### Area E — Security + Threat Model

- **D-E1 (payload-size DoS cap):** Add `BEAVA_MAX_PAYLOAD_BYTES`
  env-clamped default 1 MiB (1_048_576). Enforce at `parse_command`
  BEFORE any `read_string`/`decode_event_binary` call. Larger payloads
  return `BeavaError::Protocol("payload exceeds BEAVA_MAX_PAYLOAD_BYTES")`
  and close the connection. Zero-cost for events under the cap.
  Rationale: binary decoder's pre-allocation safety (`cap =
  field_count.min(buf.len() / 4)` at protocol.rs:820) already prevents
  the worst pre-alloc amplification; the env cap adds a hard ceiling
  on memory per-frame.
- **D-E2 (handshake downgrade attack):** `OP_NEGOTIATE_WIRE_FORMAT`
  request is stateless (no session key, no crypto). An attacker that
  MitM's the TCP stream could force a downgrade from binary to JSON
  by forging the server's response bits — but Beava's TCP PUSH is
  already not TLS-protected (operator runs behind a reverse-proxy
  for external exposure per `docs/operations.md`). Downgrade attack
  is out-of-scope at the PUSH layer; TLS termination is the
  operator's responsibility. Filed as 59-NEXT if user objects.
  **Threat accepted, not mitigated.**
- **D-E3 (binary decoder overflow):** `decode_event_binary` already
  has bounded pre-alloc (protocol.rs:820) + per-field truncation guards
  (protocol.rs:825, 832, 840, 849). Phase 59 adds NO new decoder paths.
  Extend `tests/protocol_binary_decode_fuzz.rs` (new, Wave 0) with 500
  Arbitrary-generated inputs. Regression guard; should pass immediately.
- **D-E4 (unknown opcode handling):** `OP_NEGOTIATE_WIRE_FORMAT = 0x18`
  lands in an unused opcode slot. Clients on pre-59 servers will
  receive `BeavaError::Protocol("unknown opcode 0x18")` which maps to
  `STATUS_ERROR` on the wire — non-fatal, connection stays open.
  Python SDK's `BEAVA_WIRE_NEGOTIATE=1` MUST handle this gracefully:
  if server returns STATUS_ERROR on the negotiate opcode, fall back
  silently to "emit binary without handshake." Enforced by
  `tests/python_sdk_pre_59_server_fallback.rs` (new, Wave 3).

### Area F — Contingency Ladder for W4 Perf Gate

**Non-negotiable floor = 1,514,095 EPS.** The ladder is evaluated in
order; each tier invoked ONLY if prior tier fails.

- **C1 (pre-allocate per-shard BytesMut):** Pre-allocate a
  `bytes::BytesMut` scratch buffer of `BEAVA_SHARD_INBOX_SIZE * 512`
  on each shard thread's stack (lazy-init on first push). Eliminates
  per-push `bytes::Bytes::from(Vec::new())` allocation. Apply if C0
  misses floor by ≤ 5%.
- **C2 (inline decode):** Skip the `ShardEvent.payload → Value` step
  entirely on the shard thread. Decode directly into per-field
  `engine.push_*` calls, reading one TYPE_ tag at a time and
  dispatching. ~3× more code but eliminates the one remaining
  `decode_event_binary` allocation. Apply if C1 still misses floor.
- **C3 (human_needed escalation):** Same pattern as Phase 56 SC-5 /
  Phase 57 D-D4 / Phase 58 SC-1+SC-3: commit best evidence, document
  the delta, surface to user. Linux-host re-run on Hetzner CCX43 is
  the obvious next step; Phase 59 is a per-event CPU win that should
  translate more linearly to Linux than Phase 58's platform-specific
  runtime-bridge work.

### Claude's Discretion

- Exact `ShardEvent.payload_fmt` encoding (enum repr, derive set,
  Default impl) — pick consistent with existing `ShardOp` repr.
- Whether to introduce `push_with_cascade_on_shard_bytes` as a new
  method OR extend the existing signature with a default param —
  whichever touches fewer call sites; both are acceptable.
- `OP_NEGOTIATE_WIRE_FORMAT` payload wire-byte exact ordering
  (capability bits first vs version tag first) — document
  whichever in `src/server/protocol.rs` opcode comment.
- Python SDK `BEAVA_WIRE_NEGOTIATE` env flag default (on vs off) —
  recommend off for minor-version safety; user-facing flag for the
  next release cycle.
- `BEAVA_MAX_PAYLOAD_BYTES` default value in {256 KiB, 1 MiB, 4 MiB} —
  D-E1 recommends 1 MiB; adjust within this range if a specific
  user workload requires.

### Folded Todos

None. All user-facing decisions are locked above.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 59 Source of Truth
- `.planning/ROADMAP.md` § Phase 59 — goal, success criteria, TPC-PERF-09.
- `.planning/STATE.md` — Phase 58 engineering-complete 2026-04-21 baseline;
  C1 1,376,450 EPS floor denominator for D-D2.
- `.planning/REQUIREMENTS.md` — add TPC-PERF-09 row (Wave 0 deliverable).

### Architecture
- `.planning/arch/TPC-SHARD-DESIGN.md` — TPC architecture baseline.
- `.planning/phases/54-legacy-engine-removal/54-05-SUMMARY.md` — original
  pprof identifying JSON + tokio as the top-two CPU leaves.

### Phase 58 Handoff (directly consumed by Phase 59)
- `.planning/phases/58-tokio-connection-handling-rewrite/58-04-SUMMARY.md`
  § Next Phase Handoff — identifies `src/server/tcp.rs::handle_push_batch`
  JSON parse, `src/client/wire.rs` framing, and
  `src/shard/thread.rs::ShardEvent` payload carriage as Phase 59
  integration points.
- `.planning/phases/58-tokio-connection-handling-rewrite/58-PERF-GATE.md` —
  C1 baseline 1,376,450 EPS + p99 30,632.5 µs (floor denominators).

### Phase 55 Wire-Format Inspiration
- `.planning/phases/55-stream-table-cascade-crossshard-and-source-tables/55-CONTEXT.md`
  § Area B — source-table wire shape + `source_lsn` echo pattern; the
  OP_NEGOTIATE_WIRE_FORMAT response-body-echo convention (D-B1)
  directly mirrors it.

### Existing Wire Surface (already binary — DO NOT re-invent)
- `src/server/protocol.rs::decode_event_binary` — TYPE_NULL/BOOL/I64/F64/STR
  + u16 BE field_count header. IN PRODUCTION SINCE PHASE 11.
- `src/server/protocol.rs::parse_command` OP_PUSH (0x01) / OP_PUSH_ASYNC
  (0x07) / OP_PUSH_BATCH (0x0A) branches — call `decode_event_binary`
  on the post-stream-name tail.
- `python/beava/_protocol.py::_encode_event_body` — Python side of the
  same binary encoding, shipped by Beava SDK.

### Requirements
- `.planning/REQUIREMENTS.md` — TPC-PERF-09 (NEW — added in Wave 0).

### Benchmark Harness
- `benchmark/fraud-pipeline/run_bench.sh` — MODE=complex DURATION=60
  CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576 BEAVA_MAX_CONNS_PER_SHARD=1024
  yields the Phase 58 C1 1,376,450 EPS baseline. Identical invocation
  used by Phase 59 perf gate.
- `scripts/samply-probe-tokio-share.sh` (from Phase 58) — template for
  `scripts/samply-probe-json-share.sh`.

</canonical_refs>

<code_context>
## Existing Code Insights

### Current JSON round-trip WASTE (grep-verified)

| Location | Line | Code | Action (Phase 59) |
|----------|------|------|-------------------|
| `src/server/tcp.rs` | 2159 | `bytes::Bytes::from(serde_json::to_vec(payload).unwrap_or_default())` | **Delete.** Forward `raw_payload` (the already-binary bytes) as `Bytes` with `PayloadFmt::Binary` tag. |
| `src/server/tcp.rs` | 2538 | `bytes::Bytes::from(serde_json::to_vec(r.payload).unwrap_or_default())` | **Delete.** Same pattern in batch path. Forward `raw_payload` per-event (already captured by parse_command at protocol.rs:905). |
| `src/shard/thread.rs` | 724 | `let payload: serde_json::Value = match serde_json::from_slice(&event.payload) { ... }` | **Conditional.** Keep for `PayloadFmt::Json`; replace with `decode_event_binary(&mut &event.payload[..])` for `PayloadFmt::Binary`. |
| `src/server/tcp.rs` | 2023 (replica relog) | `serde_json::to_vec(payload)` in `make_log_payload` | **Conditional.** Use `raw_payload` verbatim when caller has binary bytes; keep JSON re-serialize path only for legacy-only callers. |

### Reusable Assets
- `src/server/protocol.rs::parse_command` captures `raw_payload = buf.to_vec()`
  for OP_PUSH (line 905), OP_PUSH_ASYNC (line 915), OP_PUSH_BATCH (line 1023).
  Plumb this through `Command::Push.raw_payload` → `ShardEvent.payload`
  DIRECTLY via `Bytes::from(raw_payload)` — NO re-serialize.
- `src/server/protocol.rs::decode_event_binary` — already exists;
  extend shard thread to call this when `PayloadFmt::Binary`.
- `bytes::Bytes` is already a dependency (`Cargo.toml:54 bytes = "1.11"`).
  Zero-copy clone via Arc-backed internals.
- `postcard` is already a dep (fjall internal) — NOT used on wire
  path. D-A1 explicitly rejects adding it there.

### Established Patterns
- Phase 50+ shard-thread SPSC dispatch: listener parses, constructs
  `ShardEvent`, calls `handle_clone.inbox_tx.try_send(ev)`. Phase 59
  adds the `payload_fmt` field to ShardEvent; dispatch pattern unchanged.
- Phase 55-02 opcode wire-format style — varint-prefixed strings +
  u32 LE lengths + u64 LE source_lsn — is the convention for new opcodes.
  OP_NEGOTIATE_WIRE_FORMAT follows this style (D-B1).
- Phase 58 samply probe harness-unable sentinel pattern (coverage floor
  ≥ 1.0%) — adopted verbatim for `samply-probe-json-share.sh`.

### Integration Points
- `src/server/tcp.rs::handle_push_core_ex` (line 2081) — signature
  already accepts `raw_payload: &[u8]`. Phase 59 rewires the function's
  internal WASTE ① (line 2159) to forward `raw_payload` as `Bytes` with
  `PayloadFmt::Binary` tag instead of re-serializing `payload`.
- `src/server/tcp.rs::handle_push_batch` (line 2408) — similar rewire
  for the batch path. Each `PendingAsync` already carries `raw_payload: Vec<u8>`
  (line 2230); use it.
- `src/shard/thread.rs::process_shard_event` (line 705) — add
  `PayloadFmt` dispatch; call `decode_event_binary` in Binary branch.
- `src/server/replica.rs` / `replica_ingest_batch` — already handles
  `LOG_FMT_BINARY` vs `LOG_FMT_JSON` (tcp.rs:1763, 1876). Reuse the
  same byte mapping for `PayloadFmt`.
- `src/server/http_ingest.rs` — no changes (D-A4 scope exclusion).
- `python/beava/_protocol.py` + `python/beava/_client.py` — add
  OP_NEGOTIATE_WIRE_FORMAT constant + handshake helper + env flag.

### 11% Arithmetic Breakdown (from Phase 58 pprof + code inspection)

- `serde_json::to_vec` on shard-bound hot path: ~4.5% leaf samples
  (tcp.rs:2159 + :2538 fire once per event).
- `serde_json::from_slice` on shard-thread parse: ~3.5% leaf samples
  (thread.rs:724 fires once per event).
- `std::str::from_utf8` inside serde_json's string decode: ~2% leaf
  samples (called by both sides of the round-trip).
- Misc `serde_json::Value` drop + map alloc: ~1% leaf samples.

Total ~11% CPU, eliminable by passing Bytes through with a format tag.
Decode_event_binary remains as ONE necessary parse on the shard side —
~3% leaf samples, which is the ≤ 3% D-D3 target (arithmetic matches).

</code_context>

<specifics>
## Specific Ideas

- Samply reproducibility: `scripts/samply-probe-json-share.sh` is the
  Phase 59 analog of `samply-probe-tokio-share.sh` and MUST be written
  Wave 0 so Wave 4 has a one-command gate.
- Payload-size DoS cap (D-E1) is small work (~20 LOC) but critical
  for production deployment. Include in Wave 1's codec module landing.
- Python SDK's existing binary-emit is already Phase-59-ready — the
  Wave 3 work is net-additive (handshake + deprecation warning on
  legacy JSON emit path).
- Replica ingest and the `handle_log_fetch` replica-cursor path both
  use `LOG_FMT_*` byte tags. The new `PayloadFmt` enum in ShardEvent
  should map bijectively so `LOG_FMT_BINARY → PayloadFmt::Binary`
  and the replica side gets Phase-59 speedups for free.
- The "invent a new binary codec" temptation is specifically rejected
  by D-A1 — inventor's-fallacy trap. Existing TYPE_* tags are already
  compact and ≤ 3% probe-target achievable without inventing anything.

</specifics>

<deferred>
## Deferred Ideas

- HTTP PUSH wire switch to binary — stays JSON per D-A4. If a customer
  specifically requests it, file as post-v1.3 work with a separate
  phase.
- New binary codec (postcard/bincode/rkyv) — explicit NO per D-A1.
  Filed as "v1.4+ territory; only if TYPE_ proves insufficient."
- Removal of JSON-over-TCP OP_PUSH backward-compat path — D-B3 keeps
  it ≥ 1 release cycle. Removal = 59-NEXT #1 (next minor bump).
- TLS termination / wire encryption to close D-E2 handshake downgrade
  attack — operator-deploy concern; out of Phase 59 scope.
- io_uring for Linux server on the binary PUSH path — explicit "v1.3 /
  Beava Cloud" territory per ROADMAP.
- `BEAVA_WIRE_NEGOTIATE=1` default-on — D-B4 default-off for minor
  version safety; flip default in next minor cycle.

</deferred>

---

*Phase: 59-binary-wire-format-for-push*
*Context gathered: 2026-04-20 (auto, by /gsd-plan-phase)*
