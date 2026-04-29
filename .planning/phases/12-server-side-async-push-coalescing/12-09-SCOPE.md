---
phase: 12-server-side-async-push-coalescing
plan: 12-09
type: scope-not-yet-planned
captured: 2026-04-29
status: ready-for-planning
depends_on: 12-07
parallelizable_with: 12-08
---

# Plan 12-09 (proposed) — TCP /get schema: MessagePack-first; HTTP keeps JSON

## Diagnosis

Per Plan 12-07 stage trace + Plan 12-08 perf trace, JSON encode+decode dominates apply-thread per-request cost on the read path:

| Stage | p50 cost | % of apply per-/get work |
|---|---:|---:|
| `serde_json::from_slice<BatchGetBody>` (parse body) | 125 ns | 23% |
| `serde_json::to_vec` (serialize response) + BTreeMap build | 167 ns | 31% |
| Hashmap lookup + atomic load (real work) | 125 ns | 23% |
| Other (resolve, lock, drop) | 125 ns | 23% |

**JSON parse + serialize is ~54% of `dispatch_get_batch` apply work.** Eliminating it on TCP would ~2× the per-request apply throughput on the read hot path.

## Goal

Switch TCP `/get` to MessagePack body + response by default. HTTP keeps JSON (browser/curl compatibility). Server detects content_type; same-format response. ~50% reduction in apply-thread `/get` cost.

Custom binary format (CT_BEAVA_BIN with key/feature interning) is **deferred to v0.1+**: gives another 5-10× over MessagePack but adds SDK complexity not justified for v0.

## Architectural rationale

- Redis uses RESP (text-based, simple recursive parser). MessagePack is in the same speed class as RESP — ~50-150 ns per simple object — so this aligns Beava's TCP wire with Redis's "fast simple format" philosophy.
- MessagePack support already exists in the Beava codebase: `CT_MSGPACK = 0x02` is a known content_type byte; push path uses it; `rmp_serde` is a workspace dependency; bench-v18 already drives msgpack push successfully.
- Plan 12-07 wired TCP `/get` opcodes (OP_GET / OP_MGET / OP_GET_MULTI) with `body_format: u8`. The byte is already plumbed end-to-end; we just don't dispatch on it for /get yet.

## Locked decisions

**D-A: TCP /get accepts both CT_JSON and CT_MSGPACK for body.**

`dispatch_get_batch_sync` and `dispatch_get_single_sync` inspect the `body_format` byte from the WireRequest variant:
- `CT_JSON (0x01)` → `serde_json::from_slice` (current path)
- `CT_MSGPACK (0x02)` → `rmp_serde::from_slice` (new fast path)
- Other → `GlueResponse::InternalError("unsupported content_type")`

**D-B: Response uses SAME format as request.**

Server emits the response in the content_type the request used. The TCP encoder (`encode_glue_response_tcp`) already emits the response opcode + content_type byte; this plan just makes the response body match (msgpack response for msgpack request).

`GlueResponse::QueryResult { body: Bytes }` carries the body; the format is decided at dispatch time and stamped onto the encoder. New optional field on the variant: `format: u8` (matches what the bench encoder needs).

**D-C: Python SDK on TCP transport defaults to msgpack for /get.**

Mirrors push-on-TCP which already defaults to msgpack. `app.get(...)` over `tcp://...` uses `CT_MSGPACK`; over `http://...` uses `CT_JSON`.

**D-D: HTTP path UNCHANGED — JSON only.**

Browser/curl compatibility is the main reason HTTP exists. No need to support msgpack on HTTP; Wave 4 of Plan 12-07 already wired HTTP `/get` to JSON. Don't touch it.

**D-E: Custom binary `CT_BEAVA_BIN` is OUT OF SCOPE for this plan.**

A purpose-built format with key-id + feature-id interning would give another 5-10× speedup over msgpack but requires:
- Per-connection key/feature interning step (bidirectional handshake)
- New SDK encoder/decoder
- Wire-format spec
- Backward-compat strategy

For v0 ship, msgpack is enough. CT_BEAVA_BIN is captured as a v0.1+ idea in `.planning/ideas/` but not built.

## Plan structure (waves)

- **Wave 1**: Add `body_format` parameter to `dispatch_get_single_sync` and `dispatch_get_batch_sync`. Branch on CT_JSON vs CT_MSGPACK in body parse. Red-green per format.
- **Wave 2**: Response build path: `dispatch_get_batch` uses `rmp_serde::to_vec` when body_format == CT_MSGPACK. Same shape (`{result: {...}}`); just msgpack-encoded.
- **Wave 3**: TCP encoder + GlueResponse plumbing: `GlueResponse::QueryResult { body: Bytes, format: u8 }`. `encode_glue_response_tcp` writes `format` as the content_type byte in the OP_GET_RESPONSE frame.
- **Wave 4**: apply_shard: pass `body_format` from the TcpGet/TcpMGet/TcpGetMulti variants into the dispatch helpers. Wire it through.
- **Wave 5**: Python SDK: extend `app.get(...)` to use CT_MSGPACK on tcp:// transport. Update `read_bench.py` (or a new TCP variant) to drive msgpack /get.
- **Wave 6**: Tests: TCP /get with msgpack request returns msgpack response; bytes round-trip; Apple-M4 read-path microbench shows ≥40% reduction in p50 apply work (155 ns → ~95 ns single-cell).
- **Wave 7**: Throughput rebaseline; baselines append.

Each wave is red-green-paired per CLAUDE.md TDD.

## Estimated impact

| Metric | Plan 12-07 baseline (Apple-M4) | Plan 12-09 estimate |
|---|---:|---:|
| dispatch_get_batch p50 (single-cell) | 542 ns | ~280 ns |
| TCP /get throughput (32 workers, 1×1) | 175,843 r/s | ~280,000 r/s |
| TCP /get throughput (32 workers, 100×5) | 9,575 r/s | ~16,000 r/s |

Combined with 12-08 (multiplied):

| Metric | Combined 12-08 + 12-09 (Apple-M4) |
|---|---:|
| TCP /get single-cell | ~510,000 r/s |
| TCP /get 100×5 batch | ~28,000 r/s |

(These are rough — actual lift depends on whether Plan 12-08 lands first.)

## Files to read

- `/Users/petrpan26/work/tally/CLAUDE.md` (TDD + perf gate)
- `/Users/petrpan26/work/tally/crates/beava-server/src/runtime_core_glue.rs:188-440` (dispatch_get_*; current JSON-only impl)
- `/Users/petrpan26/work/tally/crates/beava-server/src/apply_shard.rs:198-260` (TcpGet variant dispatch)
- `/Users/petrpan26/work/tally/crates/beava-runtime-core/src/wire_request.rs` (TcpGet variants — already carry body_format)
- `/Users/petrpan26/work/tally/crates/beava-server/src/server.rs:1644+` (encode_glue_response_tcp)
- `/Users/petrpan26/work/tally/crates/beava-core/src/wire.rs:97-100` (CT_JSON, CT_MSGPACK constants)
- Phase 18-09 / 18-10 SUMMARY (precedent for msgpack on push wire path)
- `/Users/petrpan26/work/tally/sdk/python/beava/app.py` (SDK get method)

## Out of scope

- Custom binary CT_BEAVA_BIN — v0.1+
- HTTP msgpack support — kept JSON-only
- push-and-get — Plan 12-10
- Apply-thread overhead reduction — Plan 12-08

## Risks

1. **Apple-M4 numbers don't reflect real hardware** — msgpack lift might be smaller on Linux (perf showed 0.46% serde_json on apply during read; full elimination still significant but less than 50%). Mitigation: measure on Hetzner before claiming the 2× lift.
2. **Python SDK msgpack default changes wire shape** — existing fraud-team users on Python SDK currently send JSON over TCP push (Phase 18-09 changed default). For /get, the SDK currently doesn't have a TCP path at all (only HTTP /get exists in the SDK). New code, no migration risk.
3. **read_bench.py uses HTTP** — the existing harness validates HTTP /get; we'd add a Rust or Python TCP-msgpack /get harness for Wave 7. Or just use bench-v18 with `--read-workers` (already TCP+msgpack-capable post Plan 12-08 if we adjust the get-task to use msgpack body too).

## Status

- **NOT YET PLANNED** — needs `/gsd-plan-phase 12` (or scoped planner)
- **Blocks:** Plan 12-10 push-and-get hot path benefits from this.
