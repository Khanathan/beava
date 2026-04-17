# Event Time Semantics

This is the authoritative reference for Beava's event-time model, covering how events
are bucketed, how watermarks advance, and the guarantees provided at stream boundaries.

## Contents

- Bucket assignment (Plan 08)
- Watermark lateness defaults and per-stream configuration (Plan 08)
- Crash-replay determinism (Plan 08)
- TTL semantics (event-time, not wall-clock) (Plan 08)
- Join idle-input behavior (this page, below)
- Fork watermark propagation (Plan 08)

## Backfill

Backfill uses the single-event ingest path; the 2a batch-path fix does not affect
backfill bucketing.

## Join idle-input behavior

In v1, joins require both sides to produce events for the downstream watermark to advance;
if one side is idle, the join output watermark stalls.
Per-stream idle markers (deferred to v1.1, see DX-06 in REQUIREMENTS.md) would fix join-stall
with silent sides by advancing the watermark when a side is quiescent, but are not in v1.

---

_Sections marked "Plan 08" above are filled in by Phase 46 Plan 08 (OBS-03 -- full
event-time reference)._
