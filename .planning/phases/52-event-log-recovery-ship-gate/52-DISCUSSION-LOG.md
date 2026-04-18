# Phase 52: event-log-recovery-ship-gate - Discussion Log

> Audit trail only. Decisions in CONTEXT.md.

**Date:** 2026-04-18
**Areas discussed:** Snapshot v7 backward-compat · Parallel recovery thread count · N=1↔N=K parity test scope · Fork/replica double-emit dedup

## Snapshot v7 read compatibility
**Chosen:** Read v7 + v8; write v8 only.
Zero-downtime upgrade from v1.0-launch data dirs. v1.3 drops v7 read (announced deprecation). → D-03, D-04.

## Parallel recovery thread count
**Chosen:** N threads — one per shard. No env knob yet. → D-05.

## N=1↔N=K parity test scope
**Chosen:** All operators (filter, map, agg, join, fork). Nightly ≤10 min; per-PR smoke ≤30s. Hard pre-merge gate. → D-13, D-14.

## Fork/replica double-emit dedup window (SCOPE EXPANSION)
**Chosen:** LSN-based dedup in Wave 4. User opted IN to ~1 week additional work to close the rolling-restart double-emit window. Monotonic u64 LSN = `(upstream_shard_id, stream_ord, seq)`; replica tracks `max_lsn_seen` persistently in snapshot v8 metadata; drops events with LSN ≤ max_seen on reconnect.
→ D-08 through D-11. Extends TPC-CORR-06 scope beyond original REQUIREMENTS.md wording.

## Claude's Discretion
- Exact LSN bit-packing layout.
- Proptest generator design for correlated streams.
- Reshard CLI output format (JSON vs plain text).
