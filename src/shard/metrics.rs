//! Per-shard Prometheus metrics — Phase 50 (Wave 2), D-07.
//!
//! All series emitted via the `metrics` crate global recorder installed in
//! Plan 50-01. The hand-rolled /metrics path remains functional in parallel
//! through Wave 3 (D-06 parallel period).
//!
//! Metric name constants are the single source of truth — no magic strings
//! at call sites.

// ---- metric name constants ----
/// Per-shard reactor utilization gauge (0..1).
pub const SHARD_REACTOR_UTILIZATION: &str = "beava_shard_reactor_utilization";
/// Per-shard SPSC inbox backlog depth gauge.
pub const SHARD_INBOX_DEPTH: &str = "beava_shard_inbox_depth";
/// Per-shard event counter (outcome: accepted|dropped).
pub const SHARD_EVENTS_TOTAL: &str = "beava_shard_events_total";
/// Per-shard owned-key count gauge.
pub const SHARD_KEYS_OWNED: &str = "beava_shard_keys_owned";
/// Per-shard watermark lag in seconds gauge.
pub const SHARD_WATERMARK_LAG_SECONDS: &str = "beava_shard_watermark_lag_seconds";
/// Per-shard inbox-full drop counter (backpressure drops).
pub const SHARD_INBOX_FULL_TOTAL: &str = "beava_shard_inbox_full_total";
/// Per-shard DOWN counter (D-02 panic quarantine events).
pub const SHARD_DOWN_TOTAL: &str = "beava_shard_down_total";
/// Global events-dropped counter with reason label.
pub const EVENTS_DROPPED_TOTAL: &str = "beava_events_dropped_total";
/// Cross-shard fanout counter — DEFINED here, NOT incremented until Wave 3.
pub const CROSS_SHARD_FANOUT_TOTAL: &str = "beava_cross_shard_fanout_total";

// ---- Phase 55-01: cascade metrics (SC-5 — five new series on /metrics) ----
//
// Emitted ONLY by `CascadeBuffer::flush` (cross_shard_total / queue_depth /
// lag_seconds) and the engine's same-shard inline fast path
// (intra_shard_total), plus `record_inbox_depth` (high_watermark).
// `LiveCascadeTargets::dispatch_batch` does NOT emit cross_shard_total —
// that keeps a single emission site and avoids double-count.

/// Phase 55-01 SC-5: per-(source, target) cross-shard TT-cascade deliveries.
/// Counter; incremented by `CascadeBuffer::flush` on successful dispatch.
pub const CASCADE_CROSS_SHARD_TOTAL: &str = "beava_cascade_cross_shard_total";
/// Phase 55-01 SC-5: per-shard same-shard TT-cascade writes (inline fast path).
/// Counter; incremented by the engine's inline cascade path.
pub const CASCADE_INTRA_SHARD_TOTAL: &str = "beava_cascade_intra_shard_total";
/// Phase 55-01 SC-5: per-(source, target) current cascade queue depth.
/// Gauge; set by `CascadeBuffer::flush` to the per-target coalesced-write count.
pub const CASCADE_QUEUE_DEPTH: &str = "beava_cascade_queue_depth";
/// Phase 55-01 SC-5: per-(source, target) cascade dispatch latency (seconds).
/// Histogram; recorded by `CascadeBuffer::flush` using batch-start time.
pub const CASCADE_LAG_SECONDS: &str = "beava_cascade_lag_seconds";
/// Phase 55-01 SC-5: per-shard inbox-depth high-watermark counter.
/// Counter; incremented when target inbox depth crosses 75 % of capacity.
pub const SHARD_INBOX_HIGH_WATERMARK_TOTAL: &str = "beava_shard_inbox_high_watermark_total";
/// Phase 55 MED-1: counter incremented when a PendingRetraction marker
/// append fails (disk full / fsync failure / permission loss). The row is
/// already hard-deleted from state when this fires, so Phase 57
/// retraction propagation will silently miss the row — operators must
/// investigate. Unlabelled so dashboards alert cleanly.
pub const PENDING_RETRACTION_APPEND_FAILED_TOTAL: &str =
    "beava_pending_retraction_append_failed_total";

// ---- Phase 56: cross-shard EnrichFromTable + StreamStreamJoin counters ----
//
// Five new series for SC-1 (EnrichFromTable cross-shard), SC-2 (SSJ
// cross-shard), and SC-3 (register-time relaxation). Emitted by the new
// ShardOp dispatch arms + pipeline.rs helpers. Wave 1 registers + emits on
// dispatch; Wave 2/3 wires the operator-level emitters (intra_shard path,
// register-time warning).

/// Phase 56 D-A1 (TPC-CORR-08): cross-shard EnrichFromTable reads
/// dispatched via ShardOp::ReadEntityAt / ReadEntityBatch. Incremented
/// on the target-shard dispatch arm (one per ReadEntityAt; `keys.len()`
/// per ReadEntityBatch). Labelled by `table`.
pub const ENRICH_CROSS_SHARD_TOTAL: &str = "beava_enrich_cross_shard_total";
/// Phase 56 D-A3: same-shard EnrichFromTable reads (fast path, no inbox
/// hop). Incremented by `PipelineEngine::read_entity_at_shard` when
/// `target_shard_idx == current_shard_idx`. Labelled by `table`.
pub const ENRICH_INTRA_SHARD_TOTAL: &str = "beava_enrich_intra_shard_total";
/// Phase 56 D-A4: EnrichFromTable reads that returned None
/// (null-safe enrichment fields — downstream decides). Incremented by
/// both the cross-shard dispatch arm and the same-shard fast path.
/// Labelled by `table`.
pub const ENRICH_MISSING_TOTAL: &str = "beava_enrich_missing_total";
/// Phase 56 D-B1 (TPC-CORR-09): cross-shard StreamStreamJoin inserts
/// dispatched via ShardOp::SsjInsert. Incremented on the target-shard
/// dispatch arm (one per SsjInsert). Labelled by `join_id`.
pub const SSJ_CROSS_SHARD_TOTAL: &str = "beava_ssj_cross_shard_total";
/// Phase 56 TPC-CORR-04 relaxation: joins registered with mismatched
/// shard_key (left vs right vs join.on). Incremented by `register()`
/// when the mismatch case is detected; co-located joins do NOT bump
/// this counter (D-B5). Labelled by `join_id`.
pub const CROSSSHARD_JOINS_REGISTERED_TOTAL: &str =
    "beava_crossshard_joins_registered_total";

// ---- Phase 57: cross-shard retraction counters (TPC-CORR-10) ----
//
// Five new series for SC-1 (source-table DELETE retraction), SC-2 (SSJ
// tombstone retraction), SC-3 (late-retraction warning), and D-B5 (depth
// guard). Emitted by the new `ShardOp::RetractDownstream` dispatch arm +
// `PipelineEngine::retract_downstream_at_shard` helper. Wave 1 registers +
// emits from the primitives; Waves 2/3 drive real label traffic via
// operator wiring.
//
// Single-emission-site discipline: `RETRACTIONS_SENT_TOTAL` emits ONLY
// from the source-side helper; the target dispatch arm emits exactly one
// of `{APPLIED,NOOPED,BEYOND_HISTORY,DEPTH_EXCEEDED}`. This gives
// dashboards an exact "sent - (applied+nooped+beyond_history+depth_exceeded)"
// leak detector for target-unreachable dispatch failures.

/// Phase 57 D-D2 (TPC-CORR-10): total retraction dispatches issued from
/// source shards. One increment per `ShardOp::RetractDownstream` try_send
/// OR same-shard fast-path call in
/// `PipelineEngine::retract_downstream_at_shard`. Labelled by `operator`
/// (the downstream stream's name) and `reason` (SourceTableDelete /
/// EntityTombstone / PrimaryEventRetract).
pub const RETRACTIONS_SENT_TOTAL: &str = "beava_retractions_sent_total";
/// Phase 57 D-D2: successful target-side retractions — the row was live,
/// is now tombstoned. Incremented exactly once per
/// `RetractOutcome::Retracted` on the target dispatch arm (and on the
/// same-shard fast path in the pipeline helper). Labelled by `operator`.
pub const RETRACTIONS_APPLIED_TOTAL: &str = "beava_retractions_applied_total";
/// Phase 57 D-B4 idempotency surface: retractions that no-op'd because
/// the row was absent or already-tombstoned. Incremented on the target
/// dispatch arm whenever `apply_retraction` returns
/// `RetractOutcome::NoOp`. Labelled by `operator`. High no-op rates are
/// expected under source-side retry + fan-out collisions — not an error.
pub const RETRACTIONS_NOOPED_TOTAL: &str = "beava_retractions_nooped_total";
/// Phase 57 D-C1 surface (SC-3): retractions skipped because the
/// contributing event is older than `watermark - history_ttl`. Wave 1
/// registers the series but does NOT emit (the live check lands with
/// Wave 4's plan 57-04). Labelled by `operator`.
pub const RETRACTION_BEYOND_HISTORY_TOTAL: &str =
    "beava_retraction_beyond_history_total";
/// Phase 57 D-B5 guard trip (SC-4 adjacent): retractions that tripped the
/// 16-hop cascade cap. Unlabelled — trips are rare and the dashboards
/// need a single alertable series. Incremented exactly once per trip
/// (either the dispatch arm's pre-probe or the defence-in-depth check
/// inside `Shard::apply_retraction`, NOT both).
pub const RETRACTION_DEPTH_EXCEEDED_TOTAL: &str =
    "beava_retraction_depth_exceeded_total";

// ---- Phase 53-05 (W-4 revision): per-shard fjall metrics ----
//
// Three UNCONDITIONAL series are emitted per shard:
//
//   * `beava_fjall_write_bytes_total{shard=N}`      — counter (sum of
//     postcard(EntityState) byte counts inserted into partition N).
//   * `beava_fjall_compaction_bytes_total{shard=N}` — counter (bytes
//     reclaimed by fjall compaction). Currently always 0 — fjall 2.11
//     does not expose per-compaction byte counters via its public API.
//     The helper + counter name are defined so Plan 06's alert rules
//     have a real metric to target; a follow-up phase can wire the
//     real number when fjall 3.x lands (or a 2.x patch release adds
//     the accessor).
//   * `beava_fjall_fsync_latency_ms{shard=N}`       — gauge (most
//     recent observed latency of a `persist(SyncData)` call, in ms).
//     The shard hot path uses `PersistMode::Buffer` so no sync runs
//     per-insert; the migration tool + explicit admin fsyncs are the
//     primary emitters. The gauge stays at 0 on shards that have not
//     yet run an explicit persist.
//
// `beava_fjall_cache_hit_ratio{shard=N}` is DELIBERATELY OMITTED
// (W-4). The Plan 01 spike recorded `cache_stats_available: false`:
// fjall 2.11 exposes only `Keyspace::cache_capacity()`, not hit/miss
// counters, so a real ratio cannot be computed. Emitting a hardcoded
// `1.0` placeholder would make Plan 06's `< 0.8 sustained` alert
// vacuous. If a future fjall release exposes cache stats, add the
// gauge + helper then — not now.

/// Per-shard fjall journal+memtable write-bytes counter.
pub const METRIC_FJALL_WRITE_BYTES: &str = "beava_fjall_write_bytes_total";
/// Per-shard fjall compaction-bytes counter (currently unincrementable;
/// fjall 2.11 API exposes no compaction-byte accessor).
pub const METRIC_FJALL_COMPACTION_BYTES: &str = "beava_fjall_compaction_bytes_total";
/// Per-shard fjall fsync latency gauge (milliseconds, most recent sample).
pub const METRIC_FJALL_FSYNC_LATENCY_MS: &str = "beava_fjall_fsync_latency_ms";

/// Outcome of a shard event dispatch.
#[derive(Clone, Copy, Debug)]
pub enum Outcome {
    /// Event was accepted and dispatched to the shard.
    Accepted,
    /// Event was dropped (before or after routing).
    Dropped,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Accepted => "accepted",
            Outcome::Dropped => "dropped",
        }
    }
}

/// Reason an event was dropped at the ingest boundary.
#[derive(Clone, Copy, Debug)]
pub enum DropReason {
    /// Tuple shard_key field missing from event payload (D-10).
    ShardKeyMissing,
    /// Shard SPSC inbox was full (D-08 backpressure).
    InboxFull,
    /// Malformed routing — shard_hint could not be resolved.
    MalformedRouting,
}

impl DropReason {
    fn as_str(self) -> &'static str {
        match self {
            DropReason::ShardKeyMissing => "shard_key_missing",
            DropReason::InboxFull => "inbox_full",
            DropReason::MalformedRouting => "malformed_routing",
        }
    }
}

/// Call once at startup after install_prometheus_recorder(), before shards start.
/// Touches all series with zero so they appear in /metrics even before the first event.
pub fn register_shard_metrics(shard_count: usize) {
    for shard in 0..shard_count {
        let s = shard.to_string();
        // Touch each gauge/counter so it appears in the scrape immediately.
        metrics::gauge!(SHARD_REACTOR_UTILIZATION, "shard" => s.clone()).set(0.0);
        metrics::gauge!(SHARD_INBOX_DEPTH, "shard" => s.clone()).set(0.0);
        metrics::counter!(SHARD_EVENTS_TOTAL, "shard" => s.clone(), "outcome" => "accepted")
            .increment(0);
        metrics::counter!(SHARD_EVENTS_TOTAL, "shard" => s.clone(), "outcome" => "dropped")
            .increment(0);
        metrics::gauge!(SHARD_KEYS_OWNED, "shard" => s.clone()).set(0.0);
        metrics::gauge!(SHARD_WATERMARK_LAG_SECONDS, "shard" => s.clone()).set(0.0);
        metrics::counter!(SHARD_INBOX_FULL_TOTAL, "shard" => s.clone()).increment(0);
        metrics::counter!(SHARD_DOWN_TOTAL, "shard" => s.clone()).increment(0);
        // Phase 53-05 (W-4): touch fjall metrics so they appear in /metrics
        // from the first scrape. cache_hit_ratio is intentionally absent.
        metrics::counter!(METRIC_FJALL_WRITE_BYTES, "shard" => s.clone()).increment(0);
        metrics::counter!(METRIC_FJALL_COMPACTION_BYTES, "shard" => s.clone()).increment(0);
        metrics::gauge!(METRIC_FJALL_FSYNC_LATENCY_MS, "shard" => s).set(0.0);
    }
    // Global reason-labeled drop counter — touch all label variants.
    for reason in &["shard_key_missing", "inbox_full", "malformed_routing"] {
        metrics::counter!(EVENTS_DROPPED_TOTAL, "reason" => *reason).increment(0);
    }
    // Cross-shard fanout counter — defined here, first increment is Wave 3.
    metrics::counter!(CROSS_SHARD_FANOUT_TOTAL, "op" => "list_streams").increment(0);

    // Phase 55-01 SC-5: touch cascade metrics so they appear in /metrics
    // from the first scrape. Labels use (source, target) pairs for
    // per-pair series; touch the (0, 0) placeholder so `# TYPE` lines
    // land before the first real cascade event.
    for src in 0..shard_count {
        let s = src.to_string();
        metrics::counter!(CASCADE_INTRA_SHARD_TOTAL, "shard" => s.clone()).increment(0);
        metrics::counter!(SHARD_INBOX_HIGH_WATERMARK_TOTAL, "shard" => s.clone())
            .increment(0);
        for tgt in 0..shard_count {
            if src == tgt { continue; }
            let t = tgt.to_string();
            metrics::counter!(
                CASCADE_CROSS_SHARD_TOTAL,
                "source" => s.clone(),
                "target" => t.clone(),
            )
            .increment(0);
            metrics::gauge!(
                CASCADE_QUEUE_DEPTH,
                "source" => s.clone(),
                "target" => t.clone(),
            )
            .set(0.0);
            metrics::histogram!(
                CASCADE_LAG_SECONDS,
                "source" => s.clone(),
                "target" => t,
            )
            .record(0.0);
        }
    }

    // Phase 56: touch cross-shard EnrichFromTable + SSJ counters with a
    // placeholder label so the series appear in /metrics from the first
    // scrape. Real labels (`table` / `join_id`) come in at runtime from
    // the ShardOp dispatch arms + pipeline.rs helpers (Wave 2/3).
    metrics::counter!(ENRICH_CROSS_SHARD_TOTAL, "table" => "__init__").increment(0);
    metrics::counter!(ENRICH_INTRA_SHARD_TOTAL, "table" => "__init__").increment(0);
    metrics::counter!(ENRICH_MISSING_TOTAL, "table" => "__init__").increment(0);
    metrics::counter!(SSJ_CROSS_SHARD_TOTAL, "join_id" => "__init__").increment(0);
    metrics::counter!(CROSSSHARD_JOINS_REGISTERED_TOTAL, "join_id" => "__init__")
        .increment(0);

    // Phase 57: touch retraction counters with placeholder labels so the
    // series appear in /metrics from the first scrape. Real labels
    // (`operator` / `reason`) come in at runtime from the
    // `ShardOp::RetractDownstream` dispatch arm +
    // `PipelineEngine::retract_downstream_at_shard` helper.
    metrics::counter!(
        RETRACTIONS_SENT_TOTAL,
        "operator" => "__init__",
        "reason" => "__init__"
    )
    .increment(0);
    metrics::counter!(RETRACTIONS_APPLIED_TOTAL, "operator" => "__init__").increment(0);
    metrics::counter!(RETRACTIONS_NOOPED_TOTAL, "operator" => "__init__").increment(0);
    metrics::counter!(RETRACTION_BEYOND_HISTORY_TOTAL, "operator" => "__init__")
        .increment(0);
    metrics::counter!(RETRACTION_DEPTH_EXCEEDED_TOTAL).increment(0);
}

// ---- update helpers called from hot path ----

/// Record one event processed by `shard_index` with the given outcome.
#[inline]
pub fn record_shard_event(shard_index: usize, outcome: Outcome) {
    let s = shard_index.to_string();
    metrics::counter!(SHARD_EVENTS_TOTAL, "shard" => s, "outcome" => outcome.as_str())
        .increment(1);
}

/// Record an inbox-full drop: increments both the per-shard counter and
/// the global beava_events_dropped_total{reason="inbox_full"}.
#[inline]
pub fn record_inbox_full(shard_index: usize) {
    let s = shard_index.to_string();
    metrics::counter!(SHARD_INBOX_FULL_TOTAL, "shard" => s).increment(1);
    metrics::counter!(EVENTS_DROPPED_TOTAL, "reason" => "inbox_full").increment(1);
}

/// Phase 55-01 SC-5: emit `beava_shard_inbox_high_watermark_total{shard}`
/// when target-inbox `depth` crosses 75 % of `capacity`. Called from
/// `LiveCascadeTargets::dispatch_batch` before each `try_send`. Cheap
/// integer math (no floats); no allocation on the fast path.
#[inline]
pub fn record_inbox_depth(shard_index: usize, depth: usize, capacity: usize) {
    if capacity > 0 && depth.saturating_mul(4) >= capacity.saturating_mul(3) {
        let s = shard_index.to_string();
        metrics::counter!(SHARD_INBOX_HIGH_WATERMARK_TOTAL, "shard" => s).increment(1);
    }
}

/// Phase 55-01: intra-shard cascade counter helper. Emitted by the engine's
/// same-shard fast path (inline `StoreView::upsert_table_row`) so the ratio
/// `cross_shard_total / (cross_shard_total + intra_shard_total)` gives
/// exact cross-shard fraction for perf dashboards.
#[inline]
pub fn record_cascade_intra_shard(shard_index: usize, n: u64) {
    let s = shard_index.to_string();
    metrics::counter!(CASCADE_INTRA_SHARD_TOTAL, "shard" => s).increment(n);
}

/// Record an event dropped at ingest because the shard_key field was missing (D-10).
#[inline]
pub fn record_shard_key_missing() {
    metrics::counter!(EVENTS_DROPPED_TOTAL, "reason" => "shard_key_missing").increment(1);
}

/// Record a shard panic / DOWN transition (D-02).
#[inline]
pub fn record_shard_down(shard_index: usize) {
    let s = shard_index.to_string();
    metrics::counter!(SHARD_DOWN_TOTAL, "shard" => s).increment(1);
}

/// Update gauge-type metrics for a shard (called periodically from shard loop, not per-event).
#[inline]
pub fn update_shard_gauges(
    shard_index: usize,
    reactor_utilization: f64,
    inbox_depth: usize,
    keys_owned: usize,
    watermark_lag_seconds: f64,
) {
    let s = shard_index.to_string();
    metrics::gauge!(SHARD_REACTOR_UTILIZATION, "shard" => s.clone()).set(reactor_utilization);
    metrics::gauge!(SHARD_INBOX_DEPTH, "shard" => s.clone()).set(inbox_depth as f64);
    metrics::gauge!(SHARD_KEYS_OWNED, "shard" => s.clone()).set(keys_owned as f64);
    metrics::gauge!(SHARD_WATERMARK_LAG_SECONDS, "shard" => s).set(watermark_lag_seconds);
}

// ---- Phase 53-05 (W-4 revision): fjall metric helpers ----

/// Record `bytes` added to the write-bytes counter for `shard_index`.
///
/// Call at the point where `postcard::to_stdvec(&EntityState)` is handed to
/// `PartitionHandle::insert` — `bytes` is the value-byte count, i.e. the
/// `postcard` blob length. Key bytes are not counted; they are small and
/// dominated by the value payload.
#[inline]
pub fn record_fjall_write_bytes(shard_index: usize, bytes: u64) {
    let s = shard_index.to_string();
    metrics::counter!(METRIC_FJALL_WRITE_BYTES, "shard" => s).increment(bytes);
}

/// Record `bytes` reclaimed by fjall compaction for `shard_index`.
///
/// Currently unincrementable — fjall 2.11 does not expose per-compaction
/// byte counters via its public API. The helper exists so Plan 06's alert
/// rules have a real metric to target; a follow-up phase will wire the
/// real bytes when a compaction-events API lands in fjall 2.x / 3.x.
#[inline]
pub fn record_fjall_compaction_bytes(shard_index: usize, bytes: u64) {
    let s = shard_index.to_string();
    metrics::counter!(METRIC_FJALL_COMPACTION_BYTES, "shard" => s).increment(bytes);
}

/// Update the fjall fsync-latency gauge for `shard_index` (in ms).
///
/// Called at sites that wrap `Keyspace::persist(PersistMode::SyncData |
/// SyncAll)` — today that is the migrate-to-fjall tool (Plan 04) and the
/// crash-recovery test harness (Plan 05). The shard hot path uses
/// `PersistMode::Buffer` (fjall's default for `insert`) so no sync runs per
/// event; the gauge stays at 0 on shards that have not yet been sync-fenced.
#[inline]
pub fn update_fjall_fsync_latency(shard_index: usize, latency_ms: f64) {
    let s = shard_index.to_string();
    metrics::gauge!(METRIC_FJALL_FSYNC_LATENCY_MS, "shard" => s).set(latency_ms);
}

#[cfg(test)]
mod tests {
    use super::*;

    // We deliberately do NOT call install_prometheus_recorder() in unit tests
    // to avoid global-state conflicts across parallel test runs.
    // The helpers must not panic when no global recorder is installed.

    #[test]
    fn metric_name_constants_are_correct() {
        // Compile-time: verify constants match D-07 naming.
        assert_eq!(SHARD_REACTOR_UTILIZATION, "beava_shard_reactor_utilization");
        assert_eq!(SHARD_INBOX_DEPTH, "beava_shard_inbox_depth");
        assert_eq!(SHARD_EVENTS_TOTAL, "beava_shard_events_total");
        assert_eq!(SHARD_KEYS_OWNED, "beava_shard_keys_owned");
        assert_eq!(SHARD_WATERMARK_LAG_SECONDS, "beava_shard_watermark_lag_seconds");
        assert_eq!(SHARD_INBOX_FULL_TOTAL, "beava_shard_inbox_full_total");
        assert_eq!(SHARD_DOWN_TOTAL, "beava_shard_down_total");
        assert_eq!(EVENTS_DROPPED_TOTAL, "beava_events_dropped_total");
        assert_eq!(CROSS_SHARD_FANOUT_TOTAL, "beava_cross_shard_fanout_total");
    }

    #[test]
    fn outcome_strings_correct() {
        assert_eq!(Outcome::Accepted.as_str(), "accepted");
        assert_eq!(Outcome::Dropped.as_str(), "dropped");
    }

    #[test]
    fn drop_reason_strings_correct() {
        assert_eq!(DropReason::ShardKeyMissing.as_str(), "shard_key_missing");
        assert_eq!(DropReason::InboxFull.as_str(), "inbox_full");
        assert_eq!(DropReason::MalformedRouting.as_str(), "malformed_routing");
    }

    #[test]
    fn helpers_dont_panic_without_recorder() {
        // With no global recorder installed, metrics! macros use a no-op recorder.
        // These calls must not panic.
        record_shard_event(0, Outcome::Accepted);
        record_inbox_full(0);
        record_shard_key_missing();
        record_shard_down(0);
        update_shard_gauges(0, 0.5, 100, 200, 0.01);
    }

    #[test]
    fn register_shard_metrics_no_panic_without_recorder() {
        // register_shard_metrics must not panic even without a global recorder.
        register_shard_metrics(4);
    }

    /// Phase 53-05 Task 2 (W-4 revision): the three unconditional fjall
    /// metric helpers must exist and not panic without a global recorder.
    ///
    /// `beava_fjall_cache_hit_ratio` is deliberately absent — the Plan 01
    /// spike recorded `cache_stats_available: false` (fjall 2.11 exposes
    /// only `Keyspace::cache_capacity()`, not hit/miss counters). Emitting
    /// a hardcoded `1.0` placeholder would make Plan 06's alert vacuous, so
    /// the gauge is omitted entirely. See W-4 comment in metrics.rs.
    #[test]
    fn fjall_metrics_helpers_do_not_panic() {
        record_fjall_write_bytes(0, 1024);
        record_fjall_compaction_bytes(1, 2048);
        update_fjall_fsync_latency(2, 1.23);
    }
}
