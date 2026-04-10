//! Per-command and per-stream latency tracker with bucketed log-spaced histograms.
//!
//! Phase 10.2 (DBUI-07): provides p50/p95/p99 percentiles for PUSH/GET/SET/MSET
//! commands, per-stream PUSH breakdown, and bounded slow-query capture.
//!
//! Design decisions (from 10.2-CONTEXT.md):
//! - Bucketed histogram with 30 log-spaced bins covering 1us-10ms (Decision 1)
//! - Two-buffer swap every 2.5 min for rolling 5-min window (Decision 4)
//! - Min-heap of top 20 slowest per command (Decision 4)
//! - PUSH attributed per-stream; GET/SET/MSET command-level only (Decision 3)

use ahash::AHashMap;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::time::{Duration, Instant, SystemTime};

/// Number of histogram bins (log-spaced from 1us to 10ms).
pub const NUM_BINS: usize = 30;

/// Precomputed bin edges in microseconds.
/// Edge[i] = 10^(i * 4.0 / 30.0) for i in 0..=30.
/// Bin 0: [0, edge[1]), Bin 29: [edge[29], +inf).
pub const BIN_EDGES: [f64; NUM_BINS + 1] = [
    1.0,                // 10^(0/30 * 4) = 10^0.000
    1.3335214321633242,  // 10^(1/30 * 4) = 10^0.133
    1.7782794100389228,  // 10^(2/30 * 4) = 10^0.267
    2.371373705661655,   // 10^(3/30 * 4) = 10^0.400
    3.1622776601683795,  // 10^(4/30 * 4) = 10^0.533
    4.216965034285822,   // 10^(5/30 * 4) = 10^0.667
    5.623413251903491,   // 10^(6/30 * 4) = 10^0.800
    7.498942093324559,   // 10^(7/30 * 4) = 10^0.933
    10.0,               // 10^(8/30 * 4) = 10^1.067
    13.335214321633243,  // 10^(9/30 * 4) = 10^1.200
    17.78279410038923,   // 10^(10/30 * 4) = 10^1.333
    23.71373705661655,   // 10^(11/30 * 4) = 10^1.467
    31.622776601683796,  // 10^(12/30 * 4) = 10^1.600
    42.16965034285822,   // 10^(13/30 * 4) = 10^1.733
    56.23413251903491,   // 10^(14/30 * 4) = 10^1.867
    74.98942093324559,   // 10^(15/30 * 4) = 10^2.000
    100.0,              // 10^(16/30 * 4) = 10^2.133
    133.35214321633242,  // 10^(17/30 * 4) = 10^2.267
    177.82794100389228,  // 10^(18/30 * 4) = 10^2.400
    237.13737056616552,  // 10^(19/30 * 4) = 10^2.533
    316.22776601683796,  // 10^(20/30 * 4) = 10^2.667
    421.6965034285822,   // 10^(21/30 * 4) = 10^2.800
    562.3413251903491,   // 10^(22/30 * 4) = 10^2.933
    749.8942093324559,   // 10^(23/30 * 4) = 10^3.067
    1000.0,             // 10^(24/30 * 4) = 10^3.200
    1333.5214321633243,  // 10^(25/30 * 4) = 10^3.333
    1778.2794100389228,  // 10^(26/30 * 4) = 10^3.467
    2371.3737056616554,  // 10^(27/30 * 4) = 10^3.600
    3162.2776601683795,  // 10^(28/30 * 4) = 10^3.733
    4216.965034285822,   // 10^(29/30 * 4) = 10^3.867
    10000.0,            // 10^(30/30 * 4) = 10^4.000
];

const SWAP_INTERVAL: Duration = Duration::from_secs(150); // 2.5 minutes
const SLOW_QUERY_CAPACITY: usize = 20;

/// TCP command type for latency attribution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum CommandKind {
    Push = 0,
    Get = 1,
    Set = 2,
    Mset = 3,
}

impl fmt::Display for CommandKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandKind::Push => write!(f, "PUSH"),
            CommandKind::Get => write!(f, "GET"),
            CommandKind::Set => write!(f, "SET"),
            CommandKind::Mset => write!(f, "MSET"),
        }
    }
}

/// Fixed-size bucketed histogram with log-spaced bins.
#[derive(Clone)]
pub struct Histogram {
    pub counts: [u64; NUM_BINS],
    pub total: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            counts: [0u64; NUM_BINS],
            total: 0,
        }
    }
}

impl Histogram {
    /// Record a latency sample in microseconds. O(log NUM_BINS) via binary search.
    #[inline]
    pub fn record(&mut self, micros: f64) {
        // Clamp negative/NaN to bin 0
        let micros = if micros.is_nan() || micros < 0.0 {
            0.0
        } else {
            micros
        };
        // Binary search for the bin: find the last edge <= micros
        let bin = match BIN_EDGES.binary_search_by(|edge| {
            edge.partial_cmp(&micros).unwrap_or(std::cmp::Ordering::Less)
        }) {
            Ok(i) => i.min(NUM_BINS - 1),
            Err(i) => i.saturating_sub(1).min(NUM_BINS - 1),
        };
        self.counts[bin] += 1;
        self.total += 1;
    }

    /// Compute the p-th percentile (0-100) via linear interpolation within the target bin.
    pub fn percentile(&self, p: f64) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        let target = (p / 100.0 * self.total as f64).ceil() as u64;
        let mut cumulative = 0u64;
        for i in 0..NUM_BINS {
            cumulative += self.counts[i];
            if cumulative >= target {
                let lower = BIN_EDGES[i];
                let upper = BIN_EDGES[i + 1];
                let bin_count = self.counts[i];
                if bin_count == 0 {
                    return lower;
                }
                let overshoot = cumulative - target;
                let fraction = 1.0 - (overshoot as f64 / bin_count as f64);
                return lower + fraction * (upper - lower);
            }
        }
        BIN_EDGES[NUM_BINS]
    }

    /// Merge two histograms element-wise.
    pub fn merge(&self, other: &Histogram) -> Histogram {
        let mut merged = Histogram::default();
        for i in 0..NUM_BINS {
            merged.counts[i] = self.counts[i] + other.counts[i];
        }
        merged.total = self.total + other.total;
        merged
    }
}

/// Two-buffer rolling histogram with swap every 2.5 minutes.
pub struct RollingHistogram {
    current: Histogram,
    previous: Histogram,
    current_start: Instant,
    swap_interval: Duration,
}

impl RollingHistogram {
    pub fn new(now: Instant) -> Self {
        Self {
            current: Histogram::default(),
            previous: Histogram::default(),
            current_start: now,
            swap_interval: SWAP_INTERVAL,
        }
    }

    /// Record a sample, swapping buffers if the interval has elapsed.
    pub fn record(&mut self, micros: f64, now: Instant) {
        if now.duration_since(self.current_start) >= self.swap_interval {
            std::mem::swap(&mut self.current, &mut self.previous);
            self.current = Histogram::default();
            self.current_start = now;
        }
        self.current.record(micros);
    }

    /// Snapshot: merge current + previous. Returns empty if idle > 2x swap_interval.
    pub fn snapshot(&self, now: Instant) -> Histogram {
        let elapsed = now.duration_since(self.current_start);
        if elapsed >= self.swap_interval * 2 {
            // Idle decay: both buffers are stale
            return Histogram::default();
        }
        self.current.merge(&self.previous)
    }
}

/// A single slow-query record.
#[derive(Clone, Debug)]
pub struct SlowQueryRecord {
    pub timestamp: SystemTime,
    pub command: CommandKind,
    pub stream: Option<String>,
    pub latency_us: f64,
    pub key_preview: String,
}

impl PartialEq for SlowQueryRecord {
    fn eq(&self, other: &Self) -> bool {
        self.latency_us == other.latency_us
    }
}

impl Eq for SlowQueryRecord {}

impl PartialOrd for SlowQueryRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SlowQueryRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.latency_us
            .partial_cmp(&other.latency_us)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Bounded min-heap of the N slowest queries.
pub struct SlowQueryHeap {
    heap: BinaryHeap<Reverse<SlowQueryRecord>>,
    capacity: usize,
}

impl SlowQueryHeap {
    pub fn new(capacity: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(capacity + 1),
            capacity,
        }
    }

    /// Insert a record if it qualifies (slower than current minimum when full).
    pub fn insert(&mut self, record: SlowQueryRecord) {
        if self.heap.len() < self.capacity {
            self.heap.push(Reverse(record));
        } else if let Some(min) = self.heap.peek() {
            if record.latency_us > min.0.latency_us {
                self.heap.pop();
                self.heap.push(Reverse(record));
            }
        }
    }

    /// Check if a sample with the given latency would qualify for insertion.
    /// Used to avoid heap allocation when the sample is too fast.
    #[inline]
    pub fn would_accept(&self, latency_us: f64) -> bool {
        if self.heap.len() < self.capacity {
            return true;
        }
        match self.heap.peek() {
            Some(min) => latency_us > min.0.latency_us,
            None => true,
        }
    }

    /// Return all records sorted descending by latency.
    pub fn sorted_desc(&self) -> Vec<&SlowQueryRecord> {
        let mut entries: Vec<&SlowQueryRecord> = self.heap.iter().map(|r| &r.0).collect();
        entries.sort_by(|a, b| b.latency_us.partial_cmp(&a.latency_us).unwrap_or(std::cmp::Ordering::Equal));
        entries
    }
}

/// Top-level latency tracker. Lives on AppState alongside ThroughputTracker.
pub struct LatencyTracker {
    /// Per-command global histograms: [PUSH, GET, SET, MSET]
    command_histograms: [RollingHistogram; 4],
    /// Per-stream PUSH histograms (only PUSH is stream-attributed)
    stream_histograms: AHashMap<String, RollingHistogram>,
    /// Slow-query heaps: one per command type
    slow_queries: [SlowQueryHeap; 4],
    /// Creation instant for RollingHistogram time base
    _created_at: Instant,
}

impl LatencyTracker {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            command_histograms: [
                RollingHistogram::new(now),
                RollingHistogram::new(now),
                RollingHistogram::new(now),
                RollingHistogram::new(now),
            ],
            stream_histograms: AHashMap::new(),
            slow_queries: [
                SlowQueryHeap::new(SLOW_QUERY_CAPACITY),
                SlowQueryHeap::new(SLOW_QUERY_CAPACITY),
                SlowQueryHeap::new(SLOW_QUERY_CAPACITY),
                SlowQueryHeap::new(SLOW_QUERY_CAPACITY),
            ],
            _created_at: now,
        }
    }

    /// Record a PUSH latency: updates both the global PUSH histogram and the per-stream histogram.
    pub fn record_push(&mut self, stream_name: &str, micros: f64, now: Instant) {
        self.command_histograms[CommandKind::Push as usize].record(micros, now);
        self.stream_histograms
            .entry(stream_name.to_string())
            .or_insert_with(|| RollingHistogram::new(now))
            .record(micros, now);
    }

    /// Record a non-PUSH command latency (GET/SET/MSET): updates only the global command histogram.
    pub fn record_command(&mut self, kind: CommandKind, micros: f64, now: Instant) {
        self.command_histograms[kind as usize].record(micros, now);
    }

    /// Conditionally record a slow query. Checks heap minimum FIRST to avoid
    /// heap allocation on the common path (Pitfall 5).
    pub fn maybe_record_slow(
        &mut self,
        kind: CommandKind,
        stream: Option<&str>,
        micros: f64,
        key_preview: String,
    ) {
        let heap = &mut self.slow_queries[kind as usize];
        if heap.would_accept(micros) {
            heap.insert(SlowQueryRecord {
                timestamp: SystemTime::now(),
                command: kind,
                stream: stream.map(|s| s.to_string()),
                latency_us: micros,
                key_preview,
            });
        }
    }

    /// Build the JSON response for `/debug/latency`.
    pub fn to_json(&self, now: Instant) -> serde_json::Value {
        let commands = [CommandKind::Push, CommandKind::Get, CommandKind::Set, CommandKind::Mset];

        // Per-command histograms
        let per_command: Vec<serde_json::Value> = commands
            .iter()
            .map(|&kind| {
                let hist = self.command_histograms[kind as usize].snapshot(now);
                serde_json::json!({
                    "command": kind.to_string(),
                    "count": hist.total,
                    "p50_us": hist.percentile(50.0),
                    "p95_us": hist.percentile(95.0),
                    "p99_us": hist.percentile(99.0),
                    "histogram": {
                        "bin_edges_us": BIN_EDGES.to_vec(),
                        "counts": hist.counts.to_vec(),
                    }
                })
            })
            .collect();

        // Per-stream PUSH histograms
        let mut per_stream: Vec<serde_json::Value> = self
            .stream_histograms
            .iter()
            .map(|(name, rh)| {
                let hist = rh.snapshot(now);
                serde_json::json!({
                    "stream": name,
                    "count": hist.total,
                    "p50_us": hist.percentile(50.0),
                    "p95_us": hist.percentile(95.0),
                    "p99_us": hist.percentile(99.0),
                })
            })
            .collect();
        per_stream.sort_by(|a, b| a["stream"].as_str().cmp(&b["stream"].as_str()));

        // Slow queries: merge all 4 heaps, sort by latency descending
        let mut slow_queries: Vec<serde_json::Value> = Vec::new();
        for kind in &commands {
            for record in self.slow_queries[*kind as usize].sorted_desc() {
                let ts_ms = record
                    .timestamp
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                slow_queries.push(serde_json::json!({
                    "timestamp_ms": ts_ms,
                    "command": record.command.to_string(),
                    "stream": record.stream,
                    "latency_us": record.latency_us,
                    "key_preview": record.key_preview,
                }));
            }
        }
        slow_queries.sort_by(|a, b| {
            b["latency_us"]
                .as_f64()
                .partial_cmp(&a["latency_us"].as_f64())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        serde_json::json!({
            "per_command": per_command,
            "per_stream": per_stream,
            "slow_queries": slow_queries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn histogram_record_places_samples_in_correct_bins() {
        let mut h = Histogram::default();
        // 0us -> bin 0
        h.record(0.0);
        assert_eq!(h.counts[0], 1);
        // 50us -> should land in a mid bin (BIN_EDGES[13]=42.17, BIN_EDGES[14]=56.23)
        h.record(50.0);
        assert_eq!(h.counts[13], 1, "50us should be in bin 13 (42.17-56.23)");
        // 10000us -> last bin (BIN_EDGES[29]=4216.97, overflow)
        h.record(10000.0);
        assert_eq!(h.counts[NUM_BINS - 1], 1, "10000us should be in last bin");
        // 100000us -> also last bin (overflow)
        h.record(100000.0);
        assert_eq!(h.counts[NUM_BINS - 1], 2, "100000us overflow to last bin");
        assert_eq!(h.total, 4);
    }

    #[test]
    fn histogram_record_handles_nan_and_negative() {
        let mut h = Histogram::default();
        h.record(f64::NAN);
        assert_eq!(h.counts[0], 1, "NaN should clamp to bin 0");
        h.record(-5.0);
        assert_eq!(h.counts[0], 2, "negative should clamp to bin 0");
        assert_eq!(h.total, 2);
    }

    #[test]
    fn histogram_percentile_empty() {
        let h = Histogram::default();
        assert_eq!(h.percentile(50.0), 0.0);
        assert_eq!(h.percentile(99.0), 0.0);
    }

    #[test]
    fn histogram_percentile_single_sample() {
        let mut h = Histogram::default();
        h.record(50.0); // bin 13 (42.17 - 56.23)
        let p50 = h.percentile(50.0);
        // With 1 sample, p50 should be within the bin containing 50us
        assert!(p50 >= 42.0 && p50 <= 57.0, "p50={} should be near 50us", p50);
    }

    #[test]
    fn histogram_percentile_many_samples() {
        let mut h = Histogram::default();
        // Record 1000 samples all at ~50us
        for _ in 0..1000 {
            h.record(50.0);
        }
        let p50 = h.percentile(50.0);
        let p99 = h.percentile(99.0);
        // All samples in same bin, so p50 and p99 should be within that bin
        assert!(
            p50 >= 42.0 && p50 <= 57.0,
            "p50={} should be in bin 13",
            p50
        );
        assert!(
            p99 >= 42.0 && p99 <= 57.0,
            "p99={} should be in bin 13",
            p99
        );
    }

    #[test]
    fn histogram_percentile_spread_distribution() {
        let mut h = Histogram::default();
        // Put 100 samples each in bins covering 10us, 100us, 1000us
        for _ in 0..100 {
            h.record(10.0);
        }
        for _ in 0..100 {
            h.record(100.0);
        }
        for _ in 0..100 {
            h.record(1000.0);
        }
        let p50 = h.percentile(50.0);
        // p50 of 300 samples: 150th sample. First 100 are ~10us, next 100 are ~100us.
        // 150th is in the 100us group.
        assert!(
            p50 >= 50.0 && p50 <= 200.0,
            "p50={} should be near 100us region",
            p50
        );
        let p99 = h.percentile(99.0);
        // 297th sample is in the 1000us group
        assert!(
            p99 >= 500.0 && p99 <= 2000.0,
            "p99={} should be near 1000us region",
            p99
        );
    }

    #[test]
    fn histogram_merge() {
        let mut a = Histogram::default();
        let mut b = Histogram::default();
        a.record(10.0);
        a.record(100.0);
        b.record(1000.0);
        let merged = a.merge(&b);
        assert_eq!(merged.total, 3);
    }

    #[test]
    fn rolling_histogram_swaps_buffers() {
        let start = Instant::now();
        let mut rh = RollingHistogram::new(start);
        // Record in current buffer
        rh.record(50.0, start);
        assert_eq!(rh.current.total, 1);
        assert_eq!(rh.previous.total, 0);
        // Advance past swap interval (150s)
        let after_swap = start + Duration::from_secs(151);
        rh.record(100.0, after_swap);
        // Previous should now have the old current (1 sample)
        assert_eq!(rh.previous.total, 1);
        // Current should have the new sample
        assert_eq!(rh.current.total, 1);
    }

    #[test]
    fn rolling_histogram_snapshot_merges() {
        let start = Instant::now();
        let mut rh = RollingHistogram::new(start);
        rh.record(50.0, start);
        // Swap
        let t2 = start + Duration::from_secs(151);
        rh.record(100.0, t2);
        // Snapshot should merge: 1 (previous) + 1 (current) = 2
        let snap = rh.snapshot(t2);
        assert_eq!(snap.total, 2);
    }

    #[test]
    fn rolling_histogram_idle_decay() {
        let start = Instant::now();
        let mut rh = RollingHistogram::new(start);
        rh.record(50.0, start);
        // Advance past 2x swap interval (300s) without recording
        let idle = start + Duration::from_secs(301);
        let snap = rh.snapshot(idle);
        assert_eq!(snap.total, 0, "should return empty histogram when idle > 2x swap");
    }

    #[test]
    fn slow_query_heap_bounded() {
        let mut heap = SlowQueryHeap::new(20);
        for i in 0..30 {
            heap.insert(SlowQueryRecord {
                timestamp: SystemTime::now(),
                command: CommandKind::Push,
                stream: None,
                latency_us: i as f64,
                key_preview: String::new(),
            });
        }
        assert_eq!(heap.heap.len(), 20, "heap should be bounded at 20");
    }

    #[test]
    fn slow_query_heap_keeps_slowest() {
        let mut heap = SlowQueryHeap::new(3);
        for i in 0..10 {
            heap.insert(SlowQueryRecord {
                timestamp: SystemTime::now(),
                command: CommandKind::Push,
                stream: None,
                latency_us: i as f64 * 10.0,
                key_preview: String::new(),
            });
        }
        let sorted = heap.sorted_desc();
        assert_eq!(sorted.len(), 3);
        // Should keep the 3 slowest: 90, 80, 70
        assert_eq!(sorted[0].latency_us, 90.0);
        assert_eq!(sorted[1].latency_us, 80.0);
        assert_eq!(sorted[2].latency_us, 70.0);
    }

    #[test]
    fn slow_query_heap_rejects_fast_samples() {
        let mut heap = SlowQueryHeap::new(2);
        heap.insert(SlowQueryRecord {
            timestamp: SystemTime::now(),
            command: CommandKind::Push,
            stream: None,
            latency_us: 100.0,
            key_preview: String::new(),
        });
        heap.insert(SlowQueryRecord {
            timestamp: SystemTime::now(),
            command: CommandKind::Push,
            stream: None,
            latency_us: 200.0,
            key_preview: String::new(),
        });
        // This should be rejected (50 < min of 100)
        assert!(!heap.would_accept(50.0));
        // This should be accepted (300 > min of 100)
        assert!(heap.would_accept(300.0));
    }

    #[test]
    fn latency_tracker_new() {
        let tracker = LatencyTracker::new();
        let now = Instant::now();
        let json = tracker.to_json(now);
        let per_cmd = json["per_command"].as_array().unwrap();
        assert_eq!(per_cmd.len(), 4);
        let per_stream = json["per_stream"].as_array().unwrap();
        assert_eq!(per_stream.len(), 0);
        let slow = json["slow_queries"].as_array().unwrap();
        assert_eq!(slow.len(), 0);
    }

    #[test]
    fn latency_tracker_record_push_updates_both() {
        let mut tracker = LatencyTracker::new();
        let now = Instant::now();
        tracker.record_push("Transactions", 50.0, now);
        tracker.record_push("Transactions", 100.0, now);
        tracker.record_push("Logins", 30.0, now);

        let json = tracker.to_json(now);

        // Global PUSH count should be 3
        let push_cmd = &json["per_command"][0];
        assert_eq!(push_cmd["command"], "PUSH");
        assert_eq!(push_cmd["count"], 3);

        // Per-stream: Transactions=2, Logins=1
        let per_stream = json["per_stream"].as_array().unwrap();
        assert_eq!(per_stream.len(), 2);
        let logins = per_stream.iter().find(|s| s["stream"] == "Logins").unwrap();
        assert_eq!(logins["count"], 1);
        let txns = per_stream.iter().find(|s| s["stream"] == "Transactions").unwrap();
        assert_eq!(txns["count"], 2);
    }

    #[test]
    fn latency_tracker_record_command_no_per_stream() {
        let mut tracker = LatencyTracker::new();
        let now = Instant::now();
        tracker.record_command(CommandKind::Get, 20.0, now);
        tracker.record_command(CommandKind::Get, 30.0, now);

        let json = tracker.to_json(now);
        let get_cmd = &json["per_command"][1];
        assert_eq!(get_cmd["command"], "GET");
        assert_eq!(get_cmd["count"], 2);

        // No per-stream entries
        assert_eq!(json["per_stream"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn latency_tracker_to_json_structure() {
        let mut tracker = LatencyTracker::new();
        let now = Instant::now();
        tracker.record_push("TestStream", 50.0, now);
        tracker.maybe_record_slow(CommandKind::Push, Some("TestStream"), 50.0, "user_123".into());

        let json = tracker.to_json(now);

        // Verify structure
        assert!(json["per_command"].is_array());
        assert!(json["per_stream"].is_array());
        assert!(json["slow_queries"].is_array());

        // Verify PUSH command has histogram data
        let push = &json["per_command"][0];
        assert!(push["histogram"]["bin_edges_us"].is_array());
        assert!(push["histogram"]["counts"].is_array());
        assert!(push["p50_us"].as_f64().is_some());
        assert!(push["p95_us"].as_f64().is_some());
        assert!(push["p99_us"].as_f64().is_some());

        // Verify slow query
        let slow = &json["slow_queries"][0];
        assert_eq!(slow["command"], "PUSH");
        assert_eq!(slow["stream"], "TestStream");
        assert_eq!(slow["key_preview"], "user_123");
        assert!(slow["latency_us"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn maybe_record_slow_avoids_alloc_when_not_needed() {
        let mut tracker = LatencyTracker::new();
        // Fill PUSH heap with 20 records at 1000us
        for i in 0..20 {
            tracker.maybe_record_slow(
                CommandKind::Push,
                Some("S"),
                1000.0 + i as f64,
                format!("key_{}", i),
            );
        }
        // This should NOT qualify (5us < min of 1000us)
        assert!(!tracker.slow_queries[CommandKind::Push as usize].would_accept(5.0));
        // This SHOULD qualify (2000us > min of 1000us)
        assert!(tracker.slow_queries[CommandKind::Push as usize].would_accept(2000.0));
    }
}
