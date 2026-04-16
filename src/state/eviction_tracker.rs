//! Phase 25-02: Per-Table bloom-filter tracker for TTL-evicted keys.
//!
//! When a key is evicted by TTL we record it in a per-Table bloom filter; if
//! the same key reappears within the rolling 7-day window we bump the
//! `beava_ttl_eviction_then_reinit_total{table}` counter. That signal drives
//! the recommendation engine in `pipeline::recommend_config`.
//!
//! Design: hand-rolled bloom filter (no new dependency). ~1 MiB per Table at
//! 8M bits / ~100K expected insertions / 1% FP target. The filter uses two
//! ahash hashers (different seeds) and derives k additional positions via the
//! classic `h1 + i*h2` double-hashing trick (Kirsch-Mitzenmacher).
//!
//! The 7-day rolling window is approximated with a "generational" two-slot
//! bloom (today / yesterday). On `rotate_generation` the older slot is
//! dropped, the newer slot becomes "yesterday", and a fresh "today" is
//! allocated. Rotation cadence is 3.5 days, so the window spans 7 days in the
//! worst case.
//!
//! Memory bound: 256-Table cap × ~2 MiB (two slots) ≈ 512 MiB worst-case,
//! documented in the STRIDE register as T-25-02-02 with mitigation: the
//! `beava_bloom_memory_bytes` metric surfaces actual usage.

use ahash::RandomState;
use dashmap::DashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

/// Rotation cadence: two-slot generational bloom gives an effective rolling
/// window of `2 * ROTATE_INTERVAL` = 7 days when set to 3.5 days.
pub const ROTATE_INTERVAL: Duration = Duration::from_secs(3 * 86400 + 12 * 3600);

/// Default bit-size of each bloom slot. 8 Mbits ≈ 1 MiB per slot.
/// With k=4 hashes and 100K insertions → FP rate ≈ 0.5%.
pub const BLOOM_BITS_DEFAULT: usize = 8 * 1024 * 1024;
/// Number of hash functions per query. Four is the sweet spot for our target
/// load/FP combo (derived analytically from the bits/insertions ratio).
pub const BLOOM_HASHES_DEFAULT: usize = 4;

/// A single-slot bloom filter with a bit array + two independent hashers.
/// Insertion and lookup use the Kirsch-Mitzenmacher double-hashing scheme to
/// derive `k` positions from two base hashes.
#[derive(Debug)]
pub struct Bloom {
    bits: Vec<u64>, // u64 word array; bit i → word i/64, bit (i%64)
    num_bits: usize,
    k: usize,
    h1: RandomState,
    h2: RandomState,
    inserted: AtomicU64,
}

impl Bloom {
    pub fn new(num_bits: usize, k: usize) -> Self {
        // Round up to a multiple of 64 so the word array covers every bit.
        let num_words = num_bits.div_ceil(64);
        let actual_bits = num_words * 64;
        // ahash seeds: deterministic per process, different seeds so h1 ≠ h2.
        // The fixed seeds here are arbitrary (just need two distinct values).
        let h1 = RandomState::with_seeds(0xa5a5_a5a5_a5a5_a5a5, 0x1234_5678_9abc_def0, 0, 0);
        let h2 = RandomState::with_seeds(0xdead_beef_cafe_babe, 0x0fed_cba9_8765_4321, 0, 0);
        Self {
            bits: vec![0u64; num_words],
            num_bits: actual_bits,
            k,
            h1,
            h2,
            inserted: AtomicU64::new(0),
        }
    }

    fn hash_pair(&self, key: &str) -> (u64, u64) {
        let mut s1 = self.h1.build_hasher();
        s1.write(key.as_bytes());
        let mut s2 = self.h2.build_hasher();
        s2.write(key.as_bytes());
        (s1.finish(), s2.finish())
    }

    pub fn insert(&mut self, key: &str) {
        let (a, b) = self.hash_pair(key);
        for i in 0..self.k {
            let idx = (a.wrapping_add((i as u64).wrapping_mul(b))) as usize % self.num_bits;
            let word = idx / 64;
            let bit = idx % 64;
            self.bits[word] |= 1u64 << bit;
        }
        self.inserted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn contains(&self, key: &str) -> bool {
        let (a, b) = self.hash_pair(key);
        for i in 0..self.k {
            let idx = (a.wrapping_add((i as u64).wrapping_mul(b))) as usize % self.num_bits;
            let word = idx / 64;
            let bit = idx % 64;
            if self.bits[word] & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }

    pub fn memory_bytes(&self) -> usize {
        self.bits.len() * std::mem::size_of::<u64>()
    }

    pub fn inserted(&self) -> u64 {
        self.inserted.load(Ordering::Relaxed)
    }
}

/// A generational (two-slot) bloom filter. `today` absorbs new inserts;
/// `yesterday` is the previous slot rotated in at the last `rotate()` call.
/// Lookups OR the two slots — a key is "seen" if either slot contains it.
#[derive(Debug)]
pub struct GenerationalBloom {
    today: Bloom,
    yesterday: Bloom,
    rotated_at: SystemTime,
    num_bits: usize,
    k: usize,
}

impl GenerationalBloom {
    pub fn new(num_bits: usize, k: usize, now: SystemTime) -> Self {
        Self {
            today: Bloom::new(num_bits, k),
            yesterday: Bloom::new(num_bits, k),
            rotated_at: now,
            num_bits,
            k,
        }
    }

    pub fn insert(&mut self, key: &str) {
        self.today.insert(key);
    }

    pub fn contains(&self, key: &str) -> bool {
        self.today.contains(key) || self.yesterday.contains(key)
    }

    /// Rotate generations if `now - rotated_at >= ROTATE_INTERVAL`.
    pub fn maybe_rotate(&mut self, now: SystemTime) -> bool {
        let elapsed = now
            .duration_since(self.rotated_at)
            .unwrap_or(Duration::ZERO);
        if elapsed >= ROTATE_INTERVAL {
            self.force_rotate(now);
            true
        } else {
            false
        }
    }

    pub fn force_rotate(&mut self, now: SystemTime) {
        // yesterday <- today; today <- fresh
        let fresh = Bloom::new(self.num_bits, self.k);
        self.yesterday = std::mem::replace(&mut self.today, fresh);
        self.rotated_at = now;
    }

    pub fn memory_bytes(&self) -> usize {
        self.today.memory_bytes() + self.yesterday.memory_bytes()
    }
}

/// Per-Table eviction tracker. Tracks (a) evicted keys in a generational
/// bloom filter and (b) a monotone counter of confirmed eviction→reinit
/// events. The counter drives the `beava_ttl_eviction_then_reinit_total`
/// metric and the recommendation engine.
#[derive(Debug, Default)]
pub struct EvictionTracker {
    per_table: DashMap<String, parking_lot::Mutex<GenerationalBloom>>,
    /// Per-Table eviction-then-reinit counter. Also surfaced on `/metrics`.
    pub reinits: DashMap<String, AtomicU64>,
    /// Per-Table eviction counter (monotonic). Also surfaced on `/metrics`.
    pub evictions: DashMap<String, AtomicU64>,
    /// Window-size / hash-count parameters. Captured so new per-Table blooms
    /// use the same shape as everyone else.
    num_bits: usize,
    k: usize,
}

impl EvictionTracker {
    pub fn new() -> Self {
        Self {
            per_table: DashMap::new(),
            reinits: DashMap::new(),
            evictions: DashMap::new(),
            num_bits: BLOOM_BITS_DEFAULT,
            k: BLOOM_HASHES_DEFAULT,
        }
    }

    pub fn with_capacity(num_bits: usize, k: usize) -> Self {
        Self {
            per_table: DashMap::new(),
            reinits: DashMap::new(),
            evictions: DashMap::new(),
            num_bits,
            k,
        }
    }

    /// Record that `key` was evicted from `table`. Bumps the eviction counter
    /// and inserts the key into the table's bloom.
    pub fn record_eviction(&self, table: &str, key: &str) {
        // Bump eviction counter
        self.evictions
            .entry(table.to_string())
            .or_default()
            .fetch_add(1, Ordering::Relaxed);

        // Insert into bloom (lazy-create per table)
        let now = SystemTime::now();
        let entry = self
            .per_table
            .entry(table.to_string())
            .or_insert_with(|| parking_lot::Mutex::new(GenerationalBloom::new(self.num_bits, self.k, now)));
        let mut slot = entry.value().lock();
        slot.insert(key);
    }

    /// Called on entity (re)creation. Returns `true` iff the bloom says we
    /// recently evicted this key — in which case the caller should bump
    /// `beava_ttl_eviction_then_reinit_total{table}`.
    ///
    /// Returns `false` (and does nothing) if no bloom exists for the table
    /// yet (i.e. no eviction was ever recorded).
    pub fn check_reinit(&self, table: &str, key: &str) -> bool {
        let entry = match self.per_table.get(table) {
            Some(e) => e,
            None => return false,
        };
        let hit = entry.value().lock().contains(key);
        if hit {
            self.reinits
                .entry(table.to_string())
                .or_default()
                .fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    /// Call from the eviction scheduler tick. Rotates every per-Table bloom
    /// whose `rotated_at` is older than `ROTATE_INTERVAL`.
    pub fn rotate_generation(&self, now: SystemTime) {
        for entry in self.per_table.iter() {
            entry.value().lock().maybe_rotate(now);
        }
    }

    /// Total memory currently held by per-Table blooms (bytes). Exposed via
    /// `beava_bloom_memory_bytes`.
    pub fn memory_bytes(&self) -> usize {
        let mut total = 0;
        for entry in self.per_table.iter() {
            total += entry.value().lock().memory_bytes();
        }
        total
    }

    /// Snapshot of the eviction counter for all Tables (for /metrics).
    pub fn evictions_snapshot(&self) -> Vec<(String, u64)> {
        self.evictions
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect()
    }

    /// Snapshot of the reinit counter for all Tables (for /metrics).
    pub fn reinits_snapshot(&self) -> Vec<(String, u64)> {
        self.reinits
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect()
    }

    /// Get the current eviction count for a table.
    pub fn eviction_count(&self, table: &str) -> u64 {
        self.evictions
            .get(table)
            .map(|e| e.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get the current eviction-then-reinit count for a table.
    pub fn reinit_count(&self, table: &str) -> u64 {
        self.reinits
            .get(table)
            .map(|e| e.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Number of tables currently tracked. Cap at 256 (T-25-02-02).
    pub fn table_count(&self) -> usize {
        self.per_table.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(secs: u64) -> SystemTime {
        std::time::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn bloom_basic_insert_contains() {
        let mut b = Bloom::new(1 << 16, 4);
        b.insert("foo");
        b.insert("bar");
        assert!(b.contains("foo"));
        assert!(b.contains("bar"));
        assert!(!b.contains("baz_not_inserted_12345"));
    }

    #[test]
    fn bloom_false_positive_under_1pct() {
        let mut b = Bloom::new(BLOOM_BITS_DEFAULT, BLOOM_HASHES_DEFAULT);
        for i in 0..10_000 {
            b.insert(&format!("inserted_key_{}", i));
        }
        let mut fps = 0;
        let mut tested = 0;
        for i in 0..10_000 {
            let k = format!("query_key_{}", i);
            tested += 1;
            if b.contains(&k) {
                fps += 1;
            }
        }
        let rate = fps as f64 / tested as f64;
        assert!(
            rate <= 0.01,
            "FP rate {:.4} exceeded 1% (fps={}, tested={})",
            rate,
            fps,
            tested
        );
    }

    #[test]
    fn generational_rotation_drops_oldest() {
        let mut g = GenerationalBloom::new(1 << 16, 4, ts(0));
        g.insert("old");
        // First rotation: "old" moves to yesterday slot.
        g.force_rotate(ts(1));
        assert!(g.contains("old"), "one rotation should keep the key");
        // Second rotation: "old" is dropped entirely.
        g.force_rotate(ts(2));
        assert!(!g.contains("old"), "two rotations should drop the key");
    }

    #[test]
    fn tracker_records_reinit_on_hit() {
        let t = EvictionTracker::new();
        t.record_eviction("Users", "u1");
        assert!(t.check_reinit("Users", "u1"));
        assert_eq!(t.reinit_count("Users"), 1);
    }

    #[test]
    fn tracker_ignores_reinit_on_miss() {
        let t = EvictionTracker::new();
        t.record_eviction("Users", "u1");
        assert!(!t.check_reinit("Users", "never_evicted_42"));
        assert_eq!(t.reinit_count("Users"), 0);
    }

    #[test]
    fn tracker_no_bloom_means_no_reinit() {
        let t = EvictionTracker::new();
        // No record_eviction ever called for "Orphans"
        assert!(!t.check_reinit("Orphans", "u1"));
        assert_eq!(t.reinit_count("Orphans"), 0);
    }

    #[test]
    fn tracker_rotation_drops_old_keys() {
        let t = EvictionTracker::new();
        t.record_eviction("Users", "u1");
        assert!(t.check_reinit("Users", "u1"));

        // Simulate two rotations: after two full rotation cycles, "u1" should
        // be gone. Force via the internal API.
        let now = SystemTime::now();
        let later = now + ROTATE_INTERVAL * 2 + Duration::from_secs(60);
        // Need two rotations to fully drop the key (moves today → yesterday → gone)
        {
            let e = t.per_table.get("Users").unwrap();
            e.value().lock().force_rotate(later);
            e.value().lock().force_rotate(later + Duration::from_secs(60));
        }
        // Reset reinit counter before re-check so we observe ONLY the post-rotation result
        t.reinits.get("Users").unwrap().store(0, Ordering::Relaxed);
        assert!(!t.check_reinit("Users", "u1"));
        assert_eq!(t.reinit_count("Users"), 0);
    }

    #[test]
    fn eviction_counter_monotone() {
        let t = EvictionTracker::new();
        t.record_eviction("Users", "u1");
        t.record_eviction("Users", "u2");
        t.record_eviction("Orders", "o1");
        assert_eq!(t.eviction_count("Users"), 2);
        assert_eq!(t.eviction_count("Orders"), 1);
    }
}
