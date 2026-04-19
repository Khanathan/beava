//! Phase 53-01 spike gate — W-4 revision — fjall 2.11 cache-stats API probe.
//!
//! Purpose: decide whether Plan 05 (metrics) should emit the
//! `beava_fjall_cache_hit_ratio` gauge or omit it, and whether Plan 06 (docs)
//! should document the `< 0.8 sustained` alert.
//!
//! Probe result is a single boolean (`cache_stats_available`) recorded in
//! `53-01-SPIKE-RESULTS.md` frontmatter. This file's job is to *compile*
//! only when fjall 2.11 exposes a usable cache-hit/miss accessor — if the
//! probe fails (no such method), this test is annotated `#[ignore]` and
//! the signature to inspect is documented inline.
//!
//! **Probe outcome (2026-04-19, fjall 2.11.2):**
//!   - `Keyspace::cache_capacity()` exists — returns `u64` (configured max bytes).
//!   - `Keyspace::cache_hits()` — DOES NOT EXIST.
//!   - `Keyspace::cache_misses()` — DOES NOT EXIST.
//!   - `Keyspace::cache_stats()` — DOES NOT EXIST.
//!   - `PartitionHandle::cache_hits()` — DOES NOT EXIST.
//!   - Transitive `lsm-tree 2.10.4` exposes `Cache` via `Config::use_cache`
//!     but no public hit/miss accessor on the `Cache` type.
//!   - Verdict: `cache_stats_available: false`.
//!
//! Implication:
//!   - Plan 05 Task 2 MUST NOT emit `beava_fjall_cache_hit_ratio` (would
//!     have to hardcode `1.0` placeholder — makes the alert vacuous).
//!   - Plan 06 operations.md MUST NOT document the `< 0.8 sustained` alert.
//!   - If fjall adds cache stats in a later 2.x patch, re-run this probe.
//!
//! This test verifies the one accessor that DOES exist (capacity) so the
//! spike is not purely documentation.

use tempfile::TempDir;

#[test]
fn probe_cache_capacity_accessor_is_available() {
    let tmp = TempDir::new().expect("tempdir");
    let cfg = fjall::Config::new(tmp.path().join("fjall"))
        .fsync_ms(None)
        .cache_size(32 * 1024 * 1024);
    let ks = cfg.open().expect("open keyspace");

    // The one cache-related accessor fjall 2.11 actually exposes on Keyspace.
    let capacity = ks.cache_capacity();
    assert_eq!(
        capacity,
        32 * 1024 * 1024,
        "cache_capacity() should echo configured cache_size"
    );

    // If a later fjall 2.x release adds cache_hits()/cache_misses(), this
    // block gets uncommented and the probe flips to `cache_stats_available: true`.
    //
    // let hits = ks.cache_hits();
    // let misses = ks.cache_misses();
    // assert!(hits + misses > 0);

    eprintln!(
        "cache_stats_available=false (only Keyspace::cache_capacity() = {} exists in fjall 2.11.2)",
        capacity
    );
}
