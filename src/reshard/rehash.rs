//! Deterministic shard-routing function for offline reshard migrations.
//!
//! `rehash_to_shard` uses `ahash::AHasher` (same hasher as Phase 48 routing)
//! to consistently map any key string to a shard index in [0, shard_count).
//!
//! # Stability
//!
//! The hash result is stable for a given `(key, shard_count)` pair within the
//! same build. It uses ahash's fixed-seed path (`AHasher::default()`), which
//! is deterministic for a given binary but may differ across major ahash
//! versions. For offline migration this is sufficient — the reshard tool and
//! the live server use the same binary.

use std::hash::{Hash, Hasher};

use ahash::AHasher;

/// Route `key` to a shard index in [0, `shard_count`).
///
/// Uses `ahash::AHasher` for speed and consistency with the Phase 48
/// live routing path (`hash(key.as_bytes()) % shard_count`).
///
/// # Special case
///
/// When `shard_count == 1` this always returns `0` (identity — all keys
/// land on shard 0, matching the N=1 single-shard layout).
///
/// # Panics
///
/// Panics if `shard_count == 0` (division by zero). The CLI validates that
/// `--to K` is at least 1 before calling this function.
pub fn rehash_to_shard(key: &str, shard_count: u8) -> u8 {
    if shard_count == 1 {
        return 0;
    }
    let mut hasher = AHasher::default();
    key.as_bytes().hash(&mut hasher);
    let h = hasher.finish();
    (h % shard_count as u64) as u8
}
