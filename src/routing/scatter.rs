//! Scatter-gather over N logical shards — Phase 51 (TPC-PERF-05).
//!
//! At N=1 (current Wave 1 deployment), scatter_gather fans out to the single
//! in-process engine and merge_fn is called with a one-element Vec. At N>1
//! (Wave 2+), each shard is a pinned thread with an SPSC inbox; this module
//! will be wired to the actual ShardHandle channel. For now the shard slice
//! is a unit `[()]` and the caller provides the engine reference directly.
//!
//! The public API is stable across both waves: callers use `scatter_gather`
//! and provide a merge function; the fanout mechanism is an implementation detail.

/// Fan out a synchronous request to all N shards, collect responses, merge.
///
/// `per_shard_fn` is called once per shard index (0..n_shards) and returns
/// a response value. All responses are collected into a Vec and passed to
/// `merge_fn`.
///
/// Wave 1: this runs synchronously (no async, no channels). The shard count
/// is 1. Wave 2 replaces this body with `futures::join_all` over SPSC inboxes.
///
/// p99 overhead vs point-read: O(N) sequential calls at N=1 → ~0 μs added.
/// At N>1 with `join_all`: all shards are polled concurrently; overhead is
/// the tokio poll overhead, which is <1 μs per shard in practice.
pub fn scatter_gather<Resp>(
    n_shards: usize,
    per_shard_fn: impl Fn(usize) -> Resp,
    merge_fn: impl Fn(Vec<Resp>) -> Resp,
) -> Resp {
    let responses: Vec<Resp> = (0..n_shards).map(|shard_id| per_shard_fn(shard_id)).collect();
    merge_fn(responses)
}

/// Merge a list of stream name lists: deduplicate, preserve first-seen order.
pub fn merge_stream_lists(lists: Vec<Vec<String>>) -> Vec<String> {
    use ahash::AHashSet;
    let mut seen: AHashSet<String> = AHashSet::new();
    let mut merged: Vec<String> = Vec::new();
    for list in lists {
        for name in list {
            if seen.insert(name.clone()) {
                merged.push(name);
            }
        }
    }
    merged
}

/// Merge per-stream watermarks: take the minimum non-zero value across shards.
/// Returns `None` if all values are `None`.
pub fn merge_watermark_min(values: Vec<Option<u64>>) -> Option<u64> {
    values.into_iter().flatten().reduce(u64::min)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Test 1: no duplicates from N=4 identical shard lists
    // -----------------------------------------------------------------------
    #[test]
    fn test_scatter_gather_dedup_n4_identical_lists() {
        let result = scatter_gather(
            4,
            |_shard_id| vec!["A".to_string(), "B".to_string(), "C".to_string()],
            merge_stream_lists,
        );
        assert_eq!(result.len(), 3, "should dedup to 3 unique streams");
        assert!(result.contains(&"A".to_string()));
        assert!(result.contains(&"B".to_string()));
        assert!(result.contains(&"C".to_string()));
    }

    // -----------------------------------------------------------------------
    // Test 2: watermark min across shards
    // -----------------------------------------------------------------------
    #[test]
    fn test_scatter_gather_watermark_min() {
        let watermarks: Vec<Option<u64>> = vec![Some(100), Some(200), Some(50), Some(300)];
        let min = merge_watermark_min(watermarks);
        assert_eq!(min, Some(50), "global min should be the smallest value");
    }

    // -----------------------------------------------------------------------
    // Test 3: empty shard list edge case
    // -----------------------------------------------------------------------
    #[test]
    fn test_scatter_gather_empty_shards_merge() {
        // 4 shards: 2 return empty, 2 return ["A"]
        let result = scatter_gather(
            4,
            |shard_id| {
                if shard_id < 2 {
                    vec![]
                } else {
                    vec!["A".to_string()]
                }
            },
            merge_stream_lists,
        );
        assert_eq!(result, vec!["A".to_string()]);
    }

    // -----------------------------------------------------------------------
    // Test 4: watermark min with all None
    // -----------------------------------------------------------------------
    #[test]
    fn test_merge_watermark_min_all_none() {
        let result = merge_watermark_min(vec![None, None, None]);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // Test 5: watermark min with mixed Some/None
    // -----------------------------------------------------------------------
    #[test]
    fn test_merge_watermark_min_mixed() {
        let result = merge_watermark_min(vec![None, Some(42), None, Some(7)]);
        assert_eq!(result, Some(7));
    }
}
