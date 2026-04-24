//! Phase 11.5: Temporal-table MVCC store + retraction primitive.
//!
//! See `.planning/phases/11.5-temporal-tables-retraction-primitive/11.5-CONTEXT.md`
//! for the full decision trail. Key decisions this module implements:
//!
//! - **D-01**: `BTreeMap<EntityKey, BTreeMap<Lsn, MvccVersion>>` — per-entity
//!   sorted chain of versions. `as_of=<lsn>` lookup is
//!   `chain.range(..=as_of).next_back()` after the retraction-skip pass.
//! - **D-02**: `MvccVersion::Live | Tombstone | Retracted` — three variants
//!   so retraction is distinct from deletion and preserves audit trail.
//! - **D-04**: **Tombstone-style retraction** — retracting event_id K at
//!   retract_lsn R inserts a `Retracted{undo_of:K}` marker at R. The original
//!   version at K is NEVER removed. Lookups at `as_of < R` still see the
//!   original upsert; lookups at `as_of >= R` skip it.
//! - **D-05**: Retention sweep is driven by wall-clock timestamps stored on
//!   every version; the snapshot path and a per-write soft cap both trigger
//!   it.
//! - **D-07**: `lookup_at_lsn(as_of)` returns the version at the largest LSN
//!   ≤ `as_of` (inclusive). Matches "what would I have seen at that moment"
//!   semantics required for Phase 12 PIT joins.
//! - **D-10/D-11**: `event_id = WAL LSN` — we don't mint a separate ID space.
//!
//! This module is pure data-structure code; it holds no locks and knows
//! nothing about WAL / HTTP / registry. The server wires MVCC stores up per
//! table under the existing single-writer lock.

use crate::row::Row;
use std::collections::BTreeMap;

/// Byte-encoded primary key. Phase 11.5 D-03: composite keys serialize as
/// `length-prefix(f1) || length-prefix(f2) || ...` so `BTreeMap` ordering
/// is total-order-preserving and future range scans remain meaningful.
pub type EntityKey = Vec<u8>;

/// A single version in an MVCC chain.
///
/// Each variant carries `wall_ms` (the wall-clock time of the write) so
/// retention-sweep (D-05) can compare against `now_wall_ms` without walking
/// the WAL. 8 bytes of overhead per version is acceptable for v0 — beats
/// maintaining a separate side-index.
#[derive(Debug, Clone, PartialEq)]
pub enum MvccVersion {
    /// A live upsert at this LSN.
    Live { row: Row, wall_ms: u64 },
    /// A delete at this LSN — the key has no value for any `as_of >= lsn`.
    Tombstone { wall_ms: u64 },
    /// A retraction marker: the upsert/delete at `undo_of` is shadowed for
    /// any `as_of >= lsn`. Lookups walking back from `as_of` skip both this
    /// marker and the version at `undo_of` when `undo_of <= as_of < lsn` is
    /// NOT the case (i.e., only when the retraction has taken effect at
    /// `as_of`).
    Retracted { undo_of: u64, wall_ms: u64 },
}

impl MvccVersion {
    /// Wall-clock timestamp stamped on this version at write time. Used by
    /// `sweep_retention` to age out versions.
    pub fn wall_ms(&self) -> u64 {
        match self {
            MvccVersion::Live { wall_ms, .. } => *wall_ms,
            MvccVersion::Tombstone { wall_ms } => *wall_ms,
            MvccVersion::Retracted { wall_ms, .. } => *wall_ms,
        }
    }
}

/// Error shape for `TemporalStore::retract`. Mapped to HTTP status codes in
/// the server handler (D-17).
#[derive(Debug, PartialEq, Eq)]
pub enum RetractError {
    /// Target event_id has no upsert/delete in this chain (either never
    /// written or already swept by retention).
    TargetNotFound,
    /// A `Retracted{undo_of: target}` marker already exists in the chain.
    AlreadyRetracted,
}

/// Per-table MVCC store. Outer `BTreeMap` keyed by entity primary key;
/// inner `BTreeMap` keyed by LSN.
///
/// `max_versions_per_entity` is the D-05 soft cap — when a single chain
/// would exceed this length after an insert, the oldest version is dropped
/// immediately. Defaults to 1024 (configurable per instance).
#[derive(Debug)]
pub struct TemporalStore {
    chains: BTreeMap<EntityKey, BTreeMap<u64, MvccVersion>>,
    pub max_versions_per_entity: usize,
}

impl Default for TemporalStore {
    fn default() -> Self {
        Self::new()
    }
}

impl TemporalStore {
    pub fn new() -> Self {
        Self {
            chains: BTreeMap::new(),
            max_versions_per_entity: 1024,
        }
    }

    /// Insert a `Live` version for `key` at `lsn`. If the chain exceeds
    /// `max_versions_per_entity` afterwards, the smallest-LSN entry is
    /// dropped.
    pub fn upsert(&mut self, key: EntityKey, lsn: u64, row: Row, wall_ms: u64) {
        let chain = self.chains.entry(key).or_default();
        chain.insert(lsn, MvccVersion::Live { row, wall_ms });
        while chain.len() > self.max_versions_per_entity {
            // Drop oldest. `pop_first` returns the smallest-key entry.
            if let Some((_k, _v)) = chain.pop_first() {
                // Nothing else — the dropped version is gone.
            }
        }
    }

    /// Insert a `Tombstone` version for `key` at `lsn`. Same cap semantics
    /// as `upsert`.
    pub fn delete(&mut self, key: EntityKey, lsn: u64, wall_ms: u64) {
        let chain = self.chains.entry(key).or_default();
        chain.insert(lsn, MvccVersion::Tombstone { wall_ms });
        while chain.len() > self.max_versions_per_entity {
            if let Some((_k, _v)) = chain.pop_first() {
                // dropped
            }
        }
    }

    /// Tombstone-style retraction (D-04). Appends a
    /// `Retracted{undo_of: target_lsn}` marker at `retract_lsn`.
    ///
    /// Returns:
    /// - `Ok(Some(prior_lsn))` — the LSN of the upsert/tombstone that is
    ///   now the visible version at `retract_lsn` (useful for clients that
    ///   want to know what they rolled back to).
    /// - `Ok(None)` — there is no prior live version to restore (the
    ///   retracted upsert was the first write to this key).
    /// - `Err(TargetNotFound)` — no version at `target_lsn` in this chain.
    /// - `Err(AlreadyRetracted)` — a `Retracted{undo_of: target_lsn}`
    ///   marker already exists.
    pub fn retract(
        &mut self,
        key: &EntityKey,
        target_lsn: u64,
        retract_lsn: u64,
        wall_ms: u64,
    ) -> Result<Option<u64>, RetractError> {
        let chain = self
            .chains
            .get_mut(key)
            .ok_or(RetractError::TargetNotFound)?;

        // Target must exist.
        if !chain.contains_key(&target_lsn) {
            return Err(RetractError::TargetNotFound);
        }

        // Detect already-retracted.
        for v in chain.values() {
            if let MvccVersion::Retracted { undo_of, .. } = v {
                if *undo_of == target_lsn {
                    return Err(RetractError::AlreadyRetracted);
                }
            }
        }

        // Compute the prior-visible LSN for the return value. Walk versions
        // strictly below target_lsn, newest-first, skipping any `Retracted`
        // markers and any LSN that is itself the undo_of target of a
        // pre-existing retraction.
        let retracted_targets: std::collections::HashSet<u64> = chain
            .values()
            .filter_map(|v| match v {
                MvccVersion::Retracted { undo_of, .. } => Some(*undo_of),
                _ => None,
            })
            .collect();

        let prior = chain
            .range(..target_lsn)
            .rev()
            .find(|(lsn, v)| {
                !matches!(v, MvccVersion::Retracted { .. }) && !retracted_targets.contains(lsn)
            })
            .map(|(lsn, _)| *lsn);

        // Insert the retraction marker.
        chain.insert(
            retract_lsn,
            MvccVersion::Retracted {
                undo_of: target_lsn,
                wall_ms,
            },
        );

        // Enforce soft cap even on retract.
        while chain.len() > self.max_versions_per_entity {
            if chain.pop_first().is_none() {
                break;
            }
        }

        Ok(prior)
    }

    /// Returns the `Row` visible at `as_of_lsn` (D-07 semantics: version at
    /// largest LSN ≤ as_of wins). Returns `None` if the key has no live
    /// version at that point in time — either because it hasn't been
    /// written, because the most recent version is a tombstone, or because
    /// all live versions are retracted.
    pub fn lookup_at_lsn(&self, key: &EntityKey, as_of_lsn: u64) -> Option<&Row> {
        let chain = self.chains.get(key)?;

        // Gather retracted targets whose Retracted marker is visible at
        // as_of_lsn. Those upserts/tombstones are shadowed for this query.
        let retracted_targets: std::collections::HashSet<u64> = chain
            .range(..=as_of_lsn)
            .filter_map(|(_, v)| match v {
                MvccVersion::Retracted { undo_of, .. } => Some(*undo_of),
                _ => None,
            })
            .collect();

        // Walk newest-first from as_of_lsn; skip Retracted markers (they
        // aren't data) and skip any version whose LSN is shadowed.
        for (&lsn, v) in chain.range(..=as_of_lsn).rev() {
            match v {
                MvccVersion::Retracted { .. } => continue,
                _ if retracted_targets.contains(&lsn) => continue,
                MvccVersion::Live { row, .. } => return Some(row),
                MvccVersion::Tombstone { .. } => return None,
            }
        }
        None
    }

    /// Age out versions older than `retention_ms` (D-05). Returns the
    /// number of versions dropped across all chains. Chains that become
    /// empty are retained (empty map); callers that care can sweep them
    /// separately.
    pub fn sweep_retention(&mut self, now_wall_ms: u64, retention_ms: u64) -> usize {
        let mut dropped = 0usize;
        for chain in self.chains.values_mut() {
            let old_keys: Vec<u64> = chain
                .iter()
                .filter(|(_, v)| now_wall_ms.saturating_sub(v.wall_ms()) > retention_ms)
                .map(|(k, _)| *k)
                .collect();
            for k in old_keys {
                chain.remove(&k);
                dropped += 1;
            }
        }
        dropped
    }

    /// Number of versions currently retained for `key`. Mostly for tests
    /// and metrics.
    pub fn version_count(&self, key: &EntityKey) -> usize {
        self.chains.get(key).map(|c| c.len()).unwrap_or(0)
    }

    /// Total version count across all chains — memory-budget metric hook.
    pub fn total_version_count(&self) -> usize {
        self.chains.values().map(|c| c.len()).sum()
    }

    /// Iterate chain entries for snapshot serialization (Phase 11.5 D-13/D-14
    /// landed here for forward-compat — even though snapshot v2 body is
    /// deferred, exposing the iterator now keeps the public surface stable).
    pub fn iter_chains(&self) -> impl Iterator<Item = (&EntityKey, &BTreeMap<u64, MvccVersion>)> {
        self.chains.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::Value;

    fn row_v(n: i64) -> Row {
        Row::new().with_field("v", Value::I64(n))
    }

    fn key(s: &str) -> EntityKey {
        s.as_bytes().to_vec()
    }

    #[test]
    fn mvcc_upsert_then_lookup_returns_value() {
        let mut s = TemporalStore::new();
        s.upsert(key("k"), 10, row_v(1), 1000);
        let r = s.lookup_at_lsn(&key("k"), 11).expect("row present");
        assert_eq!(r.get("v"), Some(&Value::I64(1)));
    }

    #[test]
    fn mvcc_lookup_at_earlier_lsn_returns_none() {
        let mut s = TemporalStore::new();
        s.upsert(key("k"), 10, row_v(1), 1000);
        assert!(s.lookup_at_lsn(&key("k"), 9).is_none());
    }

    #[test]
    fn mvcc_two_upserts_lookup_picks_largest_le() {
        let mut s = TemporalStore::new();
        s.upsert(key("k"), 10, row_v(1), 1000);
        s.upsert(key("k"), 20, row_v(2), 2000);
        let at15 = s.lookup_at_lsn(&key("k"), 15).unwrap();
        assert_eq!(at15.get("v"), Some(&Value::I64(1)));
        let at25 = s.lookup_at_lsn(&key("k"), 25).unwrap();
        assert_eq!(at25.get("v"), Some(&Value::I64(2)));
    }

    #[test]
    fn mvcc_delete_creates_tombstone() {
        let mut s = TemporalStore::new();
        s.upsert(key("k"), 10, row_v(1), 1000);
        s.delete(key("k"), 20, 2000);
        assert!(s.lookup_at_lsn(&key("k"), 25).is_none());
        let at15 = s.lookup_at_lsn(&key("k"), 15).unwrap();
        assert_eq!(at15.get("v"), Some(&Value::I64(1)));
    }

    #[test]
    fn mvcc_retract_skips_target_version_at_or_after_retract_lsn() {
        let mut s = TemporalStore::new();
        s.upsert(key("k"), 10, row_v(1), 1000);
        s.upsert(key("k"), 20, row_v(2), 2000);
        let prior = s.retract(&key("k"), 20, 30, 3000).unwrap();
        assert_eq!(prior, Some(10));

        // as_of=35 → retraction took effect; we should see v=1 (lsn=10).
        let at35 = s.lookup_at_lsn(&key("k"), 35).unwrap();
        assert_eq!(at35.get("v"), Some(&Value::I64(1)));

        // as_of=25 → retraction at 30 not yet visible; we still see v=2.
        let at25 = s.lookup_at_lsn(&key("k"), 25).unwrap();
        assert_eq!(at25.get("v"), Some(&Value::I64(2)));

        // as_of=31 → retraction just took effect; we see v=1.
        let at31 = s.lookup_at_lsn(&key("k"), 31).unwrap();
        assert_eq!(at31.get("v"), Some(&Value::I64(1)));
    }

    #[test]
    fn mvcc_double_retract_is_idempotent_or_detected() {
        let mut s = TemporalStore::new();
        s.upsert(key("k"), 10, row_v(1), 1000);
        s.retract(&key("k"), 10, 20, 2000).unwrap();
        let err = s.retract(&key("k"), 10, 30, 3000);
        assert_eq!(err, Err(RetractError::AlreadyRetracted));
    }

    #[test]
    fn mvcc_retain_within_window_drops_old_versions() {
        let mut s = TemporalStore::new();
        s.upsert(key("k"), 10, row_v(1), 1000);
        s.upsert(key("k"), 20, row_v(2), 5000);
        s.upsert(key("k"), 30, row_v(3), 8000);
        let dropped = s.sweep_retention(10000, 3000);
        assert_eq!(
            dropped, 2,
            "lsn=10 (age 9000) and lsn=20 (age 5000) exceed 3000"
        );
        assert_eq!(s.version_count(&key("k")), 1);
        // Remaining is lsn=30.
        let r = s.lookup_at_lsn(&key("k"), 35).unwrap();
        assert_eq!(r.get("v"), Some(&Value::I64(3)));
    }

    #[test]
    fn mvcc_chain_cap_drops_oldest_live_when_exceeded() {
        let mut s = TemporalStore::new();
        s.max_versions_per_entity = 4;
        for i in 1u64..=5u64 {
            s.upsert(key("k"), i * 10, row_v(i as i64), i * 1000);
        }
        assert_eq!(s.version_count(&key("k")), 4);
        // Oldest (lsn=10) should be gone; lookup at as_of=15 now returns None.
        assert!(s.lookup_at_lsn(&key("k"), 15).is_none());
        // Newest still visible.
        let r = s.lookup_at_lsn(&key("k"), 55).unwrap();
        assert_eq!(r.get("v"), Some(&Value::I64(5)));
    }
}
