//! Phase 11.5 Task 3 — MVCC temporal store skeleton (RED: bodies unimplemented).
//!
//! Public surface locked-in; impl lands in the follow-up green commit.

use crate::row::Row;
use std::collections::BTreeMap;

pub type EntityKey = Vec<u8>;

#[derive(Debug, Clone, PartialEq)]
pub enum MvccVersion {
    Live { row: Row, wall_ms: u64 },
    Tombstone { wall_ms: u64 },
    Retracted { undo_of: u64, wall_ms: u64 },
}

impl MvccVersion {
    pub fn wall_ms(&self) -> u64 {
        match self {
            MvccVersion::Live { wall_ms, .. } => *wall_ms,
            MvccVersion::Tombstone { wall_ms } => *wall_ms,
            MvccVersion::Retracted { wall_ms, .. } => *wall_ms,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RetractError {
    TargetNotFound,
    AlreadyRetracted,
}

#[derive(Debug)]
pub struct TemporalStore {
    _chains: BTreeMap<EntityKey, BTreeMap<u64, MvccVersion>>,
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
            _chains: BTreeMap::new(),
            max_versions_per_entity: 1024,
        }
    }

    pub fn upsert(&mut self, _key: EntityKey, _lsn: u64, _row: Row, _wall_ms: u64) {
        unimplemented!("Phase 11.5 Task 3.b")
    }

    pub fn delete(&mut self, _key: EntityKey, _lsn: u64, _wall_ms: u64) {
        unimplemented!("Phase 11.5 Task 3.b")
    }

    pub fn retract(
        &mut self,
        _key: &EntityKey,
        _target_lsn: u64,
        _retract_lsn: u64,
        _wall_ms: u64,
    ) -> Result<Option<u64>, RetractError> {
        unimplemented!("Phase 11.5 Task 3.b")
    }

    pub fn lookup_at_lsn(&self, _key: &EntityKey, _as_of_lsn: u64) -> Option<&Row> {
        unimplemented!("Phase 11.5 Task 3.b")
    }

    pub fn sweep_retention(&mut self, _now_wall_ms: u64, _retention_ms: u64) -> usize {
        unimplemented!("Phase 11.5 Task 3.b")
    }

    pub fn version_count(&self, _key: &EntityKey) -> usize {
        unimplemented!("Phase 11.5 Task 3.b")
    }

    pub fn total_version_count(&self) -> usize {
        unimplemented!("Phase 11.5 Task 3.b")
    }

    pub fn iter_chains(&self) -> impl Iterator<Item = (&EntityKey, &BTreeMap<u64, MvccVersion>)> {
        self._chains.iter()
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
        let at35 = s.lookup_at_lsn(&key("k"), 35).unwrap();
        assert_eq!(at35.get("v"), Some(&Value::I64(1)));
        let at25 = s.lookup_at_lsn(&key("k"), 25).unwrap();
        assert_eq!(at25.get("v"), Some(&Value::I64(2)));
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
        assert_eq!(dropped, 2);
        assert_eq!(s.version_count(&key("k")), 1);
        let r = s.lookup_at_lsn(&key("k"), 35).unwrap();
        assert_eq!(r.get("v"), Some(&Value::I64(3)));
    }

    #[test]
    fn mvcc_chain_cap_drops_oldest_live_when_exceeded() {
        let mut s = TemporalStore::new();
        s.max_versions_per_entity = 4;
        for i in 1..=5 {
            s.upsert(key("k"), i * 10, row_v(i as i64), (i * 1000) as u64);
        }
        assert_eq!(s.version_count(&key("k")), 4);
        assert!(s.lookup_at_lsn(&key("k"), 15).is_none());
        let r = s.lookup_at_lsn(&key("k"), 55).unwrap();
        assert_eq!(r.get("v"), Some(&Value::I64(5)));
    }
}
