//! N=1 ↔ N=8 sharding parity proptest integration test binary.
//!
//! Phase 52-07 (TPC-CORR-05). Pre-merge gate for v1.2 → main.
//!
//! Entry point for `cargo test -p beava --test sharding_parity`.
//!
//! CI:
//!   Nightly:  PROPTEST_CASES=10000  (bench-nightly.yml `sharding-parity-proptest`)
//!   PR smoke: PROPTEST_CASES=50     (pr.yml `sharding-parity-smoke`)

mod proptests;
