//! Phase 19.1-03 — env-var tunables for the hand-rolled WAL ring config.
//!
//! Tests cover:
//!   1. Default config when no env vars set:   4 × 32 MiB tick=20ms (D-01).
//!   2. Env-var overrides reach the resolved config (D-02).
//!   3. Out-of-range values clamp to safe ranges; near-zero values clamp to
//!      minimums (threat model OOM-typo guard).
//!
//! Env vars are process-global; tests are forced single-threaded via the
//! `--test-threads=1` invocation in the plan's verify command. Each test
//! clears all three vars at start and end.

use beava_server::wal_config::WalConfig;

const VARS: [&str; 3] = [
    "BEAVA_WAL_BUFFERS",
    "BEAVA_WAL_BUFFER_SIZE_MB",
    "BEAVA_WAL_TICK_MS",
];

/// Plan 12.6-15: env vars are process-global; tests must run sequentially
/// to avoid one clearing another's set values mid-read. The plan-author
/// originally relied on `--test-threads=1` at the cargo invocation but
/// that's not enforced when tests run as part of `cargo test --workspace`.
/// In-test mutex is the bullet-proof variant.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn clear_env() {
    for v in VARS.iter() {
        std::env::remove_var(v);
    }
}

#[test]
fn test_default_wal_config_4x32_tick20() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    clear_env();
    let cfg = WalConfig::resolve_from_env();
    assert_eq!(cfg.buffers, 4, "D-01 default buffers should be 4");
    assert_eq!(
        cfg.buffer_size_mb, 32,
        "D-01 default buffer_size_mb should be 32"
    );
    assert_eq!(cfg.tick_ms, 20, "D-01 default tick_ms should be 20");
}

#[test]
fn test_env_overrides_apply() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    clear_env();
    std::env::set_var("BEAVA_WAL_BUFFERS", "8");
    std::env::set_var("BEAVA_WAL_BUFFER_SIZE_MB", "64");
    std::env::set_var("BEAVA_WAL_TICK_MS", "100");
    let cfg = WalConfig::resolve_from_env();
    clear_env();
    assert_eq!(cfg.buffers, 8, "BEAVA_WAL_BUFFERS=8 should apply");
    assert_eq!(
        cfg.buffer_size_mb, 64,
        "BEAVA_WAL_BUFFER_SIZE_MB=64 should apply"
    );
    assert_eq!(cfg.tick_ms, 100, "BEAVA_WAL_TICK_MS=100 should apply");
}

#[test]
fn test_clamp_ranges_reject_oom_typos() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    // High side — operator typo like BEAVA_WAL_BUFFER_SIZE_MB=10000 must clamp
    // to the documented OOM-guard ceiling, not allocate ~10 GB per buffer.
    clear_env();
    std::env::set_var("BEAVA_WAL_BUFFERS", "99999");
    std::env::set_var("BEAVA_WAL_BUFFER_SIZE_MB", "10000");
    std::env::set_var("BEAVA_WAL_TICK_MS", "99999");
    let cfg = WalConfig::resolve_from_env();
    assert!(
        cfg.buffers <= 32,
        "buffers must clamp to <= 32 (got {})",
        cfg.buffers
    );
    assert!(
        cfg.buffer_size_mb <= 256,
        "buffer_size_mb must clamp to <= 256 (got {})",
        cfg.buffer_size_mb
    );
    assert!(
        cfg.tick_ms <= 1000,
        "tick_ms must clamp to <= 1000 (got {})",
        cfg.tick_ms
    );

    // Low side — `BEAVA_WAL_BUFFERS=0` (operator hoping to disable WAL) must
    // clamp to the minimum-viable ring of 2 buffers, not run with 0/1 buffers.
    clear_env();
    std::env::set_var("BEAVA_WAL_BUFFERS", "0");
    std::env::set_var("BEAVA_WAL_BUFFER_SIZE_MB", "0");
    std::env::set_var("BEAVA_WAL_TICK_MS", "0");
    let cfg = WalConfig::resolve_from_env();
    clear_env();
    assert!(
        cfg.buffers >= 2,
        "buffers must clamp to >= 2 (got {})",
        cfg.buffers
    );
    assert!(
        cfg.buffer_size_mb >= 4,
        "buffer_size_mb must clamp to >= 4 (got {})",
        cfg.buffer_size_mb
    );
    assert!(
        cfg.tick_ms >= 1,
        "tick_ms must clamp to >= 1 (got {})",
        cfg.tick_ms
    );
}
