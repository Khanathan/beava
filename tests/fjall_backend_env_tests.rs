//! Phase 53-02 Task 1 — TDD RED env-clamp + sysinfo unit tests.
//!
//! Temporary integration-test file: in Task 2 the executor moves these
//! tests into `src/shard/fjall_backend.rs::#[cfg(test)] mod tests` (idiomatic
//! Rust — env tests belong next to the module they test). At RED state,
//! putting them here is the fastest path to a "cannot find symbol" failure,
//! because `src/shard/fjall_backend.rs` does not yet exist.
//!
//! Tests 3–7 from 53-02-PLAN.md:
//!   - Test 3: `BEAVA_FJALL_FSYNC_MS` clamp (up/down/parse-fail)
//!   - Test 4: `BEAVA_FJALL_FSYNC_DISABLE=1` overrides `_FSYNC_MS`
//!   - Test 5: W-5 revision — `BEAVA_FJALL_CACHE_MB` default scales with real
//!             host memory, NOT with a hardcoded 8192 constant
//!   - Test 6: `BEAVA_FJALL_BLOCK_SIZE` clamp (power-of-two range)
//!   - Test 7: W-5 revision — `read_sys_mem_mb` returns a realistic nonzero
//!             value and is cached across calls (OnceLock)

use std::sync::{Mutex, OnceLock};

use beava::shard::fjall_backend::{fjall_config_from_env, read_sys_mem_mb};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn clear_fjall_env() {
    for k in [
        "BEAVA_FJALL_FSYNC_MS",
        "BEAVA_FJALL_FSYNC_DISABLE",
        "BEAVA_FJALL_CACHE_MB",
        "BEAVA_FJALL_FLUSH_WORKERS",
        "BEAVA_FJALL_COMPACTION_WORKERS",
        "BEAVA_FJALL_BLOCK_SIZE",
        "BEAVA_FJALL_MAX_MEMTABLE_MB",
    ] {
        std::env::remove_var(k);
    }
}

#[test]
fn env_clamp_fsync_ms_out_of_range_logs_and_clamps() {
    let _g = env_lock().lock().unwrap();
    clear_fjall_env();

    // Lower bound: 0 clamps up to 1.
    std::env::set_var("BEAVA_FJALL_FSYNC_MS", "0");
    let cfg = fjall_config_from_env(1);
    assert_eq!(cfg.fsync_ms, Some(1), "0 must clamp up to 1");

    // Upper bound: 2000 clamps down to 1000.
    std::env::set_var("BEAVA_FJALL_FSYNC_MS", "2000");
    let cfg = fjall_config_from_env(1);
    assert_eq!(cfg.fsync_ms, Some(1000), "2000 must clamp down to 1000");

    // Parse failure: non-numeric falls back to default 5.
    std::env::set_var("BEAVA_FJALL_FSYNC_MS", "abc");
    let cfg = fjall_config_from_env(1);
    assert_eq!(cfg.fsync_ms, Some(5), "non-numeric must fall back to default 5");

    clear_fjall_env();
}

#[test]
fn env_clamp_fsync_disable_overrides_fsync_ms() {
    let _g = env_lock().lock().unwrap();
    clear_fjall_env();

    std::env::set_var("BEAVA_FJALL_FSYNC_MS", "10");
    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    let cfg = fjall_config_from_env(1);
    assert_eq!(
        cfg.fsync_ms, None,
        "FSYNC_DISABLE=1 must force fsync_ms=None regardless of FSYNC_MS"
    );

    clear_fjall_env();
}

#[test]
fn env_clamp_cache_mb_default_scales_with_real_sys_mem() {
    let _g = env_lock().lock().unwrap();
    clear_fjall_env();

    // With BEAVA_FJALL_CACHE_MB unset, the default formula is
    //     (real_sys_mem_mb / num_shards / 2).min(512).max(16)
    // using the REAL host memory (no 8192 hardcode).
    let cfg = fjall_config_from_env(8);
    let sys_mem_mb = read_sys_mem_mb();
    let expected = (sys_mem_mb / 8 / 2).min(512).max(16);
    assert_eq!(
        cfg.cache_mb, expected,
        "cache_mb default must follow (sys_mem_mb/num_shards/2).min(512).max(16); sys_mem_mb={} expected={} got={}",
        sys_mem_mb, expected, cfg.cache_mb
    );

    // Sanity bounds per research table.
    assert!(cfg.cache_mb <= 512, "cache_mb must be <= 512");
    assert!(cfg.cache_mb >= 16, "cache_mb must be >= 16");

    // Real sys_mem read must exceed 512 on any plausible dev box / CI runner.
    // Catches the case where sysinfo returned 0 or a stub value.
    assert!(
        sys_mem_mb > 512,
        "read_sys_mem_mb() returned suspiciously low value {} MiB — sysinfo stub?",
        sys_mem_mb
    );

    clear_fjall_env();
}

#[test]
fn env_clamp_block_size_must_be_power_of_two_range() {
    let _g = env_lock().lock().unwrap();
    clear_fjall_env();

    // Lower bound: 100 clamps up to 1024.
    std::env::set_var("BEAVA_FJALL_BLOCK_SIZE", "100");
    let cfg = fjall_config_from_env(1);
    assert_eq!(cfg.block_size, 1024, "100 must clamp up to 1024");

    // Upper bound: 1_000_000 clamps down to 65536.
    std::env::set_var("BEAVA_FJALL_BLOCK_SIZE", "1000000");
    let cfg = fjall_config_from_env(1);
    assert_eq!(cfg.block_size, 65536, "1_000_000 must clamp down to 65536");

    clear_fjall_env();
}

#[test]
fn read_sys_mem_mb_returns_nonzero_and_cached() {
    // No env_lock needed — read_sys_mem_mb is env-free.
    let a = read_sys_mem_mb();
    assert!(
        a >= 1024,
        "read_sys_mem_mb must enforce the >= 1024 MiB floor; got {}",
        a
    );
    let b = read_sys_mem_mb();
    assert_eq!(a, b, "OnceLock cache must return identical value on repeated calls");
}
