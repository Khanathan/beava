//! fjall 2.11 keyspace + partition lifecycle (Phase 53 Plan 02).
//!
//! ## D-01 (from Plan 01): ONE keyspace, N partitions
//!
//! There is exactly ONE `fjall::Keyspace` rooted at `data_dir/fjall/`, with
//! N per-shard partitions named `shard-0`, `shard-1`, …, `shard-(N-1)` inside
//! it. See 53-RESEARCH.md §Common Pitfalls §Pitfall 1. **Do NOT open one
//! keyspace per shard** — doing so fragments the journal, multiplies fsync
//! threads, and breaks atomic cross-partition batches.
//!
//! ## Scope boundary (Plan 02)
//!
//! This module adds new surfaces only:
//! - `FjallConfig` — clamped-env-driven config struct
//! - `open_keyspace_from_env` — keyspace constructor
//! - `open_shard_partition` — partition-per-shard accessor
//! - `fjall_config_from_env` — env → FjallConfig translator
//! - `read_sys_mem_mb` — W-5 revision: real host-mem read via `sysinfo`
//!
//! It does NOT touch `Shard.state` (still AHashMap) or `src/shard/store.rs`.
//! Plan 03 wires `Shard.state` through this module.
//!
//! ## Trust boundary — T-53-02-04
//!
//! This module accepts `&Path` as an authoritative, pre-canonicalized root.
//! The caller (boot path, Plan 03) is responsible for canonicalizing the
//! path and rejecting `..` / symlink traversal. See the STRIDE register in
//! 53-02-PLAN.md.

use std::path::Path;
use std::sync::{Arc, OnceLock};

use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};

// ---------------------------------------------------------------------------
// Env var names (re-exported through `crate::config::fjall` for doc/ops use).
// ---------------------------------------------------------------------------

/// `BEAVA_FJALL_FSYNC_MS` — periodic fsync cadence in milliseconds.
/// Default `5`, clamp `[1, 1000]`. `0` is NOT the disable path (T-53-02-03);
/// use `BEAVA_FJALL_FSYNC_DISABLE=1` for tests.
pub const BEAVA_FJALL_FSYNC_MS: &str = "BEAVA_FJALL_FSYNC_MS";
/// `BEAVA_FJALL_FSYNC_DISABLE` — when `1`, disables the fsync thread (tests).
/// Overrides `BEAVA_FJALL_FSYNC_MS`. Logged WARN at startup.
pub const BEAVA_FJALL_FSYNC_DISABLE: &str = "BEAVA_FJALL_FSYNC_DISABLE";
/// `BEAVA_FJALL_CACHE_MB` — unified block+blob cache size (fjall 2.8+).
/// Default `(read_sys_mem_mb() / num_shards / 2).min(512).max(16)` (W-5 — uses
/// the real host memory, NOT an 8192 hardcode). Clamp `[16, 8192]`.
pub const BEAVA_FJALL_CACHE_MB: &str = "BEAVA_FJALL_CACHE_MB";
/// `BEAVA_FJALL_FLUSH_WORKERS` — background memtable→SSTable flush pool.
/// Default `2`, clamp `[1, 8]`.
pub const BEAVA_FJALL_FLUSH_WORKERS: &str = "BEAVA_FJALL_FLUSH_WORKERS";
/// `BEAVA_FJALL_COMPACTION_WORKERS` — background compaction pool.
/// Default `2`, clamp `[1, 8]`.
pub const BEAVA_FJALL_COMPACTION_WORKERS: &str = "BEAVA_FJALL_COMPACTION_WORKERS";
/// `BEAVA_FJALL_BLOCK_SIZE` — per-partition SSTable block size in bytes.
/// Default `4096`, clamp `[1024, 65536]`. Plan 01 spike confirmed
/// postcard p95 = 64 B, well under the 4 KiB default — no bump needed.
pub const BEAVA_FJALL_BLOCK_SIZE: &str = "BEAVA_FJALL_BLOCK_SIZE";
/// `BEAVA_FJALL_MAX_MEMTABLE_MB` — per-partition memtable size in MiB.
/// Default `16`, clamp `[1, 512]`.
pub const BEAVA_FJALL_MAX_MEMTABLE_MB: &str = "BEAVA_FJALL_MAX_MEMTABLE_MB";

// ---------------------------------------------------------------------------
// FjallConfig
// ---------------------------------------------------------------------------

/// Clamped fjall configuration, produced by `fjall_config_from_env`.
///
/// All fields are pre-clamped to the ranges documented in
/// 53-RESEARCH.md §BEAVA_FJALL_* Environment Variables. No additional
/// clamping happens at open-time.
#[derive(Debug, Clone)]
pub struct FjallConfig {
    /// Background-fsync cadence in milliseconds. `None` disables the thread.
    /// Tests only (enabled via `BEAVA_FJALL_FSYNC_DISABLE=1`).
    pub fsync_ms: Option<u16>,
    /// Unified cache size in MiB (fjall 2.8+ `cache_size()`).
    pub cache_mb: u64,
    /// Background flush worker pool size.
    pub flush_workers: usize,
    /// Background compaction worker pool size.
    pub compaction_workers: usize,
    /// Per-partition SSTable block size in bytes.
    pub block_size: u32,
    /// Per-partition memtable size in MiB.
    pub max_memtable_mb: u32,
}

// ---------------------------------------------------------------------------
// read_sys_mem_mb — W-5 revision (real sysinfo-driven host-mem read)
// ---------------------------------------------------------------------------

/// Read total host memory in MiB via the `sysinfo` crate.
///
/// Cached process-wide behind a `OnceLock` so repeated calls during startup
/// incur exactly one syscall. Replaces the prior W-5 hardcode fallback
/// (an `8192` literal assigned to a local named `sys_mem_mb`) — see plan
/// 53-02 §W-5 revision and the authoritative sysinfo example in 53-RESEARCH.md.
///
/// A floor of 1 GiB (1024 MiB) guards against degenerate returns (exotic
/// containers, stub builds) so the `(sys_mem/N/2)` default never collapses
/// to a sub-16 MiB cache that would then be clamped. Every real dev box
/// or CI runner has ≥ 1 GiB RAM.
///
/// # Trust boundary (T-53-02-05)
///
/// `sysinfo::System::total_memory` reads only the host-reported physical
/// memory scalar. No process enumeration, no PII.
pub fn read_sys_mem_mb() -> u64 {
    static CACHED: OnceLock<u64> = OnceLock::new();
    *CACHED.get_or_init(|| {
        use sysinfo::{MemoryRefreshKind, RefreshKind, System};
        let sys = System::new_with_specifics(
            RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
        );
        // sysinfo 0.30+ returns bytes. 1 MiB = 1024 * 1024 bytes.
        let mb = sys.total_memory() / 1024 / 1024;
        mb.max(1024)
    })
}

// ---------------------------------------------------------------------------
// warn-once helpers
// ---------------------------------------------------------------------------

/// Emit a single WARN line per env var per process. Idempotent; `Once` guards
/// the print so repeated clamps in the same process don't spam logs.
fn warn_once(var: &'static str, msg: &str) {
    use std::collections::HashMap;
    use std::sync::Mutex;
    static CELLS: OnceLock<Mutex<HashMap<&'static str, std::sync::Once>>> = OnceLock::new();
    let cells = CELLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cells.lock().expect("warn_once mutex poisoned");
    let once = guard.entry(var).or_insert_with(std::sync::Once::new);
    once.call_once(|| {
        eprintln!("[WARN] {}: {}", var, msg);
    });
}

/// Parse an env var as `T`, apply `clamp`, or fall back to `default` with a
/// `warn_once` log line. Generic over u16/u32/u64/usize.
fn read_clamped<T>(var: &'static str, default: T, lo: T, hi: T) -> T
where
    T: Copy + PartialOrd + std::str::FromStr + std::fmt::Display,
{
    match std::env::var(var) {
        Err(_) => default,
        Ok(s) => match s.parse::<T>() {
            Ok(v) => {
                if v < lo {
                    warn_once(
                        var,
                        &format!("value {} below minimum {}; clamping up", v, lo),
                    );
                    lo
                } else if v > hi {
                    warn_once(
                        var,
                        &format!("value {} above maximum {}; clamping down", v, hi),
                    );
                    hi
                } else {
                    v
                }
            }
            Err(_) => {
                warn_once(
                    var,
                    &format!("unparseable value {:?}; using default {}", s, default),
                );
                default
            }
        },
    }
}

// ---------------------------------------------------------------------------
// fjall_config_from_env
// ---------------------------------------------------------------------------

/// Build a clamped `FjallConfig` from `BEAVA_FJALL_*` env vars.
///
/// Default values, clamp ranges, and the `cache_mb` formula are drawn from
/// 53-RESEARCH.md §BEAVA_FJALL_* Environment Variables (authoritative). The
/// `num_shards` argument feeds the `cache_mb` default formula.
pub fn fjall_config_from_env(num_shards: u16) -> FjallConfig {
    // fsync_ms: default 5, clamp [1, 1000]. FSYNC_DISABLE=1 wins.
    let fsync_disable = std::env::var(BEAVA_FJALL_FSYNC_DISABLE).ok().as_deref() == Some("1");
    let fsync_ms = if fsync_disable {
        None
    } else {
        Some(read_clamped::<u16>(BEAVA_FJALL_FSYNC_MS, 5, 1, 1000))
    };

    // cache_mb: W-5 revision — default formula uses REAL host memory.
    // Formula: (read_sys_mem_mb() / num_shards / 2).min(512).max(16)
    // Clamp range when env is present: [16, 8192].
    let num_shards_u64 = num_shards.max(1) as u64;
    let default_cache_mb = (read_sys_mem_mb() / num_shards_u64 / 2).min(512).max(16);
    let cache_mb = match std::env::var(BEAVA_FJALL_CACHE_MB) {
        Err(_) => default_cache_mb,
        Ok(s) => match s.parse::<u64>() {
            Ok(v) => {
                if v < 16 {
                    warn_once(
                        BEAVA_FJALL_CACHE_MB,
                        &format!("value {} below minimum 16; clamping up", v),
                    );
                    16
                } else if v > 8192 {
                    warn_once(
                        BEAVA_FJALL_CACHE_MB,
                        &format!("value {} above maximum 8192; clamping down", v),
                    );
                    8192
                } else {
                    v
                }
            }
            Err(_) => {
                warn_once(
                    BEAVA_FJALL_CACHE_MB,
                    &format!(
                        "unparseable value {:?}; using sys-mem-scaled default {}",
                        s, default_cache_mb
                    ),
                );
                default_cache_mb
            }
        },
    };

    let flush_workers = read_clamped::<usize>(BEAVA_FJALL_FLUSH_WORKERS, 2, 1, 8);
    let compaction_workers = read_clamped::<usize>(BEAVA_FJALL_COMPACTION_WORKERS, 2, 1, 8);
    let block_size = read_clamped::<u32>(BEAVA_FJALL_BLOCK_SIZE, 4096, 1024, 65536);
    let max_memtable_mb = read_clamped::<u32>(BEAVA_FJALL_MAX_MEMTABLE_MB, 16, 1, 512);

    FjallConfig {
        fsync_ms,
        cache_mb,
        flush_workers,
        compaction_workers,
        block_size,
        max_memtable_mb,
    }
}

// ---------------------------------------------------------------------------
// open_keyspace_from_env / open_shard_partition
// ---------------------------------------------------------------------------

/// Open (or create) the single fjall keyspace at `data_dir/fjall/`.
///
/// Wraps the returned `Keyspace` in `Arc` so every shard thread can hold a
/// cheap clone. `PartitionHandle`s opened against this keyspace are also
/// `Clone + Send + Sync`.
///
/// # Errors
///
/// Returns `fjall::Error` on IO failure during journal recovery or SSTable
/// open. The error is surfaced unchanged so the caller (Plan 03 boot path)
/// can decide whether to retry, rebuild, or hard-fail.
pub fn open_keyspace_from_env(
    data_dir: &Path,
    cfg: &FjallConfig,
) -> fjall::Result<Arc<Keyspace>> {
    let mut ks_cfg = Config::new(data_dir.join("fjall"))
        .cache_size(cfg.cache_mb.saturating_mul(1024 * 1024))
        .flush_workers(cfg.flush_workers)
        .compaction_workers(cfg.compaction_workers);
    // fjall's `fsync_ms(Option<u16>)` panics if `Some(0)` — our clamp already
    // enforces `>= 1`, so this path is safe. `None` disables the thread.
    ks_cfg = ks_cfg.fsync_ms(cfg.fsync_ms);
    Ok(Arc::new(ks_cfg.open()?))
}

/// Open (or create) the partition for shard `shard_index` inside `ks`.
///
/// Partition name is always `shard-{shard_index}` — the single D-01 layout.
/// Compaction strategy is left at the fjall default (Leveled) per
/// 53-RESEARCH §Pattern 1. Block size and memtable size come from `cfg`.
///
/// # Errors
///
/// Returns `fjall::Error` on IO failure.
pub fn open_shard_partition(
    ks: &Keyspace,
    shard_index: usize,
    cfg: &FjallConfig,
) -> fjall::Result<PartitionHandle> {
    let opts = PartitionCreateOptions::default()
        .block_size(cfg.block_size)
        .max_memtable_size(cfg.max_memtable_mb.saturating_mul(1024 * 1024));
    ks.open_partition(&format!("shard-{}", shard_index), opts)
}

// ---------------------------------------------------------------------------
// Unit tests — Tests 3–7 from 53-02-PLAN.md moved here from
// tests/fjall_backend_env_tests.rs (idiomatic Rust: env tests live next to
// the module they test).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    /// Process-global lock for env-mutating tests. Parallel test runners MUST
    /// take this lock before touching any `BEAVA_FJALL_*` env var, or they'll
    /// race with each other through `std::env::set_var`.
    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn clear_fjall_env() {
        for k in [
            BEAVA_FJALL_FSYNC_MS,
            BEAVA_FJALL_FSYNC_DISABLE,
            BEAVA_FJALL_CACHE_MB,
            BEAVA_FJALL_FLUSH_WORKERS,
            BEAVA_FJALL_COMPACTION_WORKERS,
            BEAVA_FJALL_BLOCK_SIZE,
            BEAVA_FJALL_MAX_MEMTABLE_MB,
        ] {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn env_clamp_fsync_ms_out_of_range_logs_and_clamps() {
        let _g = env_lock().lock().unwrap();
        clear_fjall_env();

        std::env::set_var(BEAVA_FJALL_FSYNC_MS, "0");
        let cfg = fjall_config_from_env(1);
        assert_eq!(cfg.fsync_ms, Some(1), "0 must clamp up to 1");

        std::env::set_var(BEAVA_FJALL_FSYNC_MS, "2000");
        let cfg = fjall_config_from_env(1);
        assert_eq!(cfg.fsync_ms, Some(1000), "2000 must clamp down to 1000");

        std::env::set_var(BEAVA_FJALL_FSYNC_MS, "abc");
        let cfg = fjall_config_from_env(1);
        assert_eq!(cfg.fsync_ms, Some(5), "non-numeric must fall back to default 5");

        clear_fjall_env();
    }

    #[test]
    fn env_clamp_fsync_disable_overrides_fsync_ms() {
        let _g = env_lock().lock().unwrap();
        clear_fjall_env();

        std::env::set_var(BEAVA_FJALL_FSYNC_MS, "10");
        std::env::set_var(BEAVA_FJALL_FSYNC_DISABLE, "1");
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

        let cfg = fjall_config_from_env(8);
        let sys_mem_mb = read_sys_mem_mb();
        let expected = (sys_mem_mb / 8 / 2).min(512).max(16);
        assert_eq!(
            cfg.cache_mb, expected,
            "cache_mb default must follow (sys_mem/N/2).min(512).max(16); sys_mem={} expected={} got={}",
            sys_mem_mb, expected, cfg.cache_mb
        );
        assert!(cfg.cache_mb <= 512, "cache_mb must be <= 512");
        assert!(cfg.cache_mb >= 16, "cache_mb must be >= 16");
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

        std::env::set_var(BEAVA_FJALL_BLOCK_SIZE, "100");
        let cfg = fjall_config_from_env(1);
        assert_eq!(cfg.block_size, 1024, "100 must clamp up to 1024");

        std::env::set_var(BEAVA_FJALL_BLOCK_SIZE, "1000000");
        let cfg = fjall_config_from_env(1);
        assert_eq!(cfg.block_size, 65536, "1_000_000 must clamp down to 65536");

        clear_fjall_env();
    }

    #[test]
    fn read_sys_mem_mb_returns_nonzero_and_cached() {
        let a = read_sys_mem_mb();
        assert!(
            a >= 1024,
            "read_sys_mem_mb must enforce the >= 1024 MiB floor; got {}",
            a
        );
        let b = read_sys_mem_mb();
        assert_eq!(a, b, "OnceLock cache must return identical value on repeated calls");
    }
}
