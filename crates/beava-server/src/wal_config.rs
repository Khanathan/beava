//! WAL config with env-var tunables — Phase 19.1-03 (D-01, D-02, D-03).
//!
//! Default config: **4 buffers x 32 MiB, tick = 20 ms** (~128 MiB resident).
//!
//! Three env vars override the defaults at startup; values out-of-range clamp
//! to documented safe limits with a WARN log so operators see what actually
//! got applied. Parse failures (e.g. `BEAVA_WAL_BUFFERS=xyz`) fall back to the
//! default with a WARN log instead of refusing to start — operators often
//! inherit env vars from shell scripts they don't fully control.
//!
//! | Env var                    | Type   | Default | Range       |
//! | -------------------------- | ------ | ------- | ----------- |
//! | `BEAVA_WAL_BUFFERS`        | usize  | 4       | [2, 32]     |
//! | `BEAVA_WAL_BUFFER_SIZE_MB` | usize  | 32      | [4, 256]    |
//! | `BEAVA_WAL_TICK_MS`        | u64    | 20      | [1, 1000]   |
//!
//! ## Phase 18 WAL architecture invariants — UNCHANGED
//!
//! Per `~/.claude/projects/-Users-petrpan26-work-tally/memory/project_phase18_wal_architecture.md`,
//! these invariants are NOT touched by this module:
//!
//! - Lock-free apply path (single writer to active buffer, no Mutex).
//! - Multi-buffer state machine (active / sealed / flushing / free) — now
//!   defaults to 4 active slots; the algorithm is identical.
//! - Single writer + fsync thread.
//! - Four-watermark LSN discipline (committed / written / synced / acked).
//! - `O_APPEND` on the WAL file.
//! - Refuse-on-network-FS check at startup (lives elsewhere in the stack).
//!
//! Phase 19.1-03 only changes COUNT, SIZE, and TICK INTERVAL of buffers.

#[derive(Debug, Clone, Copy)]
pub struct WalConfig {
    pub buffers: usize,
    pub buffer_size_mb: usize,
    pub tick_ms: u64,
}

impl WalConfig {
    pub const DEFAULT_BUFFERS: usize = 4;
    pub const DEFAULT_BUFFER_SIZE_MB: usize = 32;
    pub const DEFAULT_TICK_MS: u64 = 20;

    pub const BUFFERS_MIN: usize = 2;
    pub const BUFFERS_MAX: usize = 32;
    pub const BUFFER_SIZE_MB_MIN: usize = 4;
    pub const BUFFER_SIZE_MB_MAX: usize = 256;
    pub const TICK_MS_MIN: u64 = 1;
    pub const TICK_MS_MAX: u64 = 1000;

    /// Read env vars, parse, and clamp to safe ranges. Falls back to the
    /// documented defaults on missing / unparseable / out-of-range values.
    pub fn resolve_from_env() -> Self {
        let buffers = parse_clamp_usize(
            "BEAVA_WAL_BUFFERS",
            Self::DEFAULT_BUFFERS,
            Self::BUFFERS_MIN,
            Self::BUFFERS_MAX,
        );
        let buffer_size_mb = parse_clamp_usize(
            "BEAVA_WAL_BUFFER_SIZE_MB",
            Self::DEFAULT_BUFFER_SIZE_MB,
            Self::BUFFER_SIZE_MB_MIN,
            Self::BUFFER_SIZE_MB_MAX,
        );
        let tick_ms = parse_clamp_u64(
            "BEAVA_WAL_TICK_MS",
            Self::DEFAULT_TICK_MS,
            Self::TICK_MS_MIN,
            Self::TICK_MS_MAX,
        );
        WalConfig {
            buffers,
            buffer_size_mb,
            tick_ms,
        }
    }
}

fn parse_clamp_usize(name: &str, default: usize, lo: usize, hi: usize) -> usize {
    match std::env::var(name) {
        Ok(s) => match s.parse::<usize>() {
            Ok(v) => {
                let clamped = v.clamp(lo, hi);
                if clamped != v {
                    tracing::warn!(
                        target: "beava.wal",
                        kind = "wal.config.clamp",
                        env_var = %name,
                        requested = v,
                        clamped = clamped,
                        range_lo = lo,
                        range_hi = hi,
                        "WAL env var clamped to safe range"
                    );
                }
                clamped
            }
            Err(e) => {
                tracing::warn!(
                    target: "beava.wal",
                    kind = "wal.config.parse_error",
                    env_var = %name,
                    value = %s,
                    error = %e,
                    default = default,
                    "WAL env var parse failed; falling back to default"
                );
                default
            }
        },
        Err(_) => default,
    }
}

fn parse_clamp_u64(name: &str, default: u64, lo: u64, hi: u64) -> u64 {
    match std::env::var(name) {
        Ok(s) => match s.parse::<u64>() {
            Ok(v) => {
                let clamped = v.clamp(lo, hi);
                if clamped != v {
                    tracing::warn!(
                        target: "beava.wal",
                        kind = "wal.config.clamp",
                        env_var = %name,
                        requested = v,
                        clamped = clamped,
                        range_lo = lo,
                        range_hi = hi,
                        "WAL env var clamped to safe range"
                    );
                }
                clamped
            }
            Err(e) => {
                tracing::warn!(
                    target: "beava.wal",
                    kind = "wal.config.parse_error",
                    env_var = %name,
                    value = %s,
                    error = %e,
                    default = default,
                    "WAL env var parse failed; falling back to default"
                );
                default
            }
        },
        Err(_) => default,
    }
}
