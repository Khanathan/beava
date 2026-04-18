//! BEAVA_SHARDS configuration surface (v1.2 TPC Wave 1 — D-10/D-11).
//!
//! Resolution order (env wins over CLI, consistent with all BEAVA_* vars):
//! 1. `BEAVA_SHARDS` env var — always wins if present and valid
//! 2. `--shards <N>` CLI flag — used when env is absent
//! 3. Default: 1 on debug builds (`cfg(debug_assertions)`), `num_cpus::get_physical()` on release
//!
//! Phase 50.5: the Wave-1 clamp (`wave1_enforced()`) has been removed. The
//! resolved `BEAVA_SHARDS` value now flows through unclamped — callers read
//! `ShardConfig.count` directly.

/// Resolved shard count. Valid range: 1..=256.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardConfig {
    pub count: u16,
}

/// Resolve the shard count from environment + CLI arguments.
///
/// `cli_shards`: the value of `--shards <N>` from `arg_value("shards")`, if present.
/// Returns a `ShardConfig` with the resolved count (before Wave 1 enforcement).
pub fn resolve_shard_count(cli_shards: Option<&str>) -> ShardConfig {
    resolve_shard_count_with_env(std::env::var("BEAVA_SHARDS").ok().as_deref(), cli_shards)
}

/// Internal resolver — accepts an explicit env value for testability.
/// Avoids races when tests mutate the process-global `BEAVA_SHARDS` env var in parallel.
pub(crate) fn resolve_shard_count_with_env(
    env_shards: Option<&str>,
    cli_shards: Option<&str>,
) -> ShardConfig {
    let default_count: u16 = if cfg!(debug_assertions) {
        1
    } else {
        num_cpus::get_physical().min(256).max(1) as u16
    };

    // Env wins over CLI.
    let raw = env_shards
        .map(str::to_string)
        .or_else(|| cli_shards.map(str::to_string));

    let count = match raw {
        None => default_count,
        Some(s) => match s.parse::<u16>() {
            Ok(n) if n >= 1 && n <= 256 => n,
            Ok(_) => {
                eprintln!(
                    "[WARN] BEAVA_SHARDS value out of range 1..=256 (got {:?}); using default {}",
                    s, default_count
                );
                default_count
            }
            Err(_) => {
                eprintln!(
                    "[WARN] BEAVA_SHARDS is not a valid u16 (got {:?}); using default {}",
                    s, default_count
                );
                default_count
            }
        },
    };

    ShardConfig { count }
}

#[cfg(test)]
mod tests {
    use super::*;

    // All env-dependent tests use resolve_shard_count_with_env() to avoid
    // process-global BEAVA_SHARDS races when tests run in parallel.

    #[test]
    fn no_env_no_cli_debug_returns_1() {
        // In debug builds (cfg(debug_assertions)), the default must be 1.
        #[cfg(debug_assertions)]
        {
            let cfg = resolve_shard_count_with_env(None, None);
            assert_eq!(cfg.count, 1, "debug default must be 1");
        }
    }

    #[test]
    fn env_wins_over_cli() {
        let cfg = resolve_shard_count_with_env(Some("4"), Some("8"));
        assert_eq!(cfg.count, 4, "env BEAVA_SHARDS=4 wins over --shards 8");
    }

    #[test]
    fn cli_used_when_env_absent() {
        let cfg = resolve_shard_count_with_env(None, Some("3"));
        assert_eq!(cfg.count, 3, "--shards 3 used when BEAVA_SHARDS not set");
    }

    #[test]
    fn clamp_zero_to_default() {
        let cfg = resolve_shard_count_with_env(Some("0"), None);
        // 0 is out-of-range; must return default (1 in debug).
        assert!(cfg.count >= 1, "out-of-range 0 clamps to at least 1");
    }

    #[test]
    fn clamp_above_256_to_default() {
        let cfg = resolve_shard_count_with_env(Some("257"), None);
        assert!(cfg.count >= 1, "out-of-range 257 clamps to default");
    }

    #[test]
    fn invalid_string_falls_back_to_default() {
        let cfg = resolve_shard_count_with_env(Some("banana"), None);
        assert!(cfg.count >= 1, "non-numeric value uses default");
    }

    #[test]
    fn resolved_count_flows_through_unclamped() {
        // Phase 50.5: Wave-1 clamp removed; BEAVA_SHARDS value is honored as-is.
        let cfg = resolve_shard_count_with_env(Some("8"), None);
        assert_eq!(cfg.count, 8, "BEAVA_SHARDS=8 must flow through unclamped");
    }

    #[test]
    fn resolved_count_one_passes_through() {
        let cfg = resolve_shard_count_with_env(Some("1"), None);
        assert_eq!(cfg.count, 1, "N=1 passes through unchanged");
    }
}
