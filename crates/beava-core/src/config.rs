//! Server configuration — Phase 1 shape.
//!
//! Minimal Config for Phase 1: only the knobs Plan 03 (logging) and Plan 04 (HTTP)
//! consume. Later phases extend this struct additively.
//!
//! Loading order (later sources override earlier):
//! 1. Defaults baked into `Config::default()`
//! 2. YAML file at the path passed to `load_config`
//! 3. Environment variables with `BEAVA_*` prefix
//!
//! Env vars recognized in Phase 1:
//! - `BEAVA_LISTEN_ADDR` → `listen_addr`
//! - `BEAVA_LOG_LEVEL`   → `log_level`

use crate::defaults::{DEFAULT_TCP_HOST, DEFAULT_TCP_MAX_FRAME_BYTES, DEFAULT_TCP_PORT};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Server configuration, Phase 1 shape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Address:port to bind the HTTP server to (e.g. "127.0.0.1:8080").
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    /// Log level for tracing filter (trace|debug|info|warn|error).
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// TCP wire listener config (Phase 2.5 D-06). Enabled by default alongside HTTP.
    #[serde(default)]
    pub tcp: TcpConfig,
}

fn default_listen_addr() -> String {
    "127.0.0.1:8080".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            log_level: default_log_level(),
            tcp: TcpConfig::default(),
        }
    }
}

// ─── TcpConfig (Phase 2.5) ────────────────────────────────────────────────────

/// TCP binary-framed wire listener configuration (Phase 2.5 D-06).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TcpConfig {
    #[serde(default = "default_tcp_enabled")]
    pub enabled: bool,
    #[serde(default = "default_tcp_host")]
    pub host: String,
    #[serde(default = "default_tcp_port")]
    pub port: u16,
    #[serde(default = "default_tcp_max_frame_bytes")]
    pub max_frame_bytes: u32,
}

fn default_tcp_enabled() -> bool {
    true
}
fn default_tcp_host() -> String {
    DEFAULT_TCP_HOST.to_string()
}
fn default_tcp_port() -> u16 {
    DEFAULT_TCP_PORT
}
fn default_tcp_max_frame_bytes() -> u32 {
    DEFAULT_TCP_MAX_FRAME_BYTES
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            enabled: default_tcp_enabled(),
            host: default_tcp_host(),
            port: default_tcp_port(),
            max_frame_bytes: default_tcp_max_frame_bytes(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config file not found: {0}")]
    FileNotFound(PathBuf),
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse YAML config {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("invalid config field `{field}`: {reason}")]
    Validation { field: &'static str, reason: String },
}

/// Load a Config from the given YAML file path, apply env-var overrides, then validate.
///
/// Env-var overrides follow the `BEAVA_*` prefix convention. Only string overrides for
/// Phase 1 — later phases may introduce typed (numeric, bool) overrides as needed.
pub fn load_config<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
    let path = path.as_ref();
    if !path.exists() {
        return Err(ConfigError::FileNotFound(path.to_path_buf()));
    }
    let raw = std::fs::read_to_string(path).map_err(|e| ConfigError::Read {
        path: path.to_path_buf(),
        source: e,
    })?;
    let mut cfg: Config = serde_yaml::from_str(&raw).map_err(|e| ConfigError::Parse {
        path: path.to_path_buf(),
        source: e,
    })?;

    apply_env_overrides(&mut cfg)?;
    validate(&cfg)?;
    Ok(cfg)
}

fn apply_env_overrides(cfg: &mut Config) -> Result<(), ConfigError> {
    if let Ok(v) = std::env::var("BEAVA_LISTEN_ADDR") {
        cfg.listen_addr = v;
    }
    if let Ok(v) = std::env::var("BEAVA_LOG_LEVEL") {
        cfg.log_level = v;
    }
    if let Ok(v) = std::env::var("BEAVA_TCP_ENABLED") {
        // Accept "0"/"1"/"true"/"false" (case-insensitive)
        cfg.tcp.enabled = matches!(v.to_ascii_lowercase().as_str(), "1" | "true");
    }
    if let Ok(v) = std::env::var("BEAVA_TCP_HOST") {
        cfg.tcp.host = v;
    }
    if let Ok(v) = std::env::var("BEAVA_TCP_PORT") {
        cfg.tcp.port = v
            .parse()
            .map_err(|e: std::num::ParseIntError| ConfigError::Validation {
                field: "tcp.port",
                reason: format!("BEAVA_TCP_PORT=`{}`: {}", v, e),
            })?;
    }
    if let Ok(v) = std::env::var("BEAVA_TCP_MAX_FRAME_BYTES") {
        cfg.tcp.max_frame_bytes =
            v.parse()
                .map_err(|e: std::num::ParseIntError| ConfigError::Validation {
                    field: "tcp.max_frame_bytes",
                    reason: format!("BEAVA_TCP_MAX_FRAME_BYTES=`{}`: {}", v, e),
                })?;
    }
    Ok(())
}

fn validate(cfg: &Config) -> Result<(), ConfigError> {
    // Validate listen_addr parses as a SocketAddr.
    cfg.listen_addr
        .parse::<std::net::SocketAddr>()
        .map_err(|e| ConfigError::Validation {
            field: "listen_addr",
            reason: format!("`{}` is not a valid socket address: {}", cfg.listen_addr, e),
        })?;

    // Validate log_level is one of the known tracing levels.
    match cfg.log_level.to_ascii_lowercase().as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        other => Err(ConfigError::Validation {
            field: "log_level",
            reason: format!("`{}` is not one of: trace|debug|info|warn|error", other),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Mutex, MutexGuard};
    use tempfile::NamedTempFile;

    /// Process-global mutex serializing all env-var-touching tests.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn write_yaml(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("create temp file");
        f.write_all(content.as_bytes()).expect("write temp");
        f
    }

    /// Helper to isolate env-var writes per test. Holds the global lock while live.
    struct EnvGuard<'a> {
        vars: Vec<(&'static str, Option<String>)>,
        _lock: MutexGuard<'a, ()>,
    }
    impl<'a> EnvGuard<'a> {
        fn set(keys: &[&'static str]) -> Self {
            let lock = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
            let vars = keys.iter().map(|&k| (k, std::env::var(k).ok())).collect();
            for &k in keys {
                std::env::remove_var(k);
            }
            EnvGuard { vars, _lock: lock }
        }
    }
    impl Drop for EnvGuard<'_> {
        fn drop(&mut self) {
            for (k, v) in &self.vars {
                match v {
                    Some(val) => std::env::set_var(k, val),
                    None => std::env::remove_var(k),
                }
            }
        }
    }

    #[test]
    fn parses_minimal_yaml() {
        let _guard = EnvGuard::set(&["BEAVA_LISTEN_ADDR", "BEAVA_LOG_LEVEL"]);
        let f = write_yaml("listen_addr: \"0.0.0.0:9000\"\nlog_level: debug\n");
        let cfg = load_config(f.path()).expect("load ok");
        assert_eq!(cfg.listen_addr, "0.0.0.0:9000");
        assert_eq!(cfg.log_level, "debug");
    }

    #[test]
    fn missing_file_returns_file_not_found() {
        let _guard = EnvGuard::set(&["BEAVA_LISTEN_ADDR", "BEAVA_LOG_LEVEL"]);
        let err = load_config("/nonexistent/path/to/beava.yaml").unwrap_err();
        assert!(matches!(err, ConfigError::FileNotFound(_)));
    }

    #[test]
    fn malformed_yaml_returns_parse_error() {
        let _guard = EnvGuard::set(&["BEAVA_LISTEN_ADDR", "BEAVA_LOG_LEVEL"]);
        let f = write_yaml("listen_addr: [not a string\n");
        let err = load_config(f.path()).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }));
    }

    #[test]
    fn env_var_overrides_listen_addr() {
        let _guard = EnvGuard::set(&["BEAVA_LISTEN_ADDR", "BEAVA_LOG_LEVEL"]);
        std::env::set_var("BEAVA_LISTEN_ADDR", "127.0.0.1:9999");
        let f = write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: info\n");
        let cfg = load_config(f.path()).expect("load ok");
        assert_eq!(cfg.listen_addr, "127.0.0.1:9999");
    }

    #[test]
    fn env_var_overrides_log_level() {
        let _guard = EnvGuard::set(&["BEAVA_LISTEN_ADDR", "BEAVA_LOG_LEVEL"]);
        std::env::set_var("BEAVA_LOG_LEVEL", "trace");
        let f = write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: info\n");
        let cfg = load_config(f.path()).expect("load ok");
        assert_eq!(cfg.log_level, "trace");
    }

    #[test]
    fn invalid_listen_addr_fails_validation() {
        let _guard = EnvGuard::set(&["BEAVA_LISTEN_ADDR", "BEAVA_LOG_LEVEL"]);
        let f = write_yaml("listen_addr: \"not-a-socket-addr\"\nlog_level: info\n");
        let err = load_config(f.path()).unwrap_err();
        match err {
            ConfigError::Validation { field, .. } => assert_eq!(field, "listen_addr"),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn unknown_log_level_fails_validation() {
        let _guard = EnvGuard::set(&["BEAVA_LISTEN_ADDR", "BEAVA_LOG_LEVEL"]);
        let f = write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: shouty\n");
        let err = load_config(f.path()).unwrap_err();
        match err {
            ConfigError::Validation { field, .. } => assert_eq!(field, "log_level"),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    // ─── Phase 2.5 TcpConfig tests ────────────────────────────────────────────

    const TCP_ENV_KEYS: &[&str] = &[
        "BEAVA_LISTEN_ADDR",
        "BEAVA_LOG_LEVEL",
        "BEAVA_TCP_ENABLED",
        "BEAVA_TCP_HOST",
        "BEAVA_TCP_PORT",
        "BEAVA_TCP_MAX_FRAME_BYTES",
    ];

    #[test]
    fn tcp_config_default_matches_constants() {
        let t = TcpConfig::default();
        assert!(t.enabled);
        assert_eq!(t.host, "127.0.0.1");
        assert_eq!(t.port, 7380);
        assert_eq!(t.max_frame_bytes, 4 * 1024 * 1024);
    }

    #[test]
    fn config_default_includes_tcp_block() {
        let c = Config::default();
        assert_eq!(c.tcp, TcpConfig::default());
    }

    #[test]
    fn yaml_without_tcp_block_uses_defaults() {
        let _guard = EnvGuard::set(TCP_ENV_KEYS);
        let f = write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: info\n");
        let cfg = load_config(f.path()).expect("load ok");
        assert_eq!(cfg.tcp, TcpConfig::default());
    }

    #[test]
    fn yaml_with_partial_tcp_block_fills_defaults() {
        let _guard = EnvGuard::set(TCP_ENV_KEYS);
        let f =
            write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: info\ntcp:\n  port: 9999\n");
        let cfg = load_config(f.path()).expect("load ok");
        assert_eq!(cfg.tcp.port, 9999);
        assert!(cfg.tcp.enabled);
        assert_eq!(cfg.tcp.host, "127.0.0.1");
        assert_eq!(cfg.tcp.max_frame_bytes, 4 * 1024 * 1024);
    }

    #[test]
    fn yaml_with_full_tcp_block_round_trips() {
        let _guard = EnvGuard::set(TCP_ENV_KEYS);
        let f = write_yaml(
            "listen_addr: \"127.0.0.1:8080\"\n\
             log_level: info\n\
             tcp:\n  enabled: false\n  host: \"0.0.0.0\"\n  port: 1234\n  max_frame_bytes: 2048\n",
        );
        let cfg = load_config(f.path()).expect("load ok");
        assert!(!cfg.tcp.enabled);
        assert_eq!(cfg.tcp.host, "0.0.0.0");
        assert_eq!(cfg.tcp.port, 1234);
        assert_eq!(cfg.tcp.max_frame_bytes, 2048);
    }

    #[test]
    fn env_var_overrides_tcp_enabled_accepts_0_and_false() {
        let _guard = EnvGuard::set(TCP_ENV_KEYS);
        let f = write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: info\n");

        std::env::set_var("BEAVA_TCP_ENABLED", "0");
        assert!(!load_config(f.path()).unwrap().tcp.enabled);
        std::env::set_var("BEAVA_TCP_ENABLED", "false");
        assert!(!load_config(f.path()).unwrap().tcp.enabled);
        std::env::set_var("BEAVA_TCP_ENABLED", "FALSE");
        assert!(!load_config(f.path()).unwrap().tcp.enabled);
        std::env::set_var("BEAVA_TCP_ENABLED", "1");
        assert!(load_config(f.path()).unwrap().tcp.enabled);
        std::env::set_var("BEAVA_TCP_ENABLED", "true");
        assert!(load_config(f.path()).unwrap().tcp.enabled);
        std::env::set_var("BEAVA_TCP_ENABLED", "TRUE");
        assert!(load_config(f.path()).unwrap().tcp.enabled);
    }

    #[test]
    fn env_var_overrides_tcp_port() {
        let _guard = EnvGuard::set(TCP_ENV_KEYS);
        std::env::set_var("BEAVA_TCP_PORT", "9999");
        let f = write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: info\n");
        let cfg = load_config(f.path()).expect("load ok");
        assert_eq!(cfg.tcp.port, 9999);
    }

    #[test]
    fn env_var_overrides_tcp_host() {
        let _guard = EnvGuard::set(TCP_ENV_KEYS);
        std::env::set_var("BEAVA_TCP_HOST", "0.0.0.0");
        let f = write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: info\n");
        let cfg = load_config(f.path()).expect("load ok");
        assert_eq!(cfg.tcp.host, "0.0.0.0");
    }

    #[test]
    fn env_var_overrides_tcp_max_frame_bytes() {
        let _guard = EnvGuard::set(TCP_ENV_KEYS);
        std::env::set_var("BEAVA_TCP_MAX_FRAME_BYTES", "1048576");
        let f = write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: info\n");
        let cfg = load_config(f.path()).expect("load ok");
        assert_eq!(cfg.tcp.max_frame_bytes, 1_048_576);
    }

    #[test]
    fn env_var_invalid_tcp_port_returns_validation_error() {
        let _guard = EnvGuard::set(TCP_ENV_KEYS);
        std::env::set_var("BEAVA_TCP_PORT", "nope");
        let f = write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: info\n");
        let err = load_config(f.path()).unwrap_err();
        match err {
            ConfigError::Validation { field, .. } => assert_eq!(field, "tcp.port"),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn env_var_invalid_tcp_max_frame_bytes_returns_validation_error() {
        let _guard = EnvGuard::set(TCP_ENV_KEYS);
        std::env::set_var("BEAVA_TCP_MAX_FRAME_BYTES", "huge");
        let f = write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: info\n");
        let err = load_config(f.path()).unwrap_err();
        match err {
            ConfigError::Validation { field, .. } => assert_eq!(field, "tcp.max_frame_bytes"),
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn deny_unknown_fields_in_tcp_block() {
        let _guard = EnvGuard::set(TCP_ENV_KEYS);
        let f =
            write_yaml("listen_addr: \"127.0.0.1:8080\"\nlog_level: info\ntcp:\n  typo_key: 1\n");
        let err = load_config(f.path()).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }));
    }
}
