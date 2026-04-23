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

    apply_env_overrides(&mut cfg);
    validate(&cfg)?;
    Ok(cfg)
}

fn apply_env_overrides(cfg: &mut Config) {
    if let Ok(v) = std::env::var("BEAVA_LISTEN_ADDR") {
        cfg.listen_addr = v;
    }
    if let Ok(v) = std::env::var("BEAVA_LOG_LEVEL") {
        cfg.log_level = v;
    }
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
}
