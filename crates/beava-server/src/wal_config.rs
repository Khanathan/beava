//! Stub for WalConfig — Plan 19.1-03 Task 3.b implements the real
//! `resolve_from_env`. This stub exists so the red tests in
//! `tests/wal_env_var_tunables.rs` link and run, but fail at the
//! assertion level (clearer red signal than a compile error).

#[derive(Debug, Clone, Copy)]
pub struct WalConfig {
    pub buffers: usize,
    pub buffer_size_mb: usize,
    pub tick_ms: u64,
}

impl WalConfig {
    /// Stub — returns 0/0/0 so the tests asserting defaults FAIL with a
    /// clear assertion (not a compile error).
    pub fn resolve_from_env() -> Self {
        WalConfig {
            buffers: 0,
            buffer_size_mb: 0,
            tick_ms: 0,
        }
    }
}
