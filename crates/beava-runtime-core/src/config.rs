//! Runtime I/O configuration.
//!
//! `IoConfig` controls how many I/O threads the `IoPool` spawns. Setting
//! `io_threads = 0` means reads are done inline on the apply thread (no pool).

/// Configuration for the I/O thread pool.
///
/// Mirrors Redis's `io-threads` config option (default: `num_cpus - 1`).
/// Setting `io_threads = 0` disables the pool — reads are done inline on the
/// apply thread. This is the correct default when num_cpus() == 1.
#[derive(Debug, Clone)]
pub struct IoConfig {
    /// Number of I/O threads to spawn.
    ///
    /// Default: `num_cpus::get().saturating_sub(1).max(1)`.
    /// Set to 0 to disable threaded I/O (inline mode).
    pub io_threads: usize,
}

impl Default for IoConfig {
    fn default() -> Self {
        // Reserve 1 core for the apply thread; at least 1 I/O thread on SMP.
        // On a single-core machine this becomes 0 (inline mode).
        let n = num_cpus::get().saturating_sub(1);
        Self { io_threads: n }
    }
}

impl IoConfig {
    /// Construct with an explicit thread count. Use 0 for inline mode.
    pub fn new(io_threads: usize) -> Self {
        Self { io_threads }
    }

    /// True when I/O threads are disabled (inline mode on apply thread).
    pub fn is_inline(&self) -> bool {
        self.io_threads == 0
    }
}
