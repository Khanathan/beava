//! Phase 6 Plan 04 crash probe binary.
//!
//! Spawned by `crates/beava-server/tests/phase6_crash.rs` as a subprocess.
//! Reads BEAVA_WAL_DIR + BEAVA_WAL_FSYNC_INTERVAL_MS from env, starts a minimal
//! beava server on an ephemeral port with a single "Test" event registered,
//! prints `PORT=<n>` to stdout, then blocks until SIGKILL.
//!
//! Task 4b lands the real implementation; task 4a commits this placeholder so
//! the crash tests fail deterministically (RED).

fn main() {
    panic!("phase6_crash_probe not implemented — task 4b will land the real binary");
}
