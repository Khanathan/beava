//! Phase 53-01 spike gate assumption A7: verify that
//! `std::process::Child::kill()` on macOS delivers SIGKILL — NOT SIGTERM.
//!
//! Why this matters: Wave 4 (Plan 53-ish / crash-recovery test) will spawn
//! the beava server, kill it mid-write, and assert the fjall WAL replays
//! the last acknowledged write. If `Child::kill()` actually delivered
//! SIGTERM, the parent would run its `Drop` impls — including `Keyspace::
//! drop` which docs say "tries to persist the journal to disk
//! synchronously". That would let the crash-recovery test pass *even if*
//! fjall's WAL replay is broken, a silent false-green.
//!
//! The only way to be sure is to spawn a child whose Drop writes a
//! canary file, SIGKILL it, and assert the canary is empty.
//!
//! Contract this test asserts:
//!   1. `child.kill()` returns `Ok(())`.
//!   2. `child.wait()` reports the child was terminated by a signal
//!      (`WIFSIGNALED`), and specifically by SIGKILL (signal number 9 on
//!      macOS/Linux) — not by a normal exit, and not by SIGTERM (15).
//!   3. The child's Drop handler did NOT run — verified by the canary
//!      file being empty.
//!
//! If any assertion fails on macOS, Plan 53-02 MUST add the `nix` crate
//! and use `nix::sys::signal::kill(pid, SIGKILL)` directly instead of
//! relying on `Child::kill()`. The SPIKE-RESULTS.md records the
//! outcome for downstream plans.
//!
//! Unix-only; tests the Beava dev box (currently macOS) explicitly.

#![cfg(unix)]

use std::os::unix::process::ExitStatusExt;
use std::process::Command;
use std::time::Duration;

/// Helper: find a Rust toolchain to compile the child binary ad hoc.
/// Avoids depending on a workspace binary, so the test stays self-contained.
fn rustc_path() -> std::path::PathBuf {
    // Use the same rustc the test harness was built with.
    std::path::PathBuf::from(env!("CARGO"))
        .parent()
        .expect("CARGO parent")
        .join("rustc")
}

/// Compile a tiny child program that writes "drop-ran" to a canary path
/// from its Drop impl, then sleeps until killed. Returns the built binary
/// path. Built into the test's tempdir so it's cleaned up automatically.
fn build_child(tmp: &std::path::Path, canary: &std::path::Path) -> std::path::PathBuf {
    let src = tmp.join("child.rs");
    let source = format!(
        r#"
        use std::fs::OpenOptions;
        use std::io::Write;
        use std::thread;
        use std::time::Duration;

        struct DropCanary;
        impl Drop for DropCanary {{
            fn drop(&mut self) {{
                let mut f = OpenOptions::new()
                    .create(true).write(true).truncate(true)
                    .open(r"{canary}").expect("open canary");
                f.write_all(b"drop-ran").expect("write canary");
                let _ = f.sync_all();
            }}
        }}

        fn main() {{
            let _c = DropCanary;
            // Announce readiness to stdout so the parent can synchronise.
            println!("ready");
            // Force stdout flush so the parent's read_line unblocks.
            use std::io::Write as _;
            std::io::stdout().flush().ok();
            // Sleep until killed. 60s is plenty; parent kills within ~100ms.
            thread::sleep(Duration::from_secs(60));
        }}
        "#,
        canary = canary.display()
    );
    std::fs::write(&src, source).expect("write child source");
    let bin = tmp.join("child_bin");
    let status = Command::new(rustc_path())
        .arg("-O")
        .arg(&src)
        .arg("-o")
        .arg(&bin)
        .status()
        .expect("spawn rustc");
    assert!(status.success(), "rustc failed to compile child");
    bin
}

#[test]
fn child_kill_delivers_sigkill_on_unix() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let canary = tmp.path().join("canary.txt");
    // Pre-create canary as empty so presence != populated.
    std::fs::write(&canary, "").expect("seed canary");

    let bin = build_child(tmp.path(), &canary);

    // Spawn and wait for "ready\n".
    let mut child = Command::new(&bin)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("spawn child");
    {
        use std::io::{BufRead, BufReader};
        let stdout = child.stdout.take().expect("stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).expect("read ready line");
        assert_eq!(line.trim(), "ready", "child did not announce readiness");
    }

    // Give the child an extra beat to ensure the DropCanary RAII guard is live.
    std::thread::sleep(Duration::from_millis(50));

    // SIGKILL per std docs: "On Unix, kill() sends SIGKILL."
    child.kill().expect("child.kill()");
    let status = child.wait().expect("child.wait()");

    // Verify signal delivery — not a normal exit.
    assert!(
        !status.success(),
        "child should not have exited successfully after kill"
    );
    let signal = status.signal();
    assert!(
        signal.is_some(),
        "child should have been terminated by a signal (WIFSIGNALED), got status {:?}",
        status
    );
    let sig = signal.unwrap();
    assert_eq!(
        sig,
        libc::SIGKILL,
        "std::process::Child::kill must deliver SIGKILL; got signal {} (SIGTERM={}, SIGINT={})",
        sig,
        libc::SIGTERM,
        libc::SIGINT
    );

    // The canary MUST be empty — Drop did not run because SIGKILL is
    // uncatchable. If we see "drop-ran" here, `kill()` is actually SIGTERM
    // on this OS and Wave 4 must switch to `nix`.
    let canary_contents = std::fs::read_to_string(&canary).expect("read canary");
    assert!(
        canary_contents.is_empty(),
        "Drop handler ran during kill() — kill() is NOT SIGKILL on this OS. \
         Canary contents: {:?}. Wave 4 must use nix::sys::signal::kill instead.",
        canary_contents
    );

    eprintln!(
        "sigkill verified: child killed by signal {} (SIGKILL={}), canary empty",
        sig,
        libc::SIGKILL
    );
}
