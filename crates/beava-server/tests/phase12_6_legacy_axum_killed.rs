//! Phase 12.6 Plan 07 — architectural test asserting the legacy axum data
//! plane has been killed.
//!
//! Six checks (RED → GREEN once Plan 07 deletes the legacy code):
//!
//! 1. `legacy_axum_files_deleted` — push.rs / http.rs / http_admin.rs /
//!    push_and_get.rs no longer exist on disk.
//! 2. `legacy_server_struct_deleted` — server.rs has only `pub struct ServerV18`,
//!    not `pub struct Server {`.
//! 3. `dispatch_wire_request_async_deleted` — runtime_core_glue.rs no longer
//!    declares `pub async fn dispatch_wire_request(`.
//! 4. `beava_dev_endpoints_env_var_deleted` — no `.rs` file under
//!    `crates/beava-server/src/` mentions `BEAVA_DEV_ENDPOINTS`.
//! 5. `temporal_http_axum_handlers_deleted` — **Plan 12.7-04** deleted the
//!    entire `temporal_http.rs` source file (events-only strip per
//!    `project_v0_events_only_scope`). The test now asserts the file is
//!    absent on disk; the original axum-symbol grep is moot once the file
//!    itself is gone. The 12.7 architectural-test pair
//!    (`phase12_7_legacy_table_handlers_killed::legacy_table_files_deleted`)
//!    is the canonical assertion of file absence; this preserved test stays
//!    here for back-compat with the Plan 12.6-07 architectural-test scaffold.
//! 6. `shutdown_signal_preserved_and_used_by_serverv18` — shutdown.rs still
//!    declares `pub async fn shutdown_signal`, doc-comment no longer cites
//!    `axum::serve`, and main.rs continues to invoke `shutdown_signal`.
//!
//! Per `project_phase18_no_dual_runtime` the mio data plane is the SOLE
//! data-plane runtime; the tokio admin sidecar (separate port) is preserved.

use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR for `beava-server` is `crates/beava-server`; the
    // workspace root is two parents up.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn legacy_axum_files_deleted() {
    let root = workspace_root();
    // Files that must be DELETED post-Plan-12.6-07 (mio is the sole data-plane runtime).
    // http_admin.rs is intentionally KEPT — it is the canonical tokio admin sidecar
    // (`BoundAdminServer`) that ServerV18 binds at `cfg.admin_addr`. See Plan 07 SUMMARY
    // §Deviations.
    for path in [
        "crates/beava-server/src/push.rs",
        "crates/beava-server/src/http.rs",
        "crates/beava-server/src/push_and_get.rs",
    ] {
        let p = root.join(path);
        assert!(
            !p.exists(),
            "{path} must be deleted post-Plan-12.6-07 (mio is the sole data-plane runtime)"
        );
    }
    // Positive assertion: http_admin.rs MUST exist as the admin sidecar.
    let admin = root.join("crates/beava-server/src/http_admin.rs");
    assert!(
        admin.exists(),
        "crates/beava-server/src/http_admin.rs must exist — it is the canonical tokio admin sidecar bound by ServerV18 (per Plan-12.6-07 SUMMARY §Deviations)"
    );
}

#[test]
fn legacy_server_struct_deleted() {
    let root = workspace_root();
    let server_rs = root.join("crates/beava-server/src/server.rs");
    let src = std::fs::read_to_string(&server_rs).expect("server.rs exists");
    // Match `pub struct Server` with whitespace + brace to disambiguate from
    // `pub struct ServerV18`.
    assert!(
        !src.contains("pub struct Server {"),
        "legacy `pub struct Server {{` must be deleted from server.rs (only ServerV18 remains)"
    );
    // Sanity-check ServerV18 IS present so the test isn't trivially green.
    assert!(
        src.contains("pub struct ServerV18 {"),
        "ServerV18 must remain — production data-plane entry"
    );
}

#[test]
fn dispatch_wire_request_async_deleted() {
    let root = workspace_root();
    let glue_rs = root.join("crates/beava-server/src/runtime_core_glue.rs");
    let src = std::fs::read_to_string(&glue_rs).expect("runtime_core_glue.rs exists");
    assert!(
        !src.contains("pub async fn dispatch_wire_request("),
        "legacy `pub async fn dispatch_wire_request(` must be deleted from runtime_core_glue.rs (only sync helpers survive)"
    );
}

/// Walk `dir` recursively, return any `.rs` file content lines containing `needle`.
fn grep_rust_files(dir: &Path, needle: &str) -> Vec<String> {
    let mut hits = Vec::new();
    fn walk(dir: &Path, needle: &str, hits: &mut Vec<String>) {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                walk(&p, needle, hits);
            } else if p.extension().and_then(|s| s.to_str()) == Some("rs") {
                if let Ok(content) = std::fs::read_to_string(&p) {
                    if content.contains(needle) {
                        hits.push(p.display().to_string());
                    }
                }
            }
        }
    }
    walk(dir, needle, &mut hits);
    hits
}

#[test]
fn beava_dev_endpoints_env_var_deleted() {
    let root = workspace_root();
    let server_src = root.join("crates/beava-server/src");
    let hits = grep_rust_files(&server_src, "BEAVA_DEV_ENDPOINTS");
    assert!(
        hits.is_empty(),
        "BEAVA_DEV_ENDPOINTS env-var infrastructure must be deleted from crates/beava-server/src/. \
         Found references in: {hits:?}"
    );
}

/// Plan 12.6-07 originally asserted `temporal_http.rs` had zero `axum::`
/// references and that the `*_via_mio` helpers were preserved.
///
/// **Plan 12.7-04** deleted the entire file as part of the events-only
/// strip (`project_v0_events_only_scope`, locked 2026-04-30). With the file
/// gone the original 12.6-07 grep checks are moot. The test is repointed at
/// the new invariant: `temporal_http.rs` MUST NOT exist on disk.
///
/// Companion architectural test
/// `phase12_7_legacy_table_handlers_killed::legacy_table_files_deleted` is
/// the canonical 12.7-side assertion (it covers `temporal.rs` and the
/// `python/beava/_tables.py` SDK module too). This shim preserves the
/// 12.6-07 test name so the historical CI signal stays attached to the
/// same invariant after the v0 surface reduction completed in Plan 12.7-04.
#[test]
fn temporal_http_axum_handlers_deleted() {
    let root = workspace_root();
    let temporal_rs = root.join("crates/beava-server/src/temporal_http.rs");
    assert!(
        !temporal_rs.exists(),
        "crates/beava-server/src/temporal_http.rs must be deleted post-Plan-12.7-04 \
         (events-only strip per project_v0_events_only_scope)"
    );
}

#[test]
fn shutdown_signal_preserved_and_used_by_serverv18() {
    let root = workspace_root();
    let shutdown_rs = root.join("crates/beava-server/src/shutdown.rs");
    let main_rs = root.join("crates/beava-server/src/main.rs");
    let shutdown = std::fs::read_to_string(&shutdown_rs).expect("shutdown.rs exists");
    let main = std::fs::read_to_string(&main_rs).expect("main.rs exists");

    assert!(
        shutdown.contains("pub async fn shutdown_signal"),
        "`pub async fn shutdown_signal` must be preserved (ServerV18 + tokio admin sidecar consume it)"
    );
    assert!(
        !shutdown.contains("axum::serve"),
        "stale `axum::serve` doc reference must be updated to cite `ServerV18::serve_with_dirs`"
    );
    assert!(
        main.contains("shutdown_signal"),
        "main.rs must continue to invoke `shutdown_signal` for the ServerV18 graceful drain"
    );
}
