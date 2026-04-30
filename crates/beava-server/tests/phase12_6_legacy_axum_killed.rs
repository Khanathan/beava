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
//! 5. `temporal_http_axum_handlers_deleted` — temporal_http.rs has zero
//!    `axum::` references; helpers (`json_object_to_row`, `entity_key_from_body`,
//!    `row_to_json`) are still present.
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
    for path in [
        "crates/beava-server/src/push.rs",
        "crates/beava-server/src/http.rs",
        "crates/beava-server/src/http_admin.rs",
        "crates/beava-server/src/push_and_get.rs",
    ] {
        let p = root.join(path);
        assert!(
            !p.exists(),
            "{path} must be deleted post-Plan-12.6-07 (mio is the sole data-plane runtime)"
        );
    }
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

#[test]
fn temporal_http_axum_handlers_deleted() {
    let root = workspace_root();
    let temporal_rs = root.join("crates/beava-server/src/temporal_http.rs");
    let src = std::fs::read_to_string(&temporal_rs).expect("temporal_http.rs exists");
    assert!(
        !src.contains("axum::"),
        "temporal_http.rs must have zero `axum::` references post-Plan-12.6-07 (mio handles upsert/delete/retract/table)"
    );
    for token in [
        "upsert_handler",
        "delete_handler",
        "retract_handler",
        "table_get_handler",
        "temporal_router",
    ] {
        assert!(
            !src.contains(token),
            "axum {token} must be deleted from temporal_http.rs (use *_via_mio helpers instead)"
        );
    }
    // Helpers preserved.
    for token in [
        "fn json_object_to_row",
        "fn entity_key_from_body",
        "fn row_to_json",
    ] {
        assert!(
            src.contains(token),
            "{token} helper must remain in temporal_http.rs (consumed by ServerV18 mio path)"
        );
    }
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
