//! Phase 12.7 Plan 02 — architectural test asserting the legacy table /
//! temporal data plane has been killed.
//!
//! Per `project_v0_events_only_scope` (locked 2026-04-30) v0 ships
//! events-only.  Six checks (RED at end of Plan 02; turned GREEN
//! incrementally as Plans 03/04/05/06 landed; Plan 12.7-10 closure
//! removed the `#[ignore]` annotations so the tests now run on every
//! `cargo test --workspace`):
//!
//! 1. `legacy_table_files_deleted` — temporal_http.rs / temporal.rs /
//!    _tables.py no longer exist on disk (Plans 04 + 06).
//! 2. `temporal_record_type_variants_deleted` — RecordType enum has no
//!    `TableUpsert` / `TableDelete` / `Retract` variants (Plan 05).
//! 3. `wire_request_table_variants_deleted` — WireRequest enum has no
//!    `HttpUpsert` / `HttpDelete` / `HttpRetract` / `HttpTableGet`
//!    variants (Plan 03).
//! 4. `route_table_variants_deleted` — Route enum has no `Upsert` /
//!    `Delete` / `Retract` / `TableGet` variants (Plan 03).
//! 5. `python_bv_table_re_export_deleted` — python/beava/__init__.py has
//!    no `from ._tables import table` line and no `"table"` token in
//!    `__all__` (Plan 06).
//! 6. `app_temporal_fields_deleted` — registry_debug.rs DevAggState has
//!    no `temporal_stores` / `event_id_index` field declarations (Plan 04).
//!
//! Companion to `phase12_7_no_table_surface.rs`: that test runs a symbol
//! grep across the workspace; THIS test pins specific files + specific
//! enum variants by name so a partial deletion can be diagnosed precisely.
//! When a Wave 2-3 plan lands, exactly the test(s) covering that surface
//! turn GREEN — the test names form a natural progress map.
//!
//! All 6 tests were `#[ignore]`-marked through Waves 1-3 while the surface
//! still existed. Plan 12.7-10 (closure) removed the `#[ignore]` annotations
//! as the final tests-pass moment, locking the events-only invariant into
//! CI on every PR.
//!
//! ## Why both tests instead of one
//!
//! The companion `phase12_7_no_table_surface.rs` walks the workspace
//! looking for any forbidden symbol; if it fails, the failure list is
//! long and unstructured.  This file's tests pin specific files + enum
//! variants by NAME so a partial deletion (e.g. Plan 12.7-03 deleted the
//! WireRequest variants but Plan 12.7-04 hasn't run yet) leaves the
//! `wire_request_table_variants_deleted` test GREEN while
//! `legacy_table_files_deleted` and `app_temporal_fields_deleted` stay
//! RED.  This is the architectural-test-pair pattern from 12.6 Plans 07
//! and 10: pair a wide-net symbol grep with narrow per-file/per-symbol
//! assertions.

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

/// Test 1 — Legacy table source files deleted on disk.
///
/// `temporal_http.rs` (Phase 12.6 Plan 14 mio bridge), `temporal.rs`
/// (Phase 11.5 MVCC store core), and `python/beava/_tables.py` (Phase
/// 11.5 SDK decorator module) all carry the entire table surface.  Per
/// `project_v0_events_only_scope` (locked 2026-04-30) the table surface
/// is not part of v0; all three files are deleted by Plans 12.7-04
/// (Rust-side) and 12.7-06 (Python-side).
///
/// Turned GREEN when both deletion plans (12.7-04 Rust-side and 12.7-06
/// Python-side) landed; `#[ignore]` removed by Plan 12.7-10 closure.
#[test]
fn legacy_table_files_deleted() {
    let root = workspace_root();
    for path in [
        "crates/beava-server/src/temporal_http.rs",
        "crates/beava-core/src/temporal.rs",
        "python/beava/_tables.py",
    ] {
        let p = root.join(path);
        assert!(
            !p.exists(),
            "{path} must be deleted post-Plan-12.7-04/06 (v0 ships events-only per project_v0_events_only_scope)"
        );
    }
}

/// Test 2 — RecordType enum has no table/retract variants.
///
/// `crates/beava-persistence/src/lib.rs` declares `RecordType::TableUpsert`
/// (`= 0x03`), `RecordType::TableDelete` (`= 0x04`), and `RecordType::Retract`
/// (`= 0x05`).  Per `project_v0_events_only_scope` v0 reduces the WAL record
/// surface to `Event = 0x01` and `RegistryBump = 0x02`; the three
/// table/retract variants are deleted by Plan 12.7-05.
///
/// Sanity-positive: the surviving variants `Event = 0x01` and
/// `RegistryBump = 0x02` MUST remain.
///
/// Turned GREEN when Plan 12.7-05 landed; `#[ignore]` removed by Plan
/// 12.7-10 closure.
#[test]
fn temporal_record_type_variants_deleted() {
    let root = workspace_root();
    let lib_rs = root.join("crates/beava-persistence/src/lib.rs");
    let src = std::fs::read_to_string(&lib_rs).expect("beava-persistence/src/lib.rs must exist");
    for variant in ["TableUpsert", "TableDelete", "Retract"] {
        // `RecordType::Variant` (call site) and `Variant = 0x` (decl site)
        // both forbidden in this enum file.
        let needle_qualified = format!("RecordType::{variant}");
        let needle_decl = format!("{variant} = 0x");
        assert!(
            !src.contains(&needle_qualified),
            "RecordType::{variant} call-site must be deleted from \
             crates/beava-persistence/src/lib.rs (v0 events-only per Plan 12.7-05)"
        );
        assert!(
            !src.contains(&needle_decl),
            "RecordType {variant} declaration (`{variant} = 0x..`) must be deleted from \
             crates/beava-persistence/src/lib.rs (v0 events-only per Plan 12.7-05)"
        );
    }
    // Sanity-positive: surviving variants still present.
    for keep in ["Event = 0x01", "RegistryBump = 0x02"] {
        assert!(
            src.contains(keep),
            "RecordType `{keep}` declaration must remain in beava-persistence/src/lib.rs (v0 events-only path)"
        );
    }
}

/// Test 3 — WireRequest enum has no table/retract variants.
///
/// `crates/beava-runtime-core/src/wire_request.rs` declares
/// `HttpUpsert { table, body }`, `HttpDelete { table, body }`,
/// `HttpRetract { body }`, and `HttpTableGet { table, query }` variants
/// to carry table-flavored mio dispatches.  Per `project_v0_events_only_scope`
/// v0 reduces the wire surface to push/get/register; the four
/// table-flavored variants are deleted by Plan 12.7-03.
///
/// Sanity-positive: surviving variants `HttpPush`, `HttpGet`, `Register`,
/// `Ping`, etc. MUST remain.
///
/// Turned GREEN when Plan 12.7-03 landed; `#[ignore]` removed by Plan
/// 12.7-10 closure.
#[test]
fn wire_request_table_variants_deleted() {
    let root = workspace_root();
    let wire_rs = root.join("crates/beava-runtime-core/src/wire_request.rs");
    let src = std::fs::read_to_string(&wire_rs)
        .expect("beava-runtime-core/src/wire_request.rs must exist");
    for variant in ["HttpUpsert", "HttpDelete", "HttpRetract", "HttpTableGet"] {
        // Match either the qualified call-site form (`WireRequest::HttpUpsert`)
        // or the bare-declaration form (`HttpUpsert {`) which appears in
        // the enum body.
        let needle_qualified = format!("WireRequest::{variant}");
        let needle_decl = format!("{variant} {{");
        assert!(
            !src.contains(&needle_qualified),
            "WireRequest::{variant} call-site must be deleted from \
             crates/beava-runtime-core/src/wire_request.rs (v0 events-only per Plan 12.7-03)"
        );
        assert!(
            !src.contains(&needle_decl),
            "WireRequest variant declaration (`{variant} {{`) must be deleted from \
             crates/beava-runtime-core/src/wire_request.rs (v0 events-only per Plan 12.7-03)"
        );
    }
    // Sanity-positive: surviving variants still present (canonical v0 surface).
    for keep in ["Register {", "HttpGet {", "Ping"] {
        assert!(
            src.contains(keep),
            "WireRequest variant `{keep}` must remain in wire_request.rs (v0 events-only path)"
        );
    }
}

/// Test 4 — Route enum has no table/retract variants.
///
/// `crates/beava-runtime-core/src/router.rs` declares `Route::Upsert`,
/// `Route::Delete`, `Route::Retract`, and `Route::TableGet` variants to
/// route the corresponding HTTP paths.  Per `project_v0_events_only_scope`
/// v0 reduces the routing surface; the four table routes are deleted by
/// Plan 12.7-03 (deleted routes return mio's default 404 with no special
/// deny-handler per CONTEXT D-02).
///
/// Sanity-positive: surviving routes (`Route::Push`, `Route::Get`,
/// `Route::Register`, etc.) MUST remain.
///
/// Turned GREEN when Plan 12.7-03 landed; `#[ignore]` removed by Plan
/// 12.7-10 closure.
#[test]
fn route_table_variants_deleted() {
    let root = workspace_root();
    let router_rs = root.join("crates/beava-runtime-core/src/router.rs");
    let src =
        std::fs::read_to_string(&router_rs).expect("beava-runtime-core/src/router.rs must exist");
    // Note `Retract` is declared as a unit variant (`Retract,`) without a
    // body, while `Upsert`, `Delete`, `TableGet` carry struct bodies.
    for variant in ["Upsert", "Delete", "TableGet"] {
        let needle_qualified = format!("Route::{variant}");
        let needle_decl_struct = format!("{variant} {{");
        assert!(
            !src.contains(&needle_qualified),
            "Route::{variant} call-site must be deleted from \
             crates/beava-runtime-core/src/router.rs (v0 events-only per Plan 12.7-03)"
        );
        assert!(
            !src.contains(&needle_decl_struct),
            "Route variant declaration (`{variant} {{`) must be deleted from \
             crates/beava-runtime-core/src/router.rs (v0 events-only per Plan 12.7-03)"
        );
    }
    // `Retract` is a unit variant — match `Route::Retract` (call-site) and
    // any line of the form `\tRetract,` or `    Retract,` (declaration).
    assert!(
        !src.contains("Route::Retract"),
        "Route::Retract call-site must be deleted from router.rs (v0 events-only per Plan 12.7-03)"
    );
    // Use `\n    Retract,` (4-space indent inside enum body) and `\n    Retract\n` to
    // catch the declaration without matching false-positives like `Retract`
    // appearing inside doc comments or qualified names.
    let bare_unit_decl = src
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
        })
        .any(|l| {
            let t = l.trim();
            t == "Retract," || t == "Retract"
        });
    assert!(
        !bare_unit_decl,
        "Route variant declaration `Retract,` (unit variant) must be deleted from \
         router.rs (v0 events-only per Plan 12.7-03)"
    );
    // Sanity-positive: surviving Route variants still present.
    for keep in ["Push", "Get", "Register"] {
        assert!(
            src.contains(keep),
            "Route variant `{keep}` must remain in router.rs (v0 events-only path)"
        );
    }
}

/// Test 5 — Python `bv.table` re-export deleted.
///
/// `python/beava/__init__.py` re-exports `from ._tables import table`
/// (line 53) and lists `"table"` in `__all__` (line 59).  Per
/// `project_v0_events_only_scope` v0's public Python surface drops the
/// `@bv.table` decorator entirely; the re-export and `__all__` entry are
/// deleted by Plan 12.7-06 (the underlying `_tables.py` module is also
/// deleted — see test 1).
///
/// After deletion `import beava as bv; bv.table(...)` raises
/// `AttributeError: module 'beava' has no attribute 'table'` naturally
/// (no explicit deny-stub per CONTEXT D-02).
///
/// Sanity-positive: surviving re-exports `from ._events import event` and
/// the `"event"` token in `__all__` MUST remain.
///
/// Turned GREEN when Plan 12.7-06 landed; `#[ignore]` removed by Plan
/// 12.7-10 closure.
///
/// **Phase 13.5 Plan 11 amendment (ADR-001 partial overturn 2026-05-03):**
/// `@bv.table` is REVIVED for aggregation-output (no upsert/delete/retract;
/// no MVCC). The forbidden artifact is the legacy `_tables` (plural) module
/// — the OLD upsert/delete table. The new `_table` (singular) module
/// containing only the `@bv.table` decorator is allowed and required.
#[test]
fn python_bv_table_re_export_deleted() {
    let root = workspace_root();
    let init_py = root.join("python/beava/__init__.py");
    let src = std::fs::read_to_string(&init_py).expect("python/beava/__init__.py must exist");
    // The legacy plural `_tables` module is forbidden (it had upsert / delete /
    // retract — ADR-001 explicitly does NOT revive these).
    assert!(
        !src.contains("from ._tables import"),
        "`from ._tables import ...` (plural — legacy upsert/delete) must \
         remain deleted from python/beava/__init__.py (v0 events-only per \
         Plan 12.7-06; ADR-001 only revives the @bv.table aggregation \
         decorator via the singular `_table` module)"
    );
    // Sanity-positive: surviving re-exports still present.
    assert!(
        src.contains("from beava._events import event")
            || src.contains("from ._events import event"),
        "Sanity: `from ._events import event` re-export must remain in \
         python/beava/__init__.py (the @bv.event decorator is part of the v0 surface)"
    );
    assert!(
        src.contains("\"event\","),
        "Sanity: `\"event\",` entry must remain in __all__ (v0 events-only path)"
    );
    // Sanity-positive (ADR-001): the @bv.table decorator is part of v0.
    assert!(
        src.contains("from beava._table import table") || src.contains("from ._table import table"),
        "ADR-001 partial overturn: `from ._table import table` (singular — \
         aggregation-output decorator) must remain re-exported"
    );
}

/// Test 6 — DevAggState has no table-flavored fields.
///
/// `crates/beava-server/src/registry_debug.rs` declares the in-memory
/// agg state with two table-flavored fields:
///   - `temporal_stores: Arc<Mutex<HashMap<String, TemporalStore>>>`
///   - `event_id_index: Arc<Mutex<hashbrown::HashMap<u64, EventIdEntry, ...>>>`
///
/// Per `project_v0_events_only_scope` both fields are deleted by Plan
/// 12.7-04 (along with the `TemporalStore` type itself — see test 1).
///
/// Sanity-positive: surviving fields (`state_tables`, `registry`,
/// `next_event_id`, `query_time_ms`) MUST remain.
///
/// Turned GREEN when Plan 12.7-04 landed; `#[ignore]` removed by Plan
/// 12.7-10 closure.
#[test]
fn app_temporal_fields_deleted() {
    let root = workspace_root();
    let registry_debug_rs = root.join("crates/beava-server/src/registry_debug.rs");
    let src = std::fs::read_to_string(&registry_debug_rs)
        .expect("crates/beava-server/src/registry_debug.rs must exist");
    // Match the `pub temporal_stores:` and `pub event_id_index:` field
    // declarations specifically (a bare `temporal_stores` token might
    // also appear in doc comments, which we tolerate).
    assert!(
        !src.contains("pub temporal_stores:"),
        "`pub temporal_stores:` field declaration must be deleted from \
         crates/beava-server/src/registry_debug.rs (v0 events-only per Plan 12.7-04)"
    );
    assert!(
        !src.contains("pub event_id_index:"),
        "`pub event_id_index:` field declaration must be deleted from \
         crates/beava-server/src/registry_debug.rs (v0 events-only per Plan 12.7-04)"
    );
    // Sanity-positive: surviving DevAggState fields still present.
    for keep in [
        "pub state_tables:",
        "pub registry:",
        "pub next_event_id:",
        "pub query_time_ms:",
    ] {
        assert!(
            src.contains(keep),
            "DevAggState field `{keep}` must remain in registry_debug.rs (v0 events-only path)"
        );
    }
}
