//! Phase 13.4.1 Plan 03 — RED integration tests for `BatchGetReqEntry`
//! `entity_id` → `key` rename with one-release `serde(alias = "entity_id")`
//! deprecation path (D-04).
//!
//! Four failing integration tests asserting the locked Phase 13.0 wire-spec
//! contract for `POST /batch_get` per-entry shape:
//!
//! - **D-04 (alias path)** — Sending the LEGACY per-entry field name
//!   `entity_id` (not `key`) on `POST /batch_get` deserialises and produces a
//!   FLAT row (no `{table, entity_id, features:{...}}` envelope). This is the
//!   one-release back-compat alias.
//! - **D-04 (canonical path)** — Sending the CANONICAL per-entry field name
//!   `key` on `POST /batch_get` deserialises and produces a FLAT row.
//! - **D-04 (WARN-on-alias)** — When the alias `entity_id` is used, the
//!   server emits a WARN-level tracing log line containing the verbatim
//!   deprecation message text from CONTEXT.md line 76:
//!   `BatchGetReqEntry: deprecated 'entity_id' field name; rename to 'key';
//!   alias removed in v0.0.x`.
//! - **D-04 (no-false-positive)** — When the canonical `key` field is used,
//!   the server does NOT emit the alias-deprecation WARN — otherwise every
//!   legitimate `batch_get` request would log a spurious warn, defeating the
//!   purpose of the deprecation log.
//!
//! ## TDD discipline (CLAUDE.md §Conventions)
//!
//! All 4 tests are RED at the time this file lands. The matching GREEN
//! commits live in Plan 13.4.1-04, which will:
//!   * Add `#[serde(alias = "entity_id")]` to the renamed `key` field on
//!     `BatchGetReqEntry`.
//!   * Wire alias-detection (custom deserialise or sentinel) so the server
//!     can tell when the request used the alias vs the canonical name.
//!   * Emit `tracing::warn!` at `dispatch_batch_get_sync` entry when any
//!     per-entry was deserialised from the `entity_id` alias source.
//!   * Flatten the `dispatch_batch_get_sync` response constructor (D-03).
//!
//! Plan 04 SHOULD pick the STRICT alias-detection strategy (alias-only WARN)
//! per PATTERNS.md cross-cutting §4 — that makes Test 4 (no-false-positive
//! on canonical path) pass alongside Test 3 (alias path WARNs). The
//! pragmatic always-warn alternative would fail Test 4.
//!
//! ## Tracing capture mechanism
//!
//! Each test that captures logs builds an in-memory `Vec<u8>` writer behind a
//! `tracing_subscriber::fmt::MakeWriter` impl, sets it as the per-thread
//! default subscriber via `tracing::subscriber::set_default`, runs the
//! request, then drops the guard and reads the captured bytes. This avoids
//! a process-global `fmt::init` (which would break parallel tests) — every
//! test gets its own scoped subscriber.
//!
//! ## Helpers
//!
//! `register_payload`, `register`, and `push_seed_events` are copied verbatim
//! from `phase13_4_op_batch_get.rs` so the fixture is identical to Plan 01's
//! fixture (Phase 13.4.1 Wave 1 cross-test consistency). After seed pushes,
//! `UserSpend(user_id="alice")` has `cnt=2, total=42.5`.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tracing_subscriber::fmt::MakeWriter;

// ─── Shared fixture (copied from phase13_4_op_batch_get.rs) ─────────────────

/// A two-table pipeline:
///   - `UserSpend(user_id) → cnt, total` driven by `Tx`.
///   - `MerchantSpend(merchant_id) → merchant_cnt` driven by `Tx`.
fn register_payload() -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {
                        "event_time": "i64",
                        "user_id": "str",
                        "merchant_id": "str",
                        "amount": "f64"
                    },
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": "UserSpend",
                "output_kind": "table",
                "upstreams": ["Tx"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt": {"op": "count", "params": {}},
                        "total": {"op": "sum", "params": {"field": "amount"}}
                    }
                }],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64", "total": "f64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            },
            {
                "kind": "derivation",
                "name": "MerchantSpend",
                "output_kind": "table",
                "upstreams": ["Tx"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["merchant_id"],
                    "agg": {
                        "merchant_cnt": {"op": "count", "params": {}}
                    }
                }],
                "schema": {
                    "fields": {"merchant_id": "str", "merchant_cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["merchant_id"]
            }
        ]
    })
}

async fn register(ts: &TestServer) {
    let resp = ts
        .post_json("/register", &register_payload())
        .await
        .expect("register");
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "register failed: status={status} body={body_text}"
    );
}

/// Push two events for alice (10 + 32.5) and one for bob (5) — all at acme.
/// After this, `UserSpend("alice") = {cnt: 2, total: 42.5}` and
/// `MerchantSpend("acme") = {merchant_cnt: 3}`.
async fn push_seed_events(ts: &TestServer) {
    let events = [
        json!({"event_time": 1000, "user_id": "alice", "merchant_id": "acme", "amount": 10.0}),
        json!({"event_time": 1001, "user_id": "alice", "merchant_id": "acme", "amount": 32.5}),
        json!({"event_time": 1002, "user_id": "bob",   "merchant_id": "acme", "amount": 5.0}),
    ];
    for body in events {
        let resp = reqwest::Client::new()
            .post(format!("{}/push/Tx", ts.base_url()))
            .json(&body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("push");
        assert!(
            resp.status().is_success(),
            "push failed: status={}, body={body:?}",
            resp.status()
        );
    }
}

// ─── In-test tracing capture ──────────────────────────────────────────────
//
// Plan 13.4.1-04 update: the original Plan 03 RED file used
// `tracing::subscriber::set_default` (per-thread). That doesn't work for the
// D-04 WARN — `dispatch_batch_get_sync` runs on the apply thread (mio data
// plane), not the test thread, so per-thread capture sees nothing. The fix
// is a process-global `set_global_default` installed once via OnceLock,
// fanning every WARN event into a shared `ACTIVE_SINK`. Tests 3 + 4 grab
// the global capture lock (via `#[serial_test::serial]`), swap their own
// `Arc<Mutex<Vec<u8>>>` into `ACTIVE_SINK`, run the request, snapshot the
// buffer, and clear the sink. Tests 1 + 2 don't capture and don't need
// the serial gate.

/// `MakeWriter` impl that routes every emitted line to the currently active
/// sink (or `/dev/null` when none is set). This is process-global because
/// `set_global_default` only fires once per process; per-test isolation
/// happens via `swap_active_sink`.
#[derive(Clone)]
struct TestWriter;

impl<'a> MakeWriter<'a> for TestWriter {
    type Writer = TestWriterHandle;
    fn make_writer(&'a self) -> Self::Writer {
        TestWriterHandle
    }
}

struct TestWriterHandle;

impl std::io::Write for TestWriterHandle {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if let Some(sink) = active_sink() {
            sink.lock().unwrap().extend_from_slice(b);
        }
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The currently-active capture sink. Tests use `swap_active_sink` to install
/// or clear their per-test buffer; the writer routes every WARN line into it.
fn active_sink() -> Option<Arc<Mutex<Vec<u8>>>> {
    ACTIVE_SINK.get().and_then(|m| m.lock().unwrap().clone())
}

// reason: test-only nested-Mutex sink; the layered Option<Arc<Mutex<…>>>
// shape lets each test install/clear its per-test buffer without racing on
// the OnceLock. A helper alias would obscure the swap_active_sink semantics.
#[allow(clippy::type_complexity)]
static ACTIVE_SINK: OnceLock<Mutex<Option<Arc<Mutex<Vec<u8>>>>>> = OnceLock::new();
static SUBSCRIBER_INSTALLED: OnceLock<()> = OnceLock::new();

fn swap_active_sink(new: Option<Arc<Mutex<Vec<u8>>>>) {
    let cell = ACTIVE_SINK.get_or_init(|| Mutex::new(None));
    *cell.lock().unwrap() = new;
}

/// Install the process-global tracing subscriber once. Idempotent across
/// tests via `OnceLock`. Subsequent calls are no-ops.
fn install_global_warn_subscriber() {
    SUBSCRIBER_INSTALLED.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_writer(TestWriter)
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        // It's okay if another test crate already installed a global
        // subscriber (e.g. via env_logger or another integration test) —
        // `set_global_default` returns Err but our local capture still
        // routes via the newly-set writer if we win the race. In practice
        // this binary's own tests are the only ones installing a global,
        // so the first call wins and subsequent ones return Err which we
        // silently drop.
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

/// Activate per-test capture. Returns a guard that clears `ACTIVE_SINK` on
/// drop, so capture buffers from one test don't leak into the next.
struct WarnCaptureGuard;

impl Drop for WarnCaptureGuard {
    fn drop(&mut self) {
        swap_active_sink(None);
    }
}

fn install_warn_capture(capture: Arc<Mutex<Vec<u8>>>) -> WarnCaptureGuard {
    install_global_warn_subscriber();
    swap_active_sink(Some(capture));
    WarnCaptureGuard
}

fn captured_logs(capture: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&capture.lock().unwrap()).to_string()
}

// ─── Test 1 — D-04 alias path: legacy `entity_id` deserialises + flat row ───
//
// All four tests share the `serial(warn_capture)` lock because the global
// tracing subscriber routes every WARN to the currently-active capture sink.
// If Test 1 (which sends `entity_id`) ran in parallel with Test 4 (which
// asserts no alias-WARN was captured), Test 1's WARN would bleed into Test
// 4's sink. Serialising all four tests under the same lock prevents this —
// at any point in time only one test "owns" the capture sink.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(warn_capture)]
async fn phase13_4_1_alias_entity_id_deserializes() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // D-04 alias path: legacy field name `entity_id`. Plan 04 adds
    // `#[serde(alias = "entity_id")]` to the renamed `key` field so this
    // request body still deserialises during the one-release deprecation
    // window.
    let req = json!({
        "requests": [
            {"table": "UserSpend", "entity_id": "alice"}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "alias entity_id must deserialise during the one-release deprecation \
         window; got status={}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().await.expect("json body");
    let results = body
        .get("results")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("expected results array, got: {body:#}"));
    assert_eq!(
        results.len(),
        1,
        "results must mirror request length; got: {body:#}"
    );

    // FLAT row — feature dict IS the result, no envelope (D-03).
    assert_eq!(
        results[0]["cnt"], 2,
        "alice cnt=2 (FLAT row, no envelope); got: {body:#}"
    );
    let alice_total = results[0]["total"]
        .as_f64()
        .unwrap_or_else(|| panic!("alice total must be number, got: {body:#}"));
    assert!(
        (alice_total - 42.5).abs() < 1e-9,
        "alice total=42.5, got total={alice_total}"
    );

    // No envelope wrapping — D-03 flat-row contract.
    assert!(
        results[0].get("table").is_none(),
        "FLAT response — no `table` envelope key; got: {body:#}"
    );
    assert!(
        results[0].get("entity_id").is_none(),
        "FLAT response — no `entity_id` envelope key; got: {body:#}"
    );
    assert!(
        results[0].get("features").is_none(),
        "FLAT response — no `features` envelope key; got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 2 — D-04 canonical path: `key` deserialises + flat row ───────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(warn_capture)]
async fn phase13_4_1_canonical_key_deserializes() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // D-04 canonical path: the renamed `key` field on `BatchGetReqEntry`.
    // Plan 04 introduces this rename — until it lands the request 500s
    // because today's `BatchGetReqEntry` only accepts `entity_id`.
    let req = json!({
        "requests": [
            {"table": "UserSpend", "key": "alice"}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "canonical key must deserialise; got status={}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().await.expect("json body");
    let results = body
        .get("results")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("expected results array, got: {body:#}"));
    assert_eq!(
        results.len(),
        1,
        "results must mirror request length; got: {body:#}"
    );

    // FLAT row — feature dict IS the result, no envelope (D-03).
    assert_eq!(
        results[0]["cnt"], 2,
        "alice cnt=2 (FLAT row, no envelope); got: {body:#}"
    );
    let alice_total = results[0]["total"]
        .as_f64()
        .unwrap_or_else(|| panic!("alice total must be number, got: {body:#}"));
    assert!(
        (alice_total - 42.5).abs() < 1e-9,
        "alice total=42.5, got total={alice_total}"
    );

    // No envelope wrapping — D-03 flat-row contract.
    assert!(
        results[0].get("table").is_none(),
        "FLAT response — no `table` envelope key; got: {body:#}"
    );
    assert!(
        results[0].get("entity_id").is_none(),
        "FLAT response — no `entity_id` envelope key; got: {body:#}"
    );
    assert!(
        results[0].get("features").is_none(),
        "FLAT response — no `features` envelope key; got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 3 — D-04 WARN log emitted on alias use ───────────────────────────
//
// `#[serial_test::serial(warn_capture)]` — all four tests in this file share
// this lock. See the comment on Test 1 for the parallel-pollution rationale.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(warn_capture)]
async fn phase13_4_1_alias_entity_id_emits_warn_log() {
    let capture: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let _guard = install_warn_capture(capture.clone());

    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // Alias path — server must emit a WARN log on `entity_id` use per
    // CONTEXT.md D-04 line 76.
    let req = json!({
        "requests": [
            {"table": "UserSpend", "entity_id": "alice"}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "alias entity_id must deserialise; got status={}",
        resp.status()
    );

    // Drain any tail of WARN events that the server may emit on the same
    // thread before drop. The scoped per-thread default subscriber should
    // pick up everything emitted on this test's thread.
    let logs = captured_logs(&capture);

    // Verbatim D-04 deprecation message substrings (CONTEXT.md line 76):
    //   "BatchGetReqEntry: deprecated 'entity_id' field name; rename to
    //    'key'; alias removed in v0.0.x"
    assert!(
        logs.contains("deprecated 'entity_id' field name"),
        "WARN log must contain verbatim deprecation substring \"deprecated \
         'entity_id' field name\"; got logs:\n{logs}"
    );
    assert!(
        logs.contains("rename to 'key'"),
        "WARN log must contain verbatim deprecation substring \"rename to \
         'key'\"; got logs:\n{logs}"
    );
    assert!(
        logs.contains("alias removed in v0.0.x"),
        "WARN log must contain verbatim deprecation substring \"alias \
         removed in v0.0.x\"; got logs:\n{logs}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 4 — D-04 canonical path does NOT emit alias-deprecation WARN ──────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(warn_capture)]
async fn phase13_4_1_canonical_key_does_not_emit_alias_warn() {
    let capture: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let _guard = install_warn_capture(capture.clone());

    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // Canonical path — server must NOT emit the alias-deprecation WARN,
    // otherwise every legitimate batch_get request would log a spurious
    // WARN, defeating the purpose. Plan 04 SHOULD pick the STRICT
    // alias-detection strategy (PATTERNS.md cross-cutting §4) so this test
    // passes alongside Test 3.
    let req = json!({
        "requests": [
            {"table": "UserSpend", "key": "alice"}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "canonical key must deserialise; got status={}",
        resp.status()
    );

    let logs = captured_logs(&capture);

    // No-false-positive: the EXACT D-04 deprecation message must NOT appear
    // when the canonical `key` field was used. Under PATTERNS.md §4 STRICT
    // (alias-only WARN) this passes; under PRAGMATIC (always-warn) it
    // fails — Plan 04 picks STRICT.
    assert!(
        !logs.contains("deprecated 'entity_id' field name"),
        "canonical `key` path must NOT emit the alias-deprecation WARN \
         (no-false-positive); got logs:\n{logs}"
    );

    ts.shutdown().await.ok();
}
