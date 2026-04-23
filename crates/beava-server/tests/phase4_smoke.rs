//! Phase 4 Rust-side acceptance gate — ROADMAP SC1/SC2/SC3/SC5 smoke over HTTP + TCP.
//! Python-side coverage of SC1..SC5 + the SC4 client/server equivalence proptest lives in Plan 04-07.

use beava_core::row::{Row, Value};
use beava_core::wire::{OP_ERROR_RESPONSE, OP_REGISTER};
use beava_server::testing::{TestServer, TestServerBuilder};
use serde_json::json;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Payload for registering a Transaction event with {event_time: i64, amount: f64}.
fn transaction_event_payload() -> serde_json::Value {
    json!({
        "nodes": [{
            "kind": "event",
            "name": "Transaction",
            "schema": {
                "fields": {"event_time": "i64", "amount": "f64"},
                "optional_fields": []
            },
            "event_time_field": "event_time"
        }]
    })
}

/// Payload for registering Transaction + a filter derivation over it.
fn transaction_plus_filter_payload(deriv_name: &str, filter_expr: &str) -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Transaction",
                "schema": {
                    "fields": {"event_time": "i64", "amount": "f64"},
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": deriv_name,
                "output_kind": "event",
                "upstreams": ["Transaction"],
                "ops": [{"op": "filter", "expr": filter_expr}],
                "schema": {
                    "fields": {"event_time": "i64", "amount": "f64"},
                    "optional_fields": []
                }
            }
        ]
    })
}

// ─── SC1 (HTTP): filter rejects failing events ────────────────────────────────

/// SC1 (HTTP): Event.filter predicate registered over HTTP; server rejects events
/// failing the predicate via POST /dev/apply_ops returning {kept: false}.
#[tokio::test]
async fn sc1_http_filter_rejects_failing_events() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    // Register Transaction + BigTx (filter: amount > 100) over HTTP.
    let body = transaction_plus_filter_payload("BigTx", "(amount > 100)");
    let resp = ts
        .post_json("/register", &body)
        .await
        .expect("register post");
    assert_eq!(resp.status().as_u16(), 200, "register must succeed");
    let reg_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        reg_body["registry_version"], 1,
        "version must bump to 1: {reg_body:#}"
    );

    // Row below threshold → should be dropped.
    let resp = ts
        .post_json(
            "/dev/apply_ops",
            &json!({"derivation": "BigTx", "row": {"event_time": 1000, "amount": 50.0}}),
        )
        .await
        .expect("apply_ops post");
    assert_eq!(resp.status().as_u16(), 200);
    let apply_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        apply_body["kept"], false,
        "amount=50 < 100 should be dropped: {apply_body:#}"
    );

    // Row above threshold → should be kept.
    let resp = ts
        .post_json(
            "/dev/apply_ops",
            &json!({"derivation": "BigTx", "row": {"event_time": 1000, "amount": 150.0}}),
        )
        .await
        .expect("apply_ops post");
    assert_eq!(resp.status().as_u16(), 200);
    let apply_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        apply_body["kept"], true,
        "amount=150 > 100 should be kept: {apply_body:#}"
    );
    assert_eq!(
        apply_body["row"]["amount"], 150.0,
        "amount should be preserved in output: {apply_body:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

// ─── SC1 (TCP): filter rejects failing events ─────────────────────────────────

/// SC1 (TCP): Same as sc1_http but the derivation is registered over TCP.
/// /dev/apply_ops verification still uses HTTP (dev endpoint is HTTP-only).
#[tokio::test]
async fn sc1_tcp_filter_rejects_failing_events() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    // Register via TCP.
    let body = transaction_plus_filter_payload("BigTxTcp", "(amount > 100)");
    let mut c = ts.tcp_client().await.expect("tcp client");
    let (op, tcp_body) = c.register_json(body).await.expect("tcp register");
    assert_eq!(op, OP_REGISTER, "register should succeed over TCP");
    assert_eq!(
        tcp_body["registry_version"], 1,
        "version must bump to 1: {tcp_body:#}"
    );

    // Row below threshold → dropped (verify via HTTP /dev/apply_ops).
    let resp = ts
        .post_json(
            "/dev/apply_ops",
            &json!({"derivation": "BigTxTcp", "row": {"event_time": 1, "amount": 50.0}}),
        )
        .await
        .expect("apply_ops");
    assert_eq!(resp.status().as_u16(), 200);
    let apply_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        apply_body["kept"], false,
        "amount=50 < 100 should be dropped: {apply_body:#}"
    );

    // Row above threshold → kept.
    let resp = ts
        .post_json(
            "/dev/apply_ops",
            &json!({"derivation": "BigTxTcp", "row": {"event_time": 1, "amount": 200.0}}),
        )
        .await
        .expect("apply_ops");
    assert_eq!(resp.status().as_u16(), 200);
    let apply_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        apply_body["kept"], true,
        "amount=200 > 100 should be kept: {apply_body:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

// ─── SC2: with_columns adds derived field visible downstream ──────────────────

/// SC2: Event.with_columns adds a derived column visible to downstream nodes.
/// Proven via:
///   1. GET /registry showing the server-propagated schema with `is_big: bool`.
///   2. A downstream derivation `OnlyBig` that filters on `is_big` registering
///      successfully (field-reference resolution proves `is_big` is in scope).
///   3. POST /dev/apply_ops confirming `is_big` appears in the output row.
#[tokio::test]
async fn sc2_with_columns_adds_derived_field_visible_downstream() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    // Register Transaction + TaggedTx (with_columns adds is_big).
    let body = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Transaction",
                "schema": {
                    "fields": {"event_time": "i64", "amount": "f64"},
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": "TaggedTx",
                "output_kind": "event",
                "upstreams": ["Transaction"],
                "ops": [{"op": "with_columns", "exprs": {"is_big": "(amount > 500)"}}],
                "schema": {
                    "fields": {"event_time": "i64", "amount": "f64", "is_big": "bool"},
                    "optional_fields": []
                }
            }
        ]
    });
    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(resp.status().as_u16(), 200);
    let reg: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        reg["registry_version"], 1,
        "first register should produce v1: {reg:#}"
    );

    // GET /registry → server-propagated schema for TaggedTx must include is_big: bool.
    let registry_dump = ts.get_json("/registry").await;
    let tagged_schema = &registry_dump["derivations"]["TaggedTx"]["schema"]["fields"];
    assert_eq!(
        tagged_schema["is_big"], "bool",
        "server-propagated schema must include is_big:bool: {registry_dump:#}"
    );

    // Register downstream OnlyBig that filters on is_big — proves field-ref resolution.
    let downstream = json!({
        "nodes": [
            {
                "kind": "derivation",
                "name": "OnlyBig",
                "output_kind": "event",
                "upstreams": ["TaggedTx"],
                "ops": [{"op": "filter", "expr": "(is_big == true)"}],
                "schema": {
                    "fields": {"event_time": "i64", "amount": "f64", "is_big": "bool"},
                    "optional_fields": []
                }
            }
        ]
    });
    let resp = ts
        .post_json("/register", &downstream)
        .await
        .expect("downstream register");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "downstream derivation referencing is_big must register successfully"
    );

    // /dev/apply_ops on TaggedTx → is_big should be true for amount=1000.
    let resp = ts
        .post_json(
            "/dev/apply_ops",
            &json!({"derivation": "TaggedTx", "row": {"event_time": 1, "amount": 1000.0}}),
        )
        .await
        .expect("apply_ops");
    assert_eq!(resp.status().as_u16(), 200);
    let apply_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(apply_body["kept"], true);
    assert_eq!(
        apply_body["row"]["is_big"], true,
        "amount=1000 > 500 → is_big must be true: {apply_body:#}"
    );

    // /dev/apply_ops on OnlyBig → is_big=true row is kept.
    let resp = ts
        .post_json(
            "/dev/apply_ops",
            &json!({"derivation": "OnlyBig", "row": {"event_time": 1, "amount": 1000.0, "is_big": true}}),
        )
        .await
        .expect("apply_ops onlybig");
    assert_eq!(resp.status().as_u16(), 200);
    let apply_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        apply_body["kept"], true,
        "is_big=true row should pass OnlyBig filter: {apply_body:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

// ─── SC3: chained ops compose correctly ──────────────────────────────────────

/// SC3: Chained ops filter → select → with_columns → cast compose correctly;
/// schema propagates through every step.
#[tokio::test]
async fn sc3_chained_ops_filter_select_with_columns_cast_schema_propagates() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    // Register Transaction + ChainedDeriv (4-op chain).
    let body = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Transaction",
                "schema": {
                    "fields": {"event_time": "i64", "amount": "f64"},
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": "ChainedDeriv",
                "output_kind": "event",
                "upstreams": ["Transaction"],
                "ops": [
                    {"op": "filter",       "expr": "(amount > 0)"},
                    {"op": "select",       "fields": ["event_time", "amount"]},
                    {"op": "with_columns", "exprs": {"is_big": "(amount > 500)"}},
                    {"op": "cast",         "type_map": {"is_big": "int"}}
                ],
                "schema": {
                    "fields": {"event_time": "i64", "amount": "f64", "is_big": "i64"},
                    "optional_fields": []
                }
            }
        ]
    });
    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "4-op chain register must succeed"
    );

    // GET /registry → ChainedDeriv.schema.fields must have is_big: i64 (cast from bool).
    let registry_dump = ts.get_json("/registry").await;
    let chained_schema = &registry_dump["derivations"]["ChainedDeriv"]["schema"]["fields"];
    assert_eq!(
        chained_schema["event_time"], "i64",
        "event_time must be i64 in propagated schema: {chained_schema:#}"
    );
    assert_eq!(
        chained_schema["amount"], "f64",
        "amount must be f64 in propagated schema: {chained_schema:#}"
    );
    assert_eq!(
        chained_schema["is_big"], "i64",
        "is_big must be i64 after cast: {chained_schema:#}"
    );

    // apply_ops with amount=1000 (passes filter, is_big → cast to 1).
    let resp = ts
        .post_json(
            "/dev/apply_ops",
            &json!({"derivation": "ChainedDeriv", "row": {"event_time": 10, "amount": 1000.0}}),
        )
        .await
        .expect("apply_ops pass");
    assert_eq!(resp.status().as_u16(), 200);
    let apply_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        apply_body["kept"], true,
        "amount=1000 > 0 should pass filter: {apply_body:#}"
    );
    assert_eq!(
        apply_body["row"]["is_big"], 1,
        "Bool(true) cast to int = 1: {apply_body:#}"
    );
    assert_eq!(
        apply_body["row"]["amount"], 1000.0,
        "amount should be preserved: {apply_body:#}"
    );

    // apply_ops with amount=-5.0 (fails filter → dropped).
    let resp = ts
        .post_json(
            "/dev/apply_ops",
            &json!({"derivation": "ChainedDeriv", "row": {"event_time": 10, "amount": -5.0}}),
        )
        .await
        .expect("apply_ops fail");
    assert_eq!(resp.status().as_u16(), 200);
    let apply_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        apply_body["kept"], false,
        "amount=-5 < 0 should be dropped: {apply_body:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

// ─── SC5 (HTTP): malformed predicate → 400 with path ─────────────────────────

/// SC5 (HTTP): Malformed predicate at REGISTER returns 400 with
/// code="invalid_expression" and path pointing to the offending op.
#[tokio::test]
async fn sc5_malformed_predicate_returns_400_with_path_http() {
    let ts = TestServer::spawn().await.expect("spawn");

    // Register with an unterminated filter expression.
    let body = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Transaction",
                "schema": {
                    "fields": {"event_time": "i64", "amount": "f64"},
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": "Bad",
                "output_kind": "event",
                "upstreams": ["Transaction"],
                "ops": [{"op": "filter", "expr": "(amount > "}],
                "schema": {
                    "fields": {"event_time": "i64", "amount": "f64"},
                    "optional_fields": []
                }
            }
        ]
    });
    let resp = ts
        .post_json("/register", &body)
        .await
        .expect("register post");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "malformed expression must return 400"
    );
    let error_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        error_body["error"]["code"], "invalid_expression",
        "code must be 'invalid_expression': {error_body:#}"
    );
    let path = error_body["error"]["path"]
        .as_str()
        .expect("path must be a string");
    assert!(path.contains("ops[0]"), "path must point to ops[0]: {path}");
    let reason = error_body["error"]["reason"]
        .as_str()
        .expect("reason must be a string");
    assert!(
        !reason.is_empty(),
        "reason must be non-empty: {error_body:#}"
    );

    ts.shutdown().await.expect("shutdown");
}

// ─── SC5 (TCP): malformed predicate → 0xFFFF error frame ─────────────────────

/// SC5 (TCP): Same malformed-predicate payload sent over TCP returns an
/// op=OP_ERROR_RESPONSE frame with code="invalid_expression".
#[tokio::test]
async fn sc5_malformed_predicate_returns_error_frame_tcp() {
    let ts = TestServer::spawn().await.expect("spawn");
    let mut c = ts.tcp_client().await.expect("tcp client");

    // Register Transaction first (needed for derivation upstream resolution).
    let event_body = transaction_event_payload();
    let (op, _) = c.register_json(event_body).await.expect("register event");
    assert_eq!(op, OP_REGISTER);

    // Register derivation with bad filter expression.
    let bad_deriv = json!({
        "nodes": [{
            "kind": "derivation",
            "name": "Bad",
            "output_kind": "event",
            "upstreams": ["Transaction"],
            "ops": [{"op": "filter", "expr": "(amount > "}],
            "schema": {
                "fields": {"event_time": "i64", "amount": "f64"},
                "optional_fields": []
            }
        }]
    });
    let (op, tcp_body) = c.register_json(bad_deriv).await.expect("register bad");
    assert_eq!(
        op, OP_ERROR_RESPONSE,
        "bad expression must return error frame: {tcp_body:#}"
    );
    assert_eq!(
        tcp_body["error"]["code"], "invalid_expression",
        "code must be 'invalid_expression': {tcp_body:#}"
    );
    let path = tcp_body["error"]["path"]
        .as_str()
        .expect("path must be present in TCP error frame");
    assert!(path.contains("ops[0]"), "path must point to ops[0]: {path}");

    ts.shutdown().await.expect("shutdown");
}

// ─── phase4_compiled_chain_is_retrievable_post_register ───────────────────────

/// Contract test: Registry::compiled_chain returns Some(Arc<OpChain>) after a
/// derivation with ops is registered, and calling chain.apply(row) in-process
/// agrees with what POST /dev/apply_ops returns for the same row.
/// Establishes the contract Phase 5's apply loop will use.
#[tokio::test]
async fn phase4_compiled_chain_is_retrievable_post_register() {
    let ts = TestServerBuilder::new()
        .dev_endpoints(true)
        .spawn()
        .await
        .expect("spawn");

    // Register Transaction + BigTx filter derivation.
    let body = transaction_plus_filter_payload("BigTx", "(amount > 100)");
    let resp = ts.post_json("/register", &body).await.expect("register");
    assert_eq!(resp.status().as_u16(), 200, "register must succeed");

    // Retrieve the compiled chain from the registry directly.
    let chain = ts
        .registry()
        .compiled_chain("BigTx")
        .expect("compiled_chain must return Some(Arc<OpChain>) after registration");

    // Apply a passing row in-process.
    let passing_row = Row::new()
        .with_field("event_time", Value::I64(1000))
        .with_field("amount", Value::F64(150.0));
    let chain_result = chain.apply(passing_row);
    assert!(
        chain_result.is_some(),
        "in-process apply: amount=150 > 100 should pass filter"
    );

    // Apply a dropping row in-process.
    let dropping_row = Row::new()
        .with_field("event_time", Value::I64(1000))
        .with_field("amount", Value::F64(50.0));
    let chain_result = chain.apply(dropping_row);
    assert!(
        chain_result.is_none(),
        "in-process apply: amount=50 < 100 should be dropped"
    );

    // Compare in-process result with /dev/apply_ops response for the same row.
    let resp = ts
        .post_json(
            "/dev/apply_ops",
            &json!({"derivation": "BigTx", "row": {"event_time": 1000, "amount": 150.0}}),
        )
        .await
        .expect("apply_ops");
    assert_eq!(resp.status().as_u16(), 200);
    let apply_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        apply_body["kept"], true,
        "/dev/apply_ops must agree with in-process chain.apply for passing row: {apply_body:#}"
    );

    let resp = ts
        .post_json(
            "/dev/apply_ops",
            &json!({"derivation": "BigTx", "row": {"event_time": 1000, "amount": 50.0}}),
        )
        .await
        .expect("apply_ops drop");
    assert_eq!(resp.status().as_u16(), 200);
    let apply_body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        apply_body["kept"], false,
        "/dev/apply_ops must agree with in-process chain.apply for dropping row: {apply_body:#}"
    );

    ts.shutdown().await.expect("shutdown");
}
