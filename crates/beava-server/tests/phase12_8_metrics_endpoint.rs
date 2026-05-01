//! Phase 12.8 Plan 06 — RED tests for 5 new memory-governance Prometheus metrics
//! + env-gate-zero-disables-enforcement test (moved from Plan 04 per Plan 06 frontmatter).
//!
//! Per CONTEXT.md "Memory governance instrumentation" + Plan 06 objective, the
//! `/metrics` admin endpoint exposes 5 new metric families:
//!
//! 1. `beava_cold_entity_evictions_total` (counter) — Plan 03 cold-TTL eviction
//!    firings (increments inside `agg_apply.rs` when `evict_entity_by_shape_if_cold`
//!    returns `true`).
//! 2. `beava_lifetime_op_cap_hit_total` (counter) — aggregate cap-hit count
//!    across lifetime aggregation operators (currently wraps the existing
//!    `EntropyStateWrap::categories_capped_count`; future expansion: top_k
//!    displacements + histogram bucket drops).
//! 3. `beava_entity_count_resident` (gauge) — current resident entity count
//!    summed across all `AggStateTable` sub-maps. Sampled at apply-time and
//!    written into a process-static `AtomicUsize` snapshot the admin sidecar
//!    reads with `.load(Relaxed)` (zero-lock admin path).
//! 4. `beava_bucket_reclaim_total` (counter) — `WindowedOp::evict_oldest_bucket`
//!    firings (increments at the apply-side eviction site in `agg_windowed.rs`).
//! 5. `beava_bytes_per_entity_p99` (gauge) — static v0 estimate (~7000 bytes per
//!    PROJECT.md memory budget). Phase 13 ship-gate may upgrade to dynamic
//!    sampling.
//!
//! v0 ships UNLABELED counters (no `{source=...}` block). Per-source labels
//! are deferred to v0.0.x; this trade is documented in CLAUDE.md § Memory
//! Governance Invariant block (Plan 09 docs landing).
//!
//! ─── Plan 06 env-gate flip caveat ──────────────────────────────────────────────
//! Plan 06 also flips `BEAVA_MEMORY_GOV_ENFORCE` default from OFF→ON. The
//! `test_env_var_zero_disables_enforcement` test (test 21, originally scoped
//! into Plan 04) lives here per the wave-3 ownership shift documented in the
//! Plan 06 frontmatter `must_haves.truths[0]`. It confirms the explicit
//! `BEAVA_MEMORY_GOV_ENFORCE=0` escape hatch.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;
use std::time::Duration;

// ─── Helpers ────────────────────────────────────────────────────────────────

const ENV_KEY: &str = "BEAVA_MEMORY_GOV_ENFORCE";

/// Process-global env mutex. Mirrors the pattern in
/// `phase12_8_unbounded_op_in_lifetime_mode.rs`. Required because Plan 06
/// reads the env per-call (no OnceLock cache) and tests in this file run in
/// the same process; without the mutex, set/remove from one test races
/// arbitrary other tests' register paths.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn enforce_off() {
    std::env::remove_var(ENV_KEY);
}

fn enforce_zero() {
    std::env::set_var(ENV_KEY, "0");
}

/// GET /metrics on the admin sidecar; return the response body.
async fn fetch_metrics(ts: &TestServer) -> String {
    let url = format!("{}/metrics", ts.admin_url());
    reqwest::get(&url)
        .await
        .expect("/metrics request")
        .text()
        .await
        .expect("/metrics body")
}

/// Helper: scrape the value of a Prometheus counter/gauge line.
/// Returns the value as a u64 if the line is present and parseable, otherwise None.
fn scrape_metric_value(body: &str, name: &str) -> Option<u64> {
    for line in body.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix(name) {
            // Possible forms:
            //   "beava_foo 5"
            //   "beava_foo{label=\"bar\"} 5"
            // Must be terminated by whitespace or '{' to avoid prefix-match false hits.
            let ch = rest.chars().next();
            match ch {
                Some(' ') | Some('\t') | Some('{') => {}
                _ => continue,
            }
            // Last whitespace-separated token = value
            if let Some(value_str) = trimmed.split_whitespace().last() {
                if let Ok(v) = value_str.parse::<u64>() {
                    return Some(v);
                }
            }
        }
    }
    None
}

/// Register a payload with `Txn` event source + `TxnAgg` count derivation.
fn register_payload_count(cold_after_ms: Option<u64>) -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                },
                "cold_after_ms": cold_after_ms,
            },
            {
                "kind": "derivation",
                "name": "TxnAgg",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                    "cnt": {"op": "count", "params": {}}
                }}],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    })
}

/// Register a payload with a windowed entropy on `category` — exercises
/// `EntropyStateWrap::categories_capped_count` cap-hits when many distinct
/// values arrive.
fn register_payload_entropy(max_categories: u64) -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {"user_id": "str", "category": "str"},
                    "optional_fields": []
                },
            },
            {
                "kind": "derivation",
                "name": "TxnEntropy",
                "output_kind": "event",
                "upstreams": ["Txn"],
                "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                    "ent": {"op": "entropy", "params": {"field": "category", "max_categories": max_categories, "window": "1h"}}
                }}],
                "schema": {
                    "fields": {"user_id": "str", "ent": "f64"},
                    "optional_fields": []
                }
            }
        ]
    })
}

/// Register a payload with a windowed `count` on a short window (~10ms) so that
/// each ~30ms-spaced push lands in a fresh bucket. Used to exercise
/// `WindowedOp::evict_oldest_bucket` (the 64-bucket cap is hit after ~64 pushes
/// across distinct epochs).
fn register_payload_windowed_count() -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                },
            },
            {
                "kind": "derivation",
                "name": "TxnWindowed",
                "output_kind": "event",
                "upstreams": ["Txn"],
                "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                    // window = 10s, but with bucket_ms = 10ms ⇒ each new
                    // 10ms-distinct epoch = a new bucket. The 64-bucket cap
                    // fires after >64 distinct epochs.
                    "cnt_w": {"op": "count", "params": {"window": "10s", "bucket": "10ms"}}
                }}],
                "schema": {
                    "fields": {"user_id": "str", "cnt_w": "i64"},
                    "optional_fields": []
                }
            }
        ]
    })
}

async fn push_user(ts: &TestServer, user_id: &str, amount: f64) {
    let body = json!({"user_id": user_id, "amount": amount});
    let resp = ts.post_json("/push/Txn", &body).await.expect("push");
    let status = resp.status();
    assert!(
        status.is_success(),
        "push for user_id={user_id} returned {status}, body={}",
        resp.text().await.unwrap_or_default()
    );
}

async fn push_user_category(ts: &TestServer, user_id: &str, category: &str) {
    let body = json!({"user_id": user_id, "category": category});
    let resp = ts.post_json("/push/Txn", &body).await.expect("push");
    let status = resp.status();
    assert!(
        status.is_success(),
        "push for user_id={user_id} category={category} returned {status}, body={}",
        resp.text().await.unwrap_or_default()
    );
}

// ─── Tests ──────────────────────────────────────────────────────────────────

/// 1. The 5 new HELP lines must appear on `/metrics` once Plan 06 lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_metrics_endpoint_includes_5_new_help_lines() {
    let _guard = ENV_LOCK.lock().await;
    enforce_off();

    let ts = TestServer::spawn().await.expect("spawn");

    let body = fetch_metrics(&ts).await;

    let expected_help_lines = [
        "# HELP beava_cold_entity_evictions_total",
        "# HELP beava_lifetime_op_cap_hit_total",
        "# HELP beava_entity_count_resident",
        "# HELP beava_bucket_reclaim_total",
        "# HELP beava_bytes_per_entity_p99",
    ];

    for help in expected_help_lines {
        assert!(
            body.contains(help),
            "metrics body must contain `{help}`; got:\n{body}"
        );
    }

    ts.shutdown().await.ok();
}

/// 2. `cold_entity_evictions_total` starts at zero before any eviction fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cold_entity_evictions_starts_at_zero() {
    let _guard = ENV_LOCK.lock().await;
    enforce_off();

    let ts = TestServer::spawn().await.expect("spawn");

    // Register a source that COULD evict (cold_after = 1d) but no events pushed
    // yet — counter must be either 0 or absent-with-default-zero.
    let payload = register_payload_count(Some(86_400_000));
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(resp.status().is_success(), "register failed");

    let body = fetch_metrics(&ts).await;
    let value = scrape_metric_value(&body, "beava_cold_entity_evictions_total")
        .expect("beava_cold_entity_evictions_total line missing");
    assert_eq!(
        value, 0,
        "no events pushed → cold_entity_evictions_total must be 0; got {value}\nbody:\n{body}"
    );

    ts.shutdown().await.ok();
}

/// 3. `cold_entity_evictions_total` increments when a cold entity is touched
///    after `cold_after_ms` has passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cold_entity_evictions_increments_on_eviction() {
    let _guard = ENV_LOCK.lock().await;
    enforce_off();

    let ts = TestServer::spawn().await.expect("spawn");

    // 100ms TTL — short enough that one tokio::time::sleep(150ms) crosses it.
    let payload = register_payload_count(Some(100));
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(resp.status().is_success(), "register failed");

    // Snapshot the counter pre-eviction.
    let before = scrape_metric_value(
        &fetch_metrics(&ts).await,
        "beava_cold_entity_evictions_total",
    )
    .expect("counter line missing pre-push");

    // Push for "alice" — establishes last_seen_ms.
    push_user(&ts, "alice", 10.0).await;

    // Sleep past the TTL so the next push triggers eviction for alice.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Push for "alice" again — apply path's cold-eviction check fires.
    push_user(&ts, "alice", 20.0).await;

    let after = scrape_metric_value(
        &fetch_metrics(&ts).await,
        "beava_cold_entity_evictions_total",
    )
    .expect("counter line missing post-eviction");

    assert!(
        after > before,
        "cold_entity_evictions_total did not increment past {before} after \
         post-TTL push (now: {after})"
    );

    ts.shutdown().await.ok();
}

/// 4. `entity_count_resident` reports total resident entity count after pushes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_entity_count_resident_reports_active_entities() {
    let _guard = ENV_LOCK.lock().await;
    enforce_off();

    let ts = TestServer::spawn().await.expect("spawn");

    let payload = register_payload_count(None);
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(resp.status().is_success(), "register failed");

    push_user(&ts, "alice", 1.0).await;
    push_user(&ts, "bob", 2.0).await;
    push_user(&ts, "carol", 3.0).await;

    let body = fetch_metrics(&ts).await;
    let count = scrape_metric_value(&body, "beava_entity_count_resident")
        .expect("beava_entity_count_resident line missing");
    assert!(
        count >= 3,
        "entity_count_resident must reflect ≥3 entities after pushes for alice/bob/carol; \
         got {count}\nbody:\n{body}"
    );

    ts.shutdown().await.ok();
}

/// 5. `bucket_reclaim_total` increments when WindowedOp::evict_oldest_bucket
///    fires (the 64-bucket cap is hit after >64 distinct-epoch pushes).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_bucket_reclaim_total_increments_on_eviction() {
    let _guard = ENV_LOCK.lock().await;
    enforce_off();

    let ts = TestServer::spawn().await.expect("spawn");

    let payload = register_payload_windowed_count();
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(
        resp.status().is_success(),
        "register failed: {}",
        resp.text().await.unwrap_or_default()
    );

    let before = scrape_metric_value(&fetch_metrics(&ts).await, "beava_bucket_reclaim_total")
        .expect("counter line missing pre-push");

    // Push 70 events spaced ~12ms apart on a 10ms-bucket window → ~70 distinct
    // epochs. The 64-bucket cap fires after the 65th, 66th, ...; final count
    // should be ≥ 6 evictions. Any wall-clock variance amplifies the count
    // (more buckets) rather than reducing it — never makes the test flaky in
    // the FALSE-NEGATIVE direction.
    for i in 0..70 {
        push_user(&ts, "alice", i as f64).await;
        tokio::time::sleep(Duration::from_millis(12)).await;
    }

    let after = scrape_metric_value(&fetch_metrics(&ts).await, "beava_bucket_reclaim_total")
        .expect("counter line missing post-pushes");

    assert!(
        after > before,
        "bucket_reclaim_total did not increment past {before} after 70 \
         distinct-epoch pushes on a 64-bucket-cap windowed op (now: {after})"
    );

    ts.shutdown().await.ok();
}

/// 6. `lifetime_op_cap_hit_total` includes the existing
///    `EntropyStateWrap::categories_capped_count` count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lifetime_op_cap_hit_total_includes_entropy_capped() {
    let _guard = ENV_LOCK.lock().await;
    enforce_off();

    let ts = TestServer::spawn().await.expect("spawn");

    // max_categories=2 — push 5 distinct categories so the 3rd, 4th, 5th hit
    // the cap and increment the counter.
    let payload = register_payload_entropy(2);
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(
        resp.status().is_success(),
        "register failed: {}",
        resp.text().await.unwrap_or_default()
    );

    let before = scrape_metric_value(
        &fetch_metrics(&ts).await,
        "beava_lifetime_op_cap_hit_total",
    )
    .expect("counter line missing pre-push");

    for cat in &["a", "b", "c", "d", "e"] {
        push_user_category(&ts, "alice", cat).await;
    }

    let after = scrape_metric_value(
        &fetch_metrics(&ts).await,
        "beava_lifetime_op_cap_hit_total",
    )
    .expect("counter line missing post-push");

    assert!(
        after > before,
        "lifetime_op_cap_hit_total did not increment past {before} after \
         5-distinct-categories push on max_categories=2 entropy op (now: {after})"
    );

    ts.shutdown().await.ok();
}

/// 7. `bytes_per_entity_p99` reports the static v0 estimate (~7000 bytes per
///    PROJECT.md memory-budget line).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_bytes_per_entity_p99_reports_static_v0_estimate() {
    let _guard = ENV_LOCK.lock().await;
    enforce_off();

    let ts = TestServer::spawn().await.expect("spawn");

    let body = fetch_metrics(&ts).await;
    let value = scrape_metric_value(&body, "beava_bytes_per_entity_p99")
        .expect("beava_bytes_per_entity_p99 line missing");

    // PROJECT.md commitment: ~7KB per entity for a rich 30-feature pack. Plan
    // 06 ships a STATIC placeholder; Phase 13 ship-gate may upgrade to dynamic
    // sampling. Test asserts the canonical 7000 value (the placeholder) appears
    // — accepts a small tolerance window in case future work refines it
    // downward without breaking the v0 test.
    assert_eq!(
        value, 7000,
        "bytes_per_entity_p99 v0 placeholder must equal 7000 per PROJECT.md \
         memory budget; got {value}\nbody:\n{body}"
    );

    ts.shutdown().await.ok();
}

// ─── Plan 04 test 21 (moved here per Plan 06 frontmatter) ───────────────────

/// 8 (test 21 from Plan 04). After the Plan 06 default-OFF→ON env-gate flip,
/// users who explicitly set `BEAVA_MEMORY_GOV_ENFORCE=0` opt back into the
/// original "no enforcement" behavior. This is the documented escape hatch
/// per Plan 06 frontmatter `must_haves.truths[0]`.
///
/// Test: with `BEAVA_MEMORY_GOV_ENFORCE=0` set explicitly, registering a
/// windowless op that WOULD be rejected under the default-ON gate (e.g.
/// `histogram` without `buckets` per Plan 04 classification) must succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_env_var_zero_disables_enforcement() {
    let _guard = ENV_LOCK.lock().await;
    enforce_zero();

    let ts = TestServer::spawn().await.expect("spawn");

    // Histogram without `buckets` — under default-ON enforcement this would
    // be rejected with `unbounded_op_in_lifetime_mode`. With the explicit
    // "0" escape hatch the shim must short-circuit and accept.
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": "TxByUser",
                "output_kind": "event",
                "upstreams": ["Tx"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {
                            "h": {"op": "histogram", "params": {"field": "amount"}}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "h": "f64"},
                    "optional_fields": []
                }
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");

    enforce_off();
    ts.shutdown().await.ok();

    assert!(
        (200..300).contains(&status),
        "with BEAVA_MEMORY_GOV_ENFORCE=0, the 4th shim must short-circuit \
         (escape hatch) and accept registration of histogram-without-buckets \
         that would otherwise be rejected with unbounded_op_in_lifetime_mode; \
         got status={status}, body={body_text}"
    );
}
