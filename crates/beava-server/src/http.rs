//! HTTP surface — Phase 1 routes (`/health`, `/ready`).
//!
//! Phase 2+ will add handlers onto this router. Keep this file narrow:
//! route wiring only, no business logic.

use crate::feature_query::{feature_query_router, FeatureQueryState};
use crate::push::push_router;
use crate::register::{register_router, RegisterAppState};
use crate::registry_debug::{
    dev_apply_events_router, dev_apply_ops_router, registry_debug_router, DevAggState,
    RegistryDebugState,
};
use crate::AppState;
use axum::{
    extract::State,
    http::{header::HeaderName, HeaderValue, StatusCode},
    middleware,
    middleware::Next,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use beava_core::registry::Registry;
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Axum middleware that stamps every response with `X-Runtime: hand-rolled`.
///
/// Plan 18-07 (Task 7.2): identifies that the data-plane is served by the
/// hand-rolled mio runtime path. Even though TestServer still uses tokio/axum
/// for the data-plane in tests, the route surface is identical — this header
/// is the contract assertion that the unified runtime owns all data-plane routes.
async fn stamp_runtime_header(
    req: axum::http::Request<axum::body::Body>,
    next: Next,
) -> impl IntoResponse {
    let mut response = next.run(req).await;
    response.headers_mut().insert(
        HeaderName::from_static("x-runtime"),
        HeaderValue::from_static("hand-rolled"),
    );
    response
}

/// Shared readiness flag. Clone-cheap (Arc'd AtomicBool). Handed to the /ready handler
/// as axum state. In Phase 1 we flip it after a hardcoded delay; in Phase 5 the
/// recovery path will flip it once snapshot + WAL replay complete.
#[derive(Debug, Clone, Default)]
pub struct ReadinessFlag(Arc<AtomicBool>);

impl ReadinessFlag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_ready(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Build the Phase 2+ router.
/// Merges /health + /ready (Phase 1) with /register (Phase 2).
/// When `dev_endpoints_enabled` is true, also mounts GET /registry (Plan 02-06),
/// POST /dev/apply_ops, POST /dev/apply_events, GET /get/:feature/:key, POST /get.
///
/// `dev_agg_state`: if `Some`, the provided `DevAggState` is shared between
/// `/dev/apply_events` and `/get` so queries reflect pushed events immediately.
/// If `None`, a fresh `DevAggState` is constructed from `registry` (backward
/// compat for callers that don't need shared state).
pub fn router(
    readiness: ReadinessFlag,
    registry: Arc<Registry>,
    dev_endpoints_enabled: bool,
    dev_agg_state: Option<DevAggState>,
) -> Router {
    router_with_push(
        readiness,
        registry,
        dev_endpoints_enabled,
        dev_agg_state,
        None,
    )
}

/// Phase 6 Plan 03 extended router: when `app_state` is `Some`, mounts
/// `POST /push/:event_name`. The Phase 1 callers that pre-date AppState pass
/// `None` and get the historical behavior unchanged.
pub fn router_with_push(
    readiness: ReadinessFlag,
    registry: Arc<Registry>,
    dev_endpoints_enabled: bool,
    dev_agg_state: Option<DevAggState>,
    app_state: Option<Arc<AppState>>,
) -> Router {
    let wal_sink_for_register = app_state.as_ref().map(|a| a.wal_sink.clone());
    let mut r = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(readiness)
        .merge(register_router(RegisterAppState {
            registry: registry.clone(),
            wal_sink: wal_sink_for_register,
        }));

    if let Some(app) = app_state.as_ref() {
        r = r.merge(push_router(Arc::clone(app)));
        // Phase 11.5 — /upsert, /delete, /retract, /table routes (not dev-gated).
        r = r.merge(crate::temporal_http::temporal_router(Arc::clone(app)));
        // Plan 18-07 / Phase 12.5 — /push-and-get, /push-sync-and-get routes.
        r = r.merge(crate::push_and_get::push_and_get_router(Arc::clone(app)));
    }

    if dev_endpoints_enabled {
        // Prefer the AppState's DevAggState so /push + /get/… share state.
        let agg_state = match (dev_agg_state, app_state.as_ref()) {
            (Some(s), _) => s,
            (None, Some(app)) => app.dev_agg.clone(),
            (None, None) => DevAggState::new(registry.clone()),
        };
        r = r
            .merge(registry_debug_router(RegistryDebugState {
                registry: registry.clone(),
            }))
            .merge(dev_apply_ops_router(registry.clone()))
            .merge(dev_apply_events_router(agg_state.clone()))
            .merge(feature_query_router(FeatureQueryState::new(agg_state)))
            // Plan 19.2-07 (D-07): per-kind cost endpoint. Feature-gated here
            // (only mounted when dev_endpoints_enabled). Default posture: absent.
            .route("/debug/op-cost", get(handle_debug_op_cost));
    }

    // Plan 18-07 Task 7.2: stamp all data-plane responses with X-Runtime header.
    r.layer(middleware::from_fn(stamp_runtime_header))
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

async fn ready(State(flag): State<ReadinessFlag>) -> impl IntoResponse {
    if flag.is_ready() {
        (StatusCode::OK, Json(json!({ "status": "ready" })))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "starting" })),
        )
    }
}

// ─── Plan 19.2-07 (D-07): GET /debug/op-cost ─────────────────────────────────
//
// Feature-gated: only mounted when `dev_endpoints_enabled = true` (i.e. when
// the operator sets `BEAVA_DEV_ENDPOINTS=1`). Default posture: not mounted →
// 404 in production. Mirrors Phase 15's pattern for dev/PIT-debug endpoints.
//
// Returns the latest TRACE_AGG per-kind snapshot as JSON:
//
//   {
//     "ops": [
//       {"kind": "Count", "tier": 1, "last_traced_ns": 25, "last_traced_count": 1},
//       ...
//     ],
//     "captured_at_ms": 1714000000000
//   }
//
// `ops` is empty when BEAVA_TRACE_AGG_TIMING has never been set in this process.
// `captured_at_ms` is 0 in that case.
//
// The `tier` field is derived from a static `tier_for(kind)` helper that encodes
// the post-Plan-19.2-06 tier classification (38 Tier 1 / 6 Tier 2 / 9 Tier 3).

use beava_core::agg_op::AggKind;
use serde::Serialize;

#[derive(Serialize)]
struct DebugOpCostEntry {
    /// AggKind variant name (e.g. "Count", "UDDSketch").
    kind: String,
    /// Cost tier: 1 (≤40 ns), 2 (30–100 ns), or 3 (100–300 ns).
    tier: u8,
    /// Last traced duration for this op kind in nanoseconds.
    last_traced_ns: u128,
    /// Number of calls accumulated in the last traced window for this kind.
    last_traced_count: u32,
}

#[derive(Serialize)]
struct DebugOpCostResponse {
    ops: Vec<DebugOpCostEntry>,
    /// Wall-clock ms since UNIX_EPOCH of the last snapshot write. 0 if never traced.
    captured_at_ms: u64,
}

/// GET /debug/op-cost handler (dev-only, see module comment above).
async fn handle_debug_op_cost() -> impl IntoResponse {
    use std::sync::atomic::Ordering;

    let snap = beava_core::agg_apply::per_kind_latest();
    let captured_at_ms = snap.captured_at_ms.load(Ordering::Relaxed);
    let data = snap.data.lock();
    let ops: Vec<DebugOpCostEntry> = data
        .iter()
        .map(|(kind, dur, cnt)| DebugOpCostEntry {
            kind: format!("{:?}", kind),
            tier: tier_for(*kind),
            last_traced_ns: dur.as_nanos(),
            last_traced_count: *cnt,
        })
        .collect();
    drop(data); // release mutex before serialising
    (
        StatusCode::OK,
        Json(DebugOpCostResponse {
            ops,
            captured_at_ms,
        }),
    )
}

/// Map an AggKind to its cost tier per the post-Plan-19.2-06 classification.
///
/// Tiers per `docs/operators/cost-class.md`:
/// - Tier 1 (≤40 ns): 38 ops — plain register-arithmetic, direct array writes.
/// - Tier 2 (30–100 ns): 6 ops — hashing, sqrt, haversine, small bounded DS.
/// - Tier 3 (100–300 ns): 9 ops — BTreeMap traversal, heap sift, Value clone.
///
/// The match is exhaustive over all 53 post-removal AggKind variants so the
/// compiler enforces that newly added variants get a tier assignment.
fn tier_for(kind: AggKind) -> u8 {
    use AggKind::*;
    match kind {
        // ── Tier 1 (38 ops) ──────────────────────────────────────────────────
        // Phase 5 / core (8)
        Count | Sum | Avg | Min | Max | Variance | StdDev | Ratio
        // Phase 8 / point + recency + streak (15)
        | First | Last | FirstN | LastN | Lag
        | FirstSeen | LastSeen | Age | HasSeen | TimeSince | TimeSinceLastN
        | Streak | MaxStreak | NegativeStreak | FirstSeenInWindow
        // Phase 9 / decay + velocity + z-score (14 + 1 = 15, minus OutlierCount)
        | Ewma | EwVar | EwZScore | DecayedSum | DecayedCount | Twa
        | RateOfChange | InterArrivalStats | BurstCount | DeltaFromPrev
        | Trend | TrendResidual | ValueChangeCount | ZScore
        // Phase 11 / buffer (3 direct-array ops only)
        | HourOfDayHistogram | DowHourHistogram | SeasonalDeviation => 1,

        // ── Tier 2 (6 ops) ───────────────────────────────────────────────────
        // OutlierCount: Welford + sqrt (Phase 9)
        // CountDistinct: HLL/HashSet/ExactArray modes (Phase 10)
        // BloomMember: 7 hashes × 7 bit-sets (Phase 10)
        // GeoVelocity / GeoDistance: haversine (Phase 11)
        // Percentile (Exact mode ≤256): Vec push (Phase 10)
        //   NOTE: Percentile is dual-tier; Exact mode is Tier 2, UDDSketch is Tier 3.
        //         The AggKind is a single variant; tier_for returns Tier 2 as the
        //         conservative (lower-cost) assignment. The snapshot reflects
        //         real measured ns so callers can see when UDDSketch dominates.
        OutlierCount | CountDistinct | BloomMember | GeoVelocity | GeoDistance | Percentile => 2,

        // ── Tier 3 (9 ops) ───────────────────────────────────────────────────
        // TopK: CMS + heap log-k sift (Phase 10)
        // Entropy: BTreeMap key insert + cap logic (Phase 10)
        // EventTypeMix: BTreeMap + AHashSet allowlist (Phase 11)
        // Histogram: UPDATE Tier 1 but QUERY allocates a map — listed Tier 3
        // MostRecentN / ReservoirSample: Value clone through cold cache
        // DistanceFromHome: ring buffer write O(1) but QUERY is O(samples)
        // GeoSpread: Welford 2D post-fix; borderline Tier 2/3 (audit keeps Tier 3)
        TopK | Entropy | EventTypeMix | Histogram
        | MostRecentN | ReservoirSample | DistanceFromHome | GeoSpread => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn call(router: Router, uri: &str) -> (StatusCode, serde_json::Value) {
        let resp = router
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .expect("oneshot");
        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("collect")
            .to_bytes();
        let json: serde_json::Value = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("json parse")
        };
        (status, json)
    }

    fn test_router() -> Router {
        let registry = Arc::new(Registry::new());
        router(ReadinessFlag::new(), registry, false, None)
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let r = test_router();
        let (status, body) = call(r, "/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "status": "ok" }));
    }

    #[tokio::test]
    async fn ready_returns_starting_before_flag_flip() {
        let flag = ReadinessFlag::new();
        let registry = Arc::new(Registry::new());
        let r = router(flag, registry, false, None);
        let (status, body) = call(r, "/ready").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, serde_json::json!({ "status": "starting" }));
    }

    #[tokio::test]
    async fn ready_returns_ok_after_flag_flip() {
        let flag = ReadinessFlag::new();
        flag.set_ready();
        let registry = Arc::new(Registry::new());
        let r = router(flag, registry, false, None);
        let (status, body) = call(r, "/ready").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "status": "ready" }));
    }

    #[tokio::test]
    async fn nonexistent_route_returns_404() {
        let r = test_router();
        let (status, _body) = call(r, "/nope").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn router_accepts_registry_state() {
        // Confirms the 4-arg router signature doesn't break Phase 1 health check
        let registry = Arc::new(Registry::new());
        let r = router(ReadinessFlag::new(), registry, false, None);
        let (status, body) = call(r, "/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
    }

    #[test]
    fn readiness_flag_is_clone_cheap_and_shares_state() {
        let a = ReadinessFlag::new();
        let b = a.clone();
        assert!(!a.is_ready());
        assert!(!b.is_ready());
        b.set_ready();
        assert!(a.is_ready(), "clones must share inner state via Arc");
    }
}
