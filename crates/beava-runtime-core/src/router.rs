//! Path dispatch for HTTP requests in the hand-rolled event loop (Phase 18).
//!
//! Simple match-based dispatch on path prefix. No regex, no macros —
//! direct pattern matching for zero overhead.

/// Recognized HTTP routes dispatched by the hand-rolled event loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// POST /push/:event_name — fire-and-forget push.
    Push { event_name: String },
    /// POST /push-sync/:event_name — synchronous push (awaits fsync).
    PushSync { event_name: String },
    /// POST /push-batch/:event_name — batched push.
    PushBatch { event_name: String },
    /// POST /get — batch feature read.
    Get,
    /// GET /get/:feature/:key — single feature read.
    GetSingle { feature: String, key: String },
    /// POST /upsert/:table — table upsert.
    Upsert { table: String },
    /// POST /delete/:table — table tombstone.
    Delete { table: String },
    /// POST /retract — retraction.
    Retract,
    /// GET /table/:table — point lookup (Plan 12.6-14). Query string
    /// `?key=<v>[&as_of=<lsn>]` carried in the parsed `WireRequest::HttpTableGet`.
    TableGet { table: String },
    /// POST /register — pipeline registration.
    Register,
    /// GET /health — liveness probe (Plan 12-07). Always 200 once listener
    /// is up; no apply-thread dependency.
    Health,
    /// GET /ready — readiness probe (Plan 12.6-01). Mirrors the legacy axum
    /// `/ready` route on the data-plane port for back-compat with the ~20
    /// test files that poll `ts.base_url()` for readiness.  The admin port
    /// also exposes `/ready` (canonical, per `project_phase18_no_dual_runtime`).
    Ready,
    /// GET /registry — registry snapshot dump (Plan 12.6-01). Mirrors the
    /// legacy axum `/registry` dev endpoint on the data-plane port for
    /// back-compat with phase4/5/11.5 tests that GET `/registry` to assert
    /// schema propagation.
    Registry,
    /// Path not in the table.
    NotFound,
    /// Path matched but wrong method.
    MethodNotAllowed,
}

/// Parse a path string into a `Route`.
///
/// Does NOT validate the HTTP method — that check is done at dispatch time
/// so the router can return `MethodNotAllowed` with the actual allowed methods.
pub struct Router;

impl Router {
    /// Dispatch `(method, path)` into a `Route`.
    pub fn route(method: &str, path: &str) -> Route {
        // Plan 12.6-14: strip query string for route matching. Path-segment
        // routes (e.g. `/upsert/:table`, `/table/:table`) capture the
        // trailing path component; the query string is parsed separately by
        // the dispatcher when needed (`HttpTableGet` carries query=...).
        let path = match path.split_once('?') {
            Some((p, _q)) => p,
            None => path,
        };
        // Normalise: strip trailing slash for tolerance.
        let path = path.trim_end_matches('/');

        // /push/:event_name
        if let Some(rest) = path.strip_prefix("/push/") {
            return if method == "POST" {
                Route::Push {
                    event_name: rest.to_owned(),
                }
            } else {
                Route::MethodNotAllowed
            };
        }
        // /push-sync/:event_name
        if let Some(rest) = path.strip_prefix("/push-sync/") {
            return if method == "POST" {
                Route::PushSync {
                    event_name: rest.to_owned(),
                }
            } else {
                Route::MethodNotAllowed
            };
        }
        // /push-batch/:event_name
        if let Some(rest) = path.strip_prefix("/push-batch/") {
            return if method == "POST" {
                Route::PushBatch {
                    event_name: rest.to_owned(),
                }
            } else {
                Route::MethodNotAllowed
            };
        }
        // /get/:feature/:key — must check before /get (exact match)
        if let Some(rest) = path.strip_prefix("/get/") {
            let parts: Vec<&str> = rest.splitn(2, '/').collect();
            if parts.len() == 2 {
                return if method == "GET" {
                    Route::GetSingle {
                        feature: parts[0].to_owned(),
                        key: parts[1].to_owned(),
                    }
                } else {
                    Route::MethodNotAllowed
                };
            }
        }
        // /get (exact)
        if path == "/get" {
            return if method == "POST" {
                Route::Get
            } else {
                Route::MethodNotAllowed
            };
        }
        // /upsert/:table
        if let Some(rest) = path.strip_prefix("/upsert/") {
            return if method == "POST" {
                Route::Upsert {
                    table: rest.to_owned(),
                }
            } else {
                Route::MethodNotAllowed
            };
        }
        // /delete/:table
        if let Some(rest) = path.strip_prefix("/delete/") {
            return if method == "POST" {
                Route::Delete {
                    table: rest.to_owned(),
                }
            } else {
                Route::MethodNotAllowed
            };
        }
        // /retract
        if path == "/retract" {
            return if method == "POST" {
                Route::Retract
            } else {
                Route::MethodNotAllowed
            };
        }
        // /table/:table — point lookup (Plan 12.6-14)
        if let Some(rest) = path.strip_prefix("/table/") {
            return if method == "GET" {
                Route::TableGet {
                    table: rest.to_owned(),
                }
            } else {
                Route::MethodNotAllowed
            };
        }
        // /register
        if path == "/register" {
            return if method == "POST" {
                Route::Register
            } else {
                Route::MethodNotAllowed
            };
        }
        // /health (Plan 12-07) — liveness probe; read_bench.py startup contract.
        if path == "/health" {
            return if method == "GET" {
                Route::Health
            } else {
                Route::MethodNotAllowed
            };
        }
        // /ready (Plan 12.6-01) — readiness probe on the data-plane port.
        // Mirrors the admin sidecar's /ready for back-compat with foundation /
        // phase1 / phase7 tests that check readiness on `ts.base_url()`.
        if path == "/ready" {
            return if method == "GET" {
                Route::Ready
            } else {
                Route::MethodNotAllowed
            };
        }
        // /registry (Plan 12.6-01) — registry snapshot dump on the data-plane
        // port. Mirrors the admin sidecar's /registry for back-compat with
        // phase4/5/11.5 tests that GET /registry to assert schema propagation.
        if path == "/registry" {
            return if method == "GET" {
                Route::Registry
            } else {
                Route::MethodNotAllowed
            };
        }

        Route::NotFound
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_push_post() {
        assert_eq!(
            Router::route("POST", "/push/Transaction"),
            Route::Push {
                event_name: "Transaction".to_owned()
            }
        );
    }

    #[test]
    fn route_push_wrong_method() {
        assert_eq!(Router::route("GET", "/push/Foo"), Route::MethodNotAllowed);
    }

    #[test]
    fn route_get_single() {
        assert_eq!(
            Router::route("GET", "/get/txn_count/user123"),
            Route::GetSingle {
                feature: "txn_count".to_owned(),
                key: "user123".to_owned(),
            }
        );
    }

    #[test]
    fn route_get_batch() {
        assert_eq!(Router::route("POST", "/get"), Route::Get);
    }

    /// Plan 12-07 — GET /health on the data-plane HTTP port routes to Route::Health.
    #[test]
    fn route_health_get() {
        assert_eq!(Router::route("GET", "/health"), Route::Health);
    }

    /// Plan 12-07 — POST /health is a method-not-allowed (matches /get behavior).
    #[test]
    fn route_health_wrong_method() {
        assert_eq!(Router::route("POST", "/health"), Route::MethodNotAllowed);
    }

    #[test]
    fn route_not_found() {
        assert_eq!(Router::route("GET", "/unknown"), Route::NotFound);
    }

    #[test]
    fn route_trailing_slash_normalised() {
        assert_eq!(
            Router::route("POST", "/push/Txn/"),
            Route::Push {
                event_name: "Txn".to_owned()
            }
        );
    }
}
