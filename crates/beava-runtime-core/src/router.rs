//! Path dispatch for HTTP requests in the hand-rolled event loop.
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
    /// POST /batch_get — heterogeneous batched feature read.
    /// Body shape `{"requests":[{"table","entity_id"}, ...]}`.
    BatchGet,
    /// POST /register — pipeline registration.
    Register,
    /// GET /health — liveness probe. Always 200 once listener is up; no
    /// apply-thread dependency.
    Health,
    /// GET /ready — readiness probe on the data-plane port. Mirrors the
    /// admin sidecar's `/ready` for back-compat with foundation tests that
    /// poll `ts.base_url()` for readiness.
    Ready,
    /// GET /registry — registry snapshot dump on the data-plane port.
    /// Mirrors the admin sidecar's `/registry` for back-compat with tests
    /// that GET `/registry` to assert schema propagation.
    Registry,
    /// POST /ping — verb-style liveness probe; HTTP mirror of the TCP
    /// `OP_PING (0x0000)` semantics. Distinct from `/health` (the existing
    /// readiness/liveness probe consumed by `read_bench.py`). Always
    /// returns 200 `{"status":"ok"}` once the listener is up.
    Ping,
    /// POST /push — verb-style fire-and-forget push. Event name lives in
    /// the JSON body (`{"event":"Tx","data":{...}}`) instead of the URL
    /// path. Legacy `POST /push/:event_name` (`Route::Push`) stays alive
    /// for in-tree tests that hit the path-segment URL.
    PushVerb,
    /// POST /push-sync — verb-style synchronous push. Event name in body;
    /// awaits fsync. Mirrors `Route::PushVerb` on the sync path. Legacy
    /// `POST /push-sync/:event_name` (`Route::PushSync`) stays alive for
    /// back-compat.
    PushSyncVerb,
    /// POST /reset — full state + registry clear. Gated on server
    /// `test_mode`; default boot returns 403 + `reset_disabled_in_production`.
    /// Body is empty `{}`.
    Reset,
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
        // Strip query string for route matching; the dispatcher parses it
        // separately when needed.
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
        // Verb-style exact-match routes MUST come AFTER the `/push/`,
        // `/push-sync/`, and `/push-batch/` strip_prefix arms above so that
        // `/push/Tx` keeps resolving to `Route::Push { event_name }`
        // (legacy backward-compat). The strip_prefix branches require a
        // trailing `/` followed by a non-empty segment; bare `/push` falls
        // through here and matches `path == "/push"`.
        // /push (verb-style — event name in JSON body)
        if path == "/push" {
            return if method == "POST" {
                Route::PushVerb
            } else {
                Route::MethodNotAllowed
            };
        }
        // /push-sync (verb-style)
        if path == "/push-sync" {
            return if method == "POST" {
                Route::PushSyncVerb
            } else {
                Route::MethodNotAllowed
            };
        }
        // /ping (verb-style — HTTP mirror of TCP OP_PING)
        if path == "/ping" {
            return if method == "POST" {
                Route::Ping
            } else {
                Route::MethodNotAllowed
            };
        }
        // /reset is verb-style HTTP mirror of TCP OP_RESET. POST-only —
        // GET / PUT / DELETE return 405 (router-level method check). The
        // dispatch arm enforces the test_mode gate; routing here is
        // unconditional so non-test_mode servers still emit a structured
        // 403 (rather than a 404 that hides the surface).
        if path == "/reset" {
            return if method == "POST" {
                Route::Reset
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
        // /batch_get (exact) — heterogeneous batched read.
        if path == "/batch_get" {
            return if method == "POST" {
                Route::BatchGet
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
        // /health — liveness probe; read_bench.py startup contract.
        if path == "/health" {
            return if method == "GET" {
                Route::Health
            } else {
                Route::MethodNotAllowed
            };
        }
        // /ready — readiness probe on the data-plane port. Mirrors the
        // admin sidecar's /ready for back-compat with foundation tests
        // that check readiness on `ts.base_url()`.
        if path == "/ready" {
            return if method == "GET" {
                Route::Ready
            } else {
                Route::MethodNotAllowed
            };
        }
        // /registry — registry snapshot dump on the data-plane port.
        // Mirrors the admin sidecar's /registry for back-compat with tests
        // that GET /registry to assert schema propagation.
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

    #[test]
    fn route_batch_get_post() {
        assert_eq!(Router::route("POST", "/batch_get"), Route::BatchGet);
    }

    #[test]
    fn route_batch_get_wrong_method() {
        assert_eq!(Router::route("GET", "/batch_get"), Route::MethodNotAllowed);
    }

    #[test]
    fn route_reset_post() {
        assert_eq!(Router::route("POST", "/reset"), Route::Reset);
    }

    #[test]
    fn route_reset_wrong_method() {
        assert_eq!(Router::route("GET", "/reset"), Route::MethodNotAllowed);
    }

    #[test]
    fn route_health_get() {
        assert_eq!(Router::route("GET", "/health"), Route::Health);
    }

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
