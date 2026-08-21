pub mod clusters;
pub mod groups;
pub mod messages;
pub mod static_files;
pub mod subjects;
pub mod topics;

use crate::state::AppState;
use axum::{Router, routing::get};
use tower_http::compression::CompressionLayer;

pub fn app(state: AppState) -> Router {
    // The topic list and the overview are the only responses whose SIZE grows
    // with the cluster — repetitive JSON, mostly field names, around a
    // megabyte at ten thousand topics. gzip takes the bulk of that off the
    // wire for one line. The default predicate leaves event streams alone: an
    // encoder buffers, and a buffered tail is not a live tail.
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/api/clusters", get(clusters::list))
        .route("/api/clusters/{cluster}/overview", get(clusters::overview))
        .route("/api/clusters/{cluster}/topics", get(topics::list))
        .route(
            "/api/clusters/{cluster}/topics/{topic}",
            get(topics::detail),
        )
        .route(
            "/api/clusters/{cluster}/topics/{topic}/consumers",
            get(topics::consumers),
        )
        .route(
            "/api/clusters/{cluster}/topics/{topic}/throughput",
            get(topics::throughput),
        )
        .route(
            "/api/clusters/{cluster}/topics/{topic}/tail",
            get(messages::tail_sse),
        )
        .route(
            "/api/clusters/{cluster}/topics/{topic}/timeline",
            get(messages::timeline_sse),
        )
        .route("/api/clusters/{cluster}/groups", get(groups::list))
        // distinct path, not /groups/lag: a group could legitimately be named "lag"
        .route("/api/clusters/{cluster}/group-lag", get(groups::lag))
        .route(
            "/api/clusters/{cluster}/broker-calls",
            get(clusters::broker_calls),
        )
        .route(
            "/api/clusters/{cluster}/groups/{group}",
            get(groups::detail),
        )
        .route(
            "/api/clusters/{cluster}/schema-registry",
            get(subjects::registry_settings),
        )
        .route(
            "/api/clusters/{cluster}/schema-ids/{id}",
            get(subjects::subject_of_id),
        )
        .route("/api/clusters/{cluster}/subjects", get(subjects::list))
        .route(
            "/api/clusters/{cluster}/subjects/{subject}",
            get(subjects::detail),
        )
        .route(
            "/api/clusters/{cluster}/subjects/{subject}/strategy",
            get(subjects::strategy),
        )
        .route(
            "/api/clusters/{cluster}/subjects/{subject}/compatibility",
            get(subjects::compatibility_level).post(subjects::check_compatibility),
        )
        // A bare handler (not wrapped in `get(...)`), so axum registers it as
        // an ANY-method fallback: any request to an UNMATCHED path — every
        // method, not just GET — reaches `spa_fallback` and gets the
        // structured envelope. A wrong-method request to a MATCHED route
        // (e.g. POST /api/clusters) never gets here: each `MethodRouter`
        // answers it itself with axum's bare, bodyless 405
        // (`method_not_allowed_fallback` exists to change that; v1's
        // frontend only ever sends the methods it registers, so it doesn't).
        .fallback(static_files::spa_fallback)
        .layer(CompressionLayer::new())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn test_state() -> AppState {
        use crate::cluster::registry::ClusterRegistry;
        use crate::config::{ClusterConfig, Limits};
        use std::sync::Arc;
        let cfg = ClusterConfig {
            name: "t".into(),
            bootstrap: "localhost:1".into(),
            sasl: None,
            schema_registry: None,
            broker_call_stats_ms: 0,
        };
        AppState {
            registry: Arc::new(ClusterRegistry::from_config(vec![cfg]).unwrap()),
            limits: Arc::new(Limits::default()),
        }
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let res = app(test_state())
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    /// The 404 envelope's own contract ("unknown `/api/*` paths return a 404
    /// envelope") isn't GET-only: a wrong-method request under `/api/*` must
    /// get the SAME structured `ApiError` body, not a bare, bodyless 405 —
    /// `MethodRouter`'s own default fallback for an unhandled method.
    #[tokio::test]
    async fn a_wrong_method_under_api_still_gets_the_404_envelope_not_a_bare_405() {
        let res = app(test_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["code"], "not_found");
    }
}
