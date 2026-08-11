pub mod clusters;

use crate::state::AppState;
use axum::{routing::get, Router};

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/api/clusters", get(clusters::list))
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
        let cfg = ClusterConfig { name: "t".into(), bootstrap: "localhost:1".into(), sasl: None, schema_registry: None };
        AppState { registry: Arc::new(ClusterRegistry::from_config(vec![cfg]).unwrap()), limits: Arc::new(Limits::default()) }
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let res = app(test_state())
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}
