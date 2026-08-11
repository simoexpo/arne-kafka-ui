use crate::state::AppState;
use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

pub async fn list(State(state): State<AppState>) -> Json<Value> {
    let handles = state.registry.all();
    let mut clusters = Vec::with_capacity(handles.len());
    for h in &handles {
        let health = h.health().await;
        clusters.push(json!({
            "name": h.name,
            "status": health.status,
            "broker_count": health.broker_count,
            "error": health.error,
        }));
    }
    Json(json!({ "clusters": clusters }))
}
