use crate::cluster::admin;
use crate::error::ApiError;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::Json;
use serde_json::json;

pub async fn list(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
) -> Result<Json<admin::TopicList>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    Ok(Json(admin::list_topics(handle).await?))
}

pub async fn detail(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
) -> Result<Json<admin::TopicDetail>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    Ok(Json(admin::topic_detail(handle, topic).await?))
}

pub async fn throughput(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    let samples = handle.sampler.rate_points(&topic);
    let as_of = handle.sampler.as_of(&topic);
    Ok(Json(json!({ "topic": topic, "samples": samples, "as_of": as_of })))
}

pub async fn consumers(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
) -> Result<Json<admin::TopicConsumers>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    Ok(Json(admin::topic_consumers(handle, topic).await?))
}
