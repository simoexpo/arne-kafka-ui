use crate::cluster::{admin, sampler, single_flight};
use crate::error::ApiError;
use crate::util::now_ms;
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

/// This request IS the sampling trigger (owner design 2026-08-19): nothing
/// samples in the background, so an unwatched cluster costs nothing. At most
/// one sample per interval, however many tabs poll.
pub async fn throughput(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    let interval_ms = (state.limits.sampler_interval_secs as i64) * 1000;
    let due = handle.sampler.age(&topic, now_ms()).is_none_or(|age| age >= interval_ms);
    if due {
        let sample_handle = handle.clone();
        let sample_topic = topic.clone();
        // Concurrent pollers of the same topic wait for the sample already in
        // flight rather than taking one of their own.
        let sampled = tokio::task::spawn_blocking(move || {
            let _flight = sample_handle
                .sampler_flight
                .begin_or_wait(sample_topic.clone(), single_flight::MAX_WAIT)?;
            Some(sampler::sample_topic_blocking(&sample_handle, &sample_topic))
        })
        .await
        .map_err(ApiError::task_join)?;
        match sampled {
            Some(Ok(total)) => handle.sampler.record(&topic, now_ms(), total),
            // Stale-but-visible: with history to show, a failed refresh must
            // not blank the panel. With nothing to show, say why.
            Some(Err(e)) if handle.sampler.as_of(&topic).is_none() => return Err(e),
            Some(Err(_)) => {}
            // Someone else's sample was in flight; serve what it left us.
            None => {}
        }
    }
    let samples = handle.sampler.rate_points(&topic, interval_ms);
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
