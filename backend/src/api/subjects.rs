use crate::error::ApiError;
use crate::message::schema_registry::{SchemaRegistry, SubjectError};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

fn registry_for(state: &AppState, cluster: &str) -> Result<Arc<SchemaRegistry>, ApiError> {
    let handle = state.registry.get(cluster)?;
    handle.schema_registry.clone().ok_or_else(|| ApiError::no_schema_registry(cluster))
}

fn subject_error(cluster: &str, subject: &str, e: SubjectError) -> ApiError {
    match e {
        SubjectError::NotFound => ApiError::subject_not_found(cluster, subject),
        SubjectError::Registry(message) => ApiError::schema_registry(cluster, message),
    }
}

pub async fn list(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let sr = registry_for(&state, &cluster)?;
    let subjects = sr
        .subjects()
        .await
        .map_err(|e| subject_error(&cluster, "", e))?;
    Ok(Json(json!({ "subjects": subjects, "as_of": crate::util::now_ms() })))
}

pub async fn registry_settings(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let sr = registry_for(&state, &cluster)?;
    let settings = sr
        .registry_settings()
        .await
        .map_err(|e| subject_error(&cluster, "", e))?;
    Ok(Json(json!({
        "compatibility_level": settings.compatibility_level,
        "mode": settings.mode,
        "as_of": crate::util::now_ms(),
    })))
}

#[derive(Deserialize)]
pub struct VersionParam {
    pub version: Option<i32>,
}

pub async fn detail(
    State(state): State<AppState>,
    Path((cluster, subject)): Path<(String, String)>,
    Query(params): Query<VersionParam>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let sr = registry_for(&state, &cluster)?;
    let detail = sr
        .subject_detail(&subject, params.version)
        .await
        .map_err(|e| subject_error(&cluster, &subject, e))?;
    Ok(Json(json!({
        "subject": detail.subject,
        "versions": detail.versions,
        "version": detail.version,
        "id": detail.id,
        "schema_type": detail.schema_type,
        "schema": detail.schema,
        "as_of": crate::util::now_ms(),
    })))
}
