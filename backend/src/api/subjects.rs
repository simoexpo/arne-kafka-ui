use crate::error::ApiError;
use crate::message::schema_registry::{SchemaRegistry, SubjectError};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

#[derive(Debug, PartialEq, serde::Serialize)]
pub struct SubjectUsage {
    pub topic: String,
    /// "topic_name" (subject is `<topic>-key`/`<topic>-value`) or
    /// "topic_record_name" (subject is `<topic>-<fully.qualified.Record>`).
    pub strategy: &'static str,
    /// key/value for the topic-name strategy; not derivable otherwise.
    pub role: Option<&'static str>,
}

/// The registry does not record which topics use a schema — usage is
/// inferred from the subject NAME per Confluent's naming strategies, and
/// only claimed for topics that actually exist. A subject following the
/// record-name strategy derives no topic at all (empty result — the UI
/// says so). The topic-record heuristic requires a dotted remainder (a
/// fully-qualified record name), so `orders-eu` the topic never
/// false-matches `orders-eu-value` the subject.
fn infer_usage(subject: &str, topics: &[String]) -> Vec<SubjectUsage> {
    let mut out = Vec::new();
    for (suffix, role) in [("-value", "value"), ("-key", "key")] {
        if let Some(t) = subject.strip_suffix(suffix)
            && topics.iter().any(|name| name == t)
        {
            out.push(SubjectUsage { topic: t.to_string(), strategy: "topic_name", role: Some(role) });
        }
    }
    for t in topics {
        if let Some(remainder) = subject.strip_prefix(&format!("{t}-"))
            && remainder.contains('.')
        {
            out.push(SubjectUsage { topic: t.clone(), strategy: "topic_record_name", role: None });
        }
    }
    out
}

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

pub async fn usage(
    State(state): State<AppState>,
    Path((cluster, subject)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    // Consistent with the rest of the schemas page: without a registry the
    // subject itself is a fiction here.
    if handle.schema_registry.is_none() {
        return Err(ApiError::no_schema_registry(&cluster));
    }
    let topics = crate::cluster::admin::topic_names(handle).await?;
    Ok(Json(json!({
        "usages": infer_usage(&subject, &topics),
        "as_of": crate::util::now_ms(),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn topic_name_strategy_matches_only_existing_topics() {
        let usage = infer_usage("orders-value", &topics(&["orders", "payments"]));
        assert_eq!(usage, vec![SubjectUsage { topic: "orders".into(), strategy: "topic_name", role: Some("value") }]);
        assert!(infer_usage("ghost-value", &topics(&["orders"])).is_empty());
        let key = infer_usage("orders-key", &topics(&["orders"]));
        assert_eq!(key[0].role, Some("key"));
    }

    #[test]
    fn topic_record_name_strategy_requires_a_dotted_record_remainder() {
        let usage = infer_usage("orders-com.acme.Order", &topics(&["orders", "orders-eu"]));
        assert_eq!(usage, vec![SubjectUsage { topic: "orders".into(), strategy: "topic_record_name", role: None }]);
        // `orders-eu-value` is the topic-name strategy for topic `orders-eu`,
        // never a topic-record match for `orders` (remainder has no dot).
        let tn = infer_usage("orders-eu-value", &topics(&["orders", "orders-eu"]));
        assert_eq!(tn.len(), 1);
        assert_eq!(tn[0].strategy, "topic_name");
        assert_eq!(tn[0].topic, "orders-eu");
    }

    #[test]
    fn record_name_strategy_derives_no_topic() {
        assert!(infer_usage("com.acme.Order", &topics(&["orders"])).is_empty());
    }
}
