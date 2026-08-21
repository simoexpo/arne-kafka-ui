use crate::error::ApiError;
use crate::message::schema_registry::{SchemaRegistry, SubjectError};
use crate::state::AppState;
use axum::Json;
use axum::extract::{Path, Query, State};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

#[derive(Debug, PartialEq, serde::Serialize)]
pub struct SubjectStrategy {
    /// "topic_name", "record_name", "topic_record_name", or `None` when
    /// nothing can be honestly claimed.
    pub strategy: Option<&'static str>,
    pub topic: Option<String>,
    /// key/value for the topic-name strategy; not derivable otherwise.
    pub role: Option<&'static str>,
}

/// The registry does not record which topics use a schema. The strategy is
/// resolved from evidence only: topic-name needs the topic to exist;
/// record-name and topic-record-name need the subject to carry the
/// record's fully qualified name AS DECLARED BY THE SCHEMA ITSELF (`fqn`),
/// never guessed from the name's shape. Anything unprovable stays `None`.
fn resolve_strategy(subject: &str, topics: &[String], fqn: Option<&str>) -> SubjectStrategy {
    for (suffix, role) in [("-value", "value"), ("-key", "key")] {
        if let Some(t) = subject.strip_suffix(suffix)
            && topics.iter().any(|name| name == t)
        {
            return SubjectStrategy {
                strategy: Some("topic_name"),
                topic: Some(t.to_string()),
                role: Some(role),
            };
        }
    }
    if let Some(fqn) = fqn {
        if subject == fqn {
            return SubjectStrategy {
                strategy: Some("record_name"),
                topic: None,
                role: None,
            };
        }
        if let Some(t) = subject.strip_suffix(&format!("-{fqn}"))
            && topics.iter().any(|name| name == t)
        {
            return SubjectStrategy {
                strategy: Some("topic_record_name"),
                topic: Some(t.to_string()),
                role: None,
            };
        }
    }
    SubjectStrategy {
        strategy: None,
        topic: None,
        role: None,
    }
}

fn registry_for(state: &AppState, cluster: &str) -> Result<Arc<SchemaRegistry>, ApiError> {
    let handle = state.registry.get(cluster)?;
    handle
        .schema_registry
        .clone()
        .ok_or_else(|| ApiError::no_schema_registry(cluster))
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
    Ok(Json(
        json!({ "subjects": subjects, "as_of": crate::util::now_ms() }),
    ))
}

/// The registry URL comes from the user's own config, but embedded
/// credentials (`scheme://user:pass@host`) must never echo back through
/// the browser — the userinfo is stripped, everything else verbatim.
fn strip_userinfo(url: &str) -> String {
    if let Some((scheme, rest)) = url.split_once("://") {
        let authority_end = rest.find('/').unwrap_or(rest.len());
        if let Some(at) = rest[..authority_end].rfind('@') {
            return format!("{scheme}://{}", &rest[at + 1..]);
        }
    }
    url.to_string()
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
        "url": strip_userinfo(sr.url()),
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

/// Resolves a schema id (as shown on a message) to the subject that
/// registered it — the message tab's link into the schemas section.
pub async fn subject_of_id(
    State(state): State<AppState>,
    Path((cluster, id)): Path<(String, i32)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let sr = registry_for(&state, &cluster)?;
    let hit = sr.subject_of_id(id).await.map_err(|e| match e {
        SubjectError::NotFound => ApiError::schema_id_not_found(&cluster, id),
        other => subject_error(&cluster, "", other),
    })?;
    Ok(Json(
        json!({ "subject": hit.subject, "version": hit.version, "as_of": crate::util::now_ms() }),
    ))
}

pub async fn strategy(
    State(state): State<AppState>,
    Path((cluster, subject)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    let sr = handle
        .schema_registry
        .clone()
        .ok_or_else(|| ApiError::no_schema_registry(&cluster))?;
    let detail = sr
        .subject_detail(&subject, None)
        .await
        .map_err(|e| subject_error(&cluster, &subject, e))?;
    let fqn = match detail.schema_type.as_str() {
        "AVRO" => crate::message::avro::record_fqn(&detail.schema),
        "PROTOBUF" => crate::message::proto::message_fqn(&detail.schema),
        _ => None,
    };
    let topics = crate::cluster::admin::topic_names(handle).await?;
    let resolved = resolve_strategy(&subject, &topics, fqn.as_deref());
    Ok(Json(json!({
        "strategy": resolved.strategy,
        "topic": resolved.topic,
        "role": resolved.role,
        "as_of": crate::util::now_ms(),
    })))
}

pub async fn compatibility_level(
    State(state): State<AppState>,
    Path((cluster, subject)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let sr = registry_for(&state, &cluster)?;
    let level = sr
        .subject_compatibility_level(&subject)
        .await
        .map_err(|e| subject_error(&cluster, &subject, e))?;
    Ok(Json(
        json!({ "level": level, "as_of": crate::util::now_ms() }),
    ))
}

#[derive(Deserialize)]
pub struct CompatibilityCheckBody {
    pub schema: String,
    pub schema_type: String,
}

pub async fn check_compatibility(
    State(state): State<AppState>,
    Path((cluster, subject)): Path<(String, String)>,
    Json(body): Json<CompatibilityCheckBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let sr = registry_for(&state, &cluster)?;
    let check = sr
        .check_compatibility(&subject, &body.schema, &body.schema_type)
        .await
        .map_err(|e| subject_error(&cluster, &subject, e))?;
    Ok(Json(json!({
        "is_compatible": check.is_compatible,
        "messages": check.messages,
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
        let s = resolve_strategy("orders-value", &topics(&["orders", "payments"]), None);
        assert_eq!(
            s,
            SubjectStrategy {
                strategy: Some("topic_name"),
                topic: Some("orders".into()),
                role: Some("value")
            }
        );
        assert_eq!(
            resolve_strategy("ghost-value", &topics(&["orders"]), None).strategy,
            None
        );
        assert_eq!(
            resolve_strategy("orders-key", &topics(&["orders"]), None).role,
            Some("key")
        );
    }

    /// The record's fully qualified name comes from the SCHEMA (not the
    /// name's shape), so record-name and topic-record-name are verified,
    /// never guessed.
    #[test]
    fn record_name_strategy_is_verified_against_the_schema_fqn() {
        let s = resolve_strategy(
            "com.acme.Order",
            &topics(&["orders"]),
            Some("com.acme.Order"),
        );
        assert_eq!(
            s,
            SubjectStrategy {
                strategy: Some("record_name"),
                topic: None,
                role: None
            }
        );
        // Same name WITHOUT a matching schema FQN stays honestly unknown.
        assert_eq!(
            resolve_strategy("com.acme.Order", &topics(&["orders"]), None).strategy,
            None
        );
        assert_eq!(
            resolve_strategy(
                "com.acme.Order",
                &topics(&["orders"]),
                Some("com.acme.Other")
            )
            .strategy,
            None
        );
    }

    #[test]
    fn topic_record_name_requires_existing_topic_and_exact_fqn_remainder() {
        let s = resolve_strategy(
            "orders-com.acme.Order",
            &topics(&["orders", "orders-eu"]),
            Some("com.acme.Order"),
        );
        assert_eq!(
            s,
            SubjectStrategy {
                strategy: Some("topic_record_name"),
                topic: Some("orders".into()),
                role: None
            }
        );
        assert_eq!(
            resolve_strategy(
                "ghost-com.acme.Order",
                &topics(&["orders"]),
                Some("com.acme.Order")
            )
            .strategy,
            None
        );
    }

    /// `orders-eu-value` is the topic-name strategy for topic `orders-eu`,
    /// even when a shorter topic (`orders`) is also a prefix.
    #[test]
    fn topic_name_wins_over_prefix_coincidences() {
        let s = resolve_strategy("orders-eu-value", &topics(&["orders", "orders-eu"]), None);
        assert_eq!(s.strategy, Some("topic_name"));
        assert_eq!(s.topic.as_deref(), Some("orders-eu"));
    }
}

#[cfg(test)]
mod url_tests {
    use super::strip_userinfo;

    /// The registry URL comes from the user's own config, but embedded
    /// credentials (`scheme://user:pass@host`) must never echo back
    /// through the browser.
    #[test]
    fn strips_userinfo_but_nothing_else() {
        assert_eq!(strip_userinfo("http://sr:8081"), "http://sr:8081");
        assert_eq!(
            strip_userinfo("https://user:pass@sr.example.com:8081"),
            "https://sr.example.com:8081"
        );
        // An @ past the authority (path/query) is not userinfo.
        assert_eq!(
            strip_userinfo("http://sr:8081/path@x"),
            "http://sr:8081/path@x"
        );
    }
}
