use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// How long a failed schema lookup is remembered before being retried.
/// Without this, every message carrying an id the registry can't (or
/// currently doesn't) serve pays a full HTTP round trip — under an
/// unreachable registry that's the request timeout, per message, and a
/// recovering registry gets stampeded by every worker retrying at once.
const NEGATIVE_TTL: Duration = Duration::from_secs(30);

struct NegativeEntry {
    until: Instant,
    message: String,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SchemaType {
    Avro,
    Protobuf,
    Json,
}

#[derive(Debug)]
pub struct RegisteredSchema {
    pub schema_type: SchemaType,
    pub schema: String,
}

/// The parsed/structured form of a registered schema — an
/// `apache_avro::Schema` or a protobuf `FileDescriptor`, rather than the raw
/// schema string. Parsing is the expensive part of decoding (temp-file +
/// `protobuf_parse` for protobuf, a full grammar parse for Avro), and
/// schemas are immutable per id, so `SchemaRegistry` caches this alongside
/// the raw string cache instead of re-parsing on every message.
pub enum ParsedSchema {
    Avro(apache_avro::Schema),
    Protobuf(protobuf::reflect::FileDescriptor),
}

#[derive(Deserialize)]
struct SrResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

#[derive(Debug)]
pub enum SubjectError {
    /// The registry answered 404: the subject (or version) doesn't exist.
    NotFound,
    /// Anything else — unreachable, non-2xx, bad body — with a
    /// product-voice message.
    Registry(String),
}

#[derive(Deserialize)]
struct SubjectVersionResponse {
    id: i32,
    version: i32,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    schema: String,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct SubjectOfId {
    pub subject: String,
    pub version: i32,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct CompatibilityCheck {
    pub is_compatible: bool,
    #[serde(default)]
    pub messages: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct RegistrySettings {
    pub compatibility_level: String,
    pub mode: String,
}

#[derive(Deserialize)]
struct SrConfigResponse {
    #[serde(rename = "compatibilityLevel")]
    compatibility_level: String,
}

#[derive(Deserialize)]
struct SrModeResponse {
    mode: String,
}

#[derive(Debug, serde::Serialize)]
pub struct SubjectDetail {
    pub subject: String,
    pub versions: Vec<i32>,
    pub version: i32,
    pub id: i32,
    pub schema_type: String,
    pub schema: String,
}

pub struct SchemaRegistry {
    base: String,
    http: reqwest::Client,
    cache: RwLock<HashMap<i32, Arc<RegisteredSchema>>>,
    parsed_cache: RwLock<HashMap<i32, Arc<ParsedSchema>>>,
    negative_cache: RwLock<HashMap<i32, NegativeEntry>>,
}

impl SchemaRegistry {
    pub fn url(&self) -> &str {
        &self.base
    }

    pub fn new(url: &str) -> Self {
        Self {
            base: url.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
            cache: RwLock::new(HashMap::new()),
            parsed_cache: RwLock::new(HashMap::new()),
            negative_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Records `message` as id's failure for `NEGATIVE_TTL` and returns it
    /// as the error, so every fallible exit from `schema` goes through the
    /// same negative-caching path.
    async fn fail(&self, id: i32, message: String) -> Result<Arc<RegisteredSchema>, String> {
        self.negative_cache.write().await.insert(
            id,
            NegativeEntry {
                until: Instant::now() + NEGATIVE_TTL,
                message: message.clone(),
            },
        );
        Err(message)
    }

    pub async fn schema(&self, id: i32) -> Result<Arc<RegisteredSchema>, String> {
        if let Some(hit) = self.cache.read().await.get(&id) {
            return Ok(hit.clone());
        }
        if let Some(neg) = self.negative_cache.read().await.get(&id)
            && neg.until > Instant::now()
        {
            return Err(neg.message.clone());
        }
        let url = format!("{}/schemas/ids/{id}", self.base);
        let res = match self.http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                return self
                    .fail(
                        id,
                        format!("schema registry unreachable fetching id {id}: {e}"),
                    )
                    .await;
            }
        };
        if !res.status().is_success() {
            return self
                .fail(
                    id,
                    format!(
                        "schema registry returned {} for schema id {id}",
                        res.status()
                    ),
                )
                .await;
        }
        let body: SrResponse = match res.json().await {
            Ok(b) => b,
            Err(e) => {
                return self
                    .fail(id, format!("schema registry bad body for id {id}: {e}"))
                    .await;
            }
        };
        let schema_type = match body.schema_type.as_deref() {
            None | Some("AVRO") => SchemaType::Avro,
            Some("PROTOBUF") => SchemaType::Protobuf,
            Some("JSON") => SchemaType::Json,
            Some(other) => {
                return self
                    .fail(id, format!("unsupported schema type '{other}' for id {id}"))
                    .await;
            }
        };
        let entry = Arc::new(RegisteredSchema {
            schema_type,
            schema: body.schema,
        });
        self.cache.write().await.insert(id, entry.clone());
        Ok(entry)
    }

    /// Every subject name in the registry, in the registry's own order.
    /// Uncached: the schema page is user-triggered and must show fresh
    /// registry state, unlike per-id decode lookups (ids are immutable).
    pub async fn subjects(&self) -> Result<Vec<String>, SubjectError> {
        let res = self
            .http
            .get(format!("{}/subjects", self.base))
            .send()
            .await
            .map_err(|e| SubjectError::Registry(format!("schema registry unreachable: {e}")))?;
        if !res.status().is_success() {
            return Err(SubjectError::Registry(format!(
                "schema registry returned {}",
                res.status()
            )));
        }
        res.json()
            .await
            .map_err(|e| SubjectError::Registry(format!("schema registry bad body: {e}")))
    }

    /// The subject (and version) that registered schema `id` — exact under
    /// any naming strategy. An id registered under several subjects
    /// returns the registry's first. Uncached, same reasoning as
    /// `subjects`.
    pub async fn subject_of_id(&self, id: i32) -> Result<SubjectOfId, SubjectError> {
        let versions: Vec<SubjectOfId> = self
            .get_subject_json(&format!("schemas/ids/{id}/versions"))
            .await?;
        versions.into_iter().next().ok_or(SubjectError::NotFound)
    }

    /// The registry's own verdict on whether `schema` is compatible with
    /// the subject's latest version, verbose (per-rule messages included).
    pub async fn check_compatibility(
        &self,
        subject: &str,
        schema: &str,
        schema_type: &str,
    ) -> Result<CompatibilityCheck, SubjectError> {
        let url = format!(
            "{}/compatibility/subjects/{subject}/versions/latest?verbose=true",
            self.base
        );
        let res = self
            .http
            .post(&url)
            .json(&serde_json::json!({ "schema": schema, "schemaType": schema_type }))
            .send()
            .await
            .map_err(|e| SubjectError::Registry(format!("schema registry unreachable: {e}")))?;
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(SubjectError::NotFound);
        }
        if !res.status().is_success() {
            return Err(SubjectError::Registry(format!(
                "schema registry returned {}",
                res.status()
            )));
        }
        res.json()
            .await
            .map_err(|e| SubjectError::Registry(format!("schema registry bad body: {e}")))
    }

    /// The subject's EFFECTIVE compatibility level: its own override when
    /// one exists, else the registry's global level (a 404 on
    /// `/config/{subject}` is the registry's way of saying "no override").
    pub async fn subject_compatibility_level(&self, subject: &str) -> Result<String, SubjectError> {
        match self
            .get_subject_json::<SrConfigResponse>(&format!("config/{subject}"))
            .await
        {
            Ok(config) => Ok(config.compatibility_level),
            Err(SubjectError::NotFound) => Ok(self.registry_settings().await?.compatibility_level),
            Err(other) => Err(other),
        }
    }

    /// Registry-wide settings for the Schemas page header: the global
    /// compatibility level and the registry mode. Uncached, same reasoning
    /// as `subjects`.
    pub async fn registry_settings(&self) -> Result<RegistrySettings, SubjectError> {
        let config: SrConfigResponse = self.get_subject_json("config").await?;
        let mode: SrModeResponse = self.get_subject_json("mode").await?;
        Ok(RegistrySettings {
            compatibility_level: config.compatibility_level,
            mode: mode.mode,
        })
    }

    /// The subject's version list plus ONE version's schema — the requested
    /// one, or the registry's latest when `None` — so the detail page is a
    /// single query. Uncached, same reasoning as `subjects`.
    pub async fn subject_detail(
        &self,
        subject: &str,
        version: Option<i32>,
    ) -> Result<SubjectDetail, SubjectError> {
        let versions: Vec<i32> = self
            .get_subject_json(&format!("subjects/{subject}/versions"))
            .await?;
        let selector = version.map_or_else(|| "latest".to_string(), |v| v.to_string());
        let body: SubjectVersionResponse = self
            .get_subject_json(&format!("subjects/{subject}/versions/{selector}"))
            .await?;
        Ok(SubjectDetail {
            subject: subject.to_string(),
            versions,
            version: body.version,
            id: body.id,
            // The registry omits the field for its default type.
            schema_type: body.schema_type.unwrap_or_else(|| "AVRO".to_string()),
            schema: body.schema,
        })
    }

    async fn get_subject_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<T, SubjectError> {
        let res = self
            .http
            .get(format!("{}/{path}", self.base))
            .send()
            .await
            .map_err(|e| SubjectError::Registry(format!("schema registry unreachable: {e}")))?;
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(SubjectError::NotFound);
        }
        if !res.status().is_success() {
            return Err(SubjectError::Registry(format!(
                "schema registry returned {}",
                res.status()
            )));
        }
        res.json()
            .await
            .map_err(|e| SubjectError::Registry(format!("schema registry bad body: {e}")))
    }

    /// Returns the parsed/structured form of schema `id` (an
    /// `apache_avro::Schema` or protobuf `FileDescriptor`), parsing it only
    /// on first use and caching the result thereafter — schemas are
    /// immutable per id, so re-parsing on every decode call is pure waste.
    /// Not meaningful for `SchemaType::Json`, which has no parsed artifact;
    /// callers decode JSON-typed schemas via `decode::decode_plain`
    /// directly instead of calling this.
    pub async fn parsed(&self, id: i32) -> Result<Arc<ParsedSchema>, String> {
        if let Some(hit) = self.parsed_cache.read().await.get(&id) {
            return Ok(hit.clone());
        }
        let schema = self.schema(id).await?;
        let parsed = match schema.schema_type {
            SchemaType::Avro => {
                crate::message::avro::parse_schema(&schema.schema).map(ParsedSchema::Avro)?
            }
            SchemaType::Protobuf => crate::message::proto::build_descriptor(&schema.schema)
                .map(ParsedSchema::Protobuf)?,
            SchemaType::Json => {
                return Err(format!(
                    "schema id {id} is JSON-typed and has no parsed artifact"
                ));
            }
        };
        let entry = Arc::new(parsed);
        self.parsed_cache.write().await.insert(id, entry.clone());
        Ok(entry)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::{Json, Router, extract::Path, routing::get};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub(crate) async fn mock_sr_for_decode() -> String {
        let hits = Arc::new(AtomicUsize::new(0));
        mock_sr(hits).await
    }

    async fn mock_sr(hits: Arc<AtomicUsize>) -> String {
        let app = Router::new()
            .route(
                "/schemas/ids/{id}",
                get(move |Path(id): Path<i32>| {
                    let hits = hits.clone();
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        match id {
                            7 => Json(serde_json::json!({"schema": "\"string\""})).into_response(),
                            8 => Json(serde_json::json!({"schema": "syntax = \"proto3\";", "schemaType": "PROTOBUF"})).into_response(),
                            9 => Json(serde_json::json!({"schema": "{}", "schemaType": "JSON"})).into_response(),
                            _ => axum::http::StatusCode::NOT_FOUND.into_response(),
                        }
                    }
                }),
            )
            .route(
                "/subjects",
                get(|| async { Json(serde_json::json!(["sr-avro-value", "sr-json-value"])) }),
            )
            .route(
                "/schemas/ids/{id}/versions",
                get(|Path(id): Path<i32>| async move {
                    match id {
                        42 => Json(serde_json::json!([{"subject": "sr-avro-value", "version": 3}])).into_response(),
                        _ => axum::http::StatusCode::NOT_FOUND.into_response(),
                    }
                }),
            )
            .route("/config", get(|| async { Json(serde_json::json!({"compatibilityLevel": "BACKWARD"})) }))
            .route(
                "/config/{subject}",
                get(|Path(subject): Path<String>| async move {
                    match subject.as_str() {
                        "sr-avro-value" => Json(serde_json::json!({"compatibilityLevel": "FULL"})).into_response(),
                        _ => axum::http::StatusCode::NOT_FOUND.into_response(),
                    }
                }),
            )
            .route(
                "/compatibility/subjects/{subject}/versions/latest",
                axum::routing::post(|Path(subject): Path<String>, Json(body): Json<serde_json::Value>| async move {
                    if subject != "sr-avro-value" {
                        return axum::http::StatusCode::NOT_FOUND.into_response();
                    }
                    let compatible = body["schema"].as_str().unwrap_or("").contains("long");
                    Json(serde_json::json!({
                        "is_compatible": compatible,
                        "messages": if compatible { serde_json::json!([]) } else { serde_json::json!(["READER_FIELD_MISSING_DEFAULT_VALUE"]) },
                    }))
                    .into_response()
                }),
            )
            .route("/mode", get(|| async { Json(serde_json::json!({"mode": "READWRITE"})) }))
            .route(
                "/subjects/{subject}/versions",
                get(|Path(subject): Path<String>| async move {
                    match subject.as_str() {
                        "sr-avro-value" => Json(serde_json::json!([1, 2, 3])).into_response(),
                        _ => axum::http::StatusCode::NOT_FOUND.into_response(),
                    }
                }),
            )
            .route(
                "/subjects/{subject}/versions/{version}",
                get(|Path((subject, version)): Path<(String, String)>| async move {
                    match (subject.as_str(), version.as_str()) {
                        ("sr-avro-value", "latest") | ("sr-avro-value", "3") => Json(serde_json::json!({
                            "subject": "sr-avro-value", "id": 42, "version": 3, "schema": "\"string\""
                        }))
                        .into_response(),
                        ("sr-avro-value", "1") => Json(serde_json::json!({
                            "subject": "sr-avro-value", "id": 40, "version": 1,
                            "schemaType": "JSON", "schema": "{}"
                        }))
                        .into_response(),
                        _ => axum::http::StatusCode::NOT_FOUND.into_response(),
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn lists_subjects() {
        let sr = SchemaRegistry::new(&mock_sr(Arc::new(AtomicUsize::new(0))).await);
        assert_eq!(
            sr.subjects().await.unwrap(),
            vec!["sr-avro-value", "sr-json-value"]
        );
    }

    /// One call returns the version list AND the requested version's schema
    /// (latest when unspecified) — the detail page is a single query.
    #[tokio::test]
    async fn subject_detail_returns_versions_and_the_requested_schema() {
        let sr = SchemaRegistry::new(&mock_sr(Arc::new(AtomicUsize::new(0))).await);
        let latest = sr.subject_detail("sr-avro-value", None).await.unwrap();
        assert_eq!(latest.versions, vec![1, 2, 3]);
        assert_eq!(latest.version, 3);
        assert_eq!(latest.id, 42);
        assert_eq!(latest.schema, "\"string\"");
        assert_eq!(latest.schema_type, "AVRO"); // registry omits the field for avro

        let v1 = sr.subject_detail("sr-avro-value", Some(1)).await.unwrap();
        assert_eq!(v1.version, 1);
        assert_eq!(v1.schema_type, "JSON");
    }

    /// Registry-wide settings for the Schemas page header: the global
    /// compatibility level (GET /config) and the registry mode (GET /mode).
    #[tokio::test]
    async fn registry_settings_report_compatibility_and_mode() {
        let sr = SchemaRegistry::new(&mock_sr(Arc::new(AtomicUsize::new(0))).await);
        let settings = sr.registry_settings().await.unwrap();
        assert_eq!(settings.compatibility_level, "BACKWARD");
        assert_eq!(settings.mode, "READWRITE");
    }

    /// A schema id links back to the subject that registered it — exact
    /// under ANY naming strategy, unlike name inference.
    #[tokio::test]
    async fn resolves_a_schema_id_to_its_subject() {
        let sr = SchemaRegistry::new(&mock_sr(Arc::new(AtomicUsize::new(0))).await);
        let hit = sr.subject_of_id(42).await.unwrap();
        assert_eq!(hit.subject, "sr-avro-value");
        assert_eq!(hit.version, 3);
        assert!(matches!(
            sr.subject_of_id(99).await.unwrap_err(),
            SubjectError::NotFound
        ));
    }

    /// Compatibility checks run against the registry itself (its verdict,
    /// not ours), with the subject's own effective level (its override or
    /// the global default) reported alongside.
    #[tokio::test]
    async fn checks_a_candidate_schema_against_the_latest_version() {
        let sr = SchemaRegistry::new(&mock_sr(Arc::new(AtomicUsize::new(0))).await);
        let ok = sr
            .check_compatibility("sr-avro-value", r#"{"type":"long"}"#, "AVRO")
            .await
            .unwrap();
        assert!(ok.is_compatible);
        assert!(ok.messages.is_empty());
        let bad = sr
            .check_compatibility("sr-avro-value", r#"{"type":"string"}"#, "AVRO")
            .await
            .unwrap();
        assert!(!bad.is_compatible);
        assert_eq!(bad.messages, vec!["READER_FIELD_MISSING_DEFAULT_VALUE"]);
    }

    #[tokio::test]
    async fn subject_level_falls_back_to_global_when_unset() {
        let sr = SchemaRegistry::new(&mock_sr(Arc::new(AtomicUsize::new(0))).await);
        assert_eq!(
            sr.subject_compatibility_level("sr-avro-value")
                .await
                .unwrap(),
            "FULL"
        );
        // The stub 404s unknown subjects' config — the registry's way of
        // saying "no override"; the global level answers instead.
        assert_eq!(
            sr.subject_compatibility_level("sr-json-value")
                .await
                .unwrap(),
            "BACKWARD"
        );
    }

    #[tokio::test]
    async fn unknown_subject_is_a_not_found_error() {
        let sr = SchemaRegistry::new(&mock_sr(Arc::new(AtomicUsize::new(0))).await);
        let err = sr.subject_detail("nope", None).await.unwrap_err();
        assert!(matches!(err, SubjectError::NotFound));
    }

    use axum::response::IntoResponse;

    #[tokio::test]
    async fn fetches_avro_default_and_caches() {
        let hits = Arc::new(AtomicUsize::new(0));
        let sr = SchemaRegistry::new(&mock_sr(hits.clone()).await);
        let s1 = sr.schema(7).await.unwrap();
        assert_eq!(s1.schema_type, SchemaType::Avro);
        assert_eq!(s1.schema, "\"string\"");
        let _s2 = sr.schema(7).await.unwrap();
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "second lookup must hit the cache"
        );
    }

    #[tokio::test]
    async fn fetches_protobuf_type() {
        let hits = Arc::new(AtomicUsize::new(0));
        let sr = SchemaRegistry::new(&mock_sr(hits).await);
        assert_eq!(
            sr.schema(8).await.unwrap().schema_type,
            SchemaType::Protobuf
        );
    }

    #[tokio::test]
    async fn missing_schema_is_a_string_error() {
        let hits = Arc::new(AtomicUsize::new(0));
        let sr = SchemaRegistry::new(&mock_sr(hits).await);
        let err = sr.schema(999).await.unwrap_err();
        assert!(err.contains("999"), "got: {err}");
    }

    /// Decoding must not re-parse the schema string (rebuilding the
    /// Avro `Schema` / protobuf `FileDescriptor` from scratch) on every
    /// single message, even though schemas are immutable per id and already
    /// have a string-level cache. `parsed()` must reuse the same parsed
    /// artifact across repeated calls for the same id — proven here by
    /// pointer identity: a fresh parse would allocate a new object, so two
    /// calls returning the *same* `Arc` allocation means the parse ran once.
    #[tokio::test]
    async fn parsed_avro_schema_is_cached_and_reused() {
        let sr = SchemaRegistry::new(&mock_sr_for_decode().await);
        let p1 = sr.parsed(7).await.unwrap();
        let p2 = sr.parsed(7).await.unwrap();
        assert!(
            Arc::ptr_eq(&p1, &p2),
            "parsed avro schema must be cached, not reparsed each call"
        );
        assert!(matches!(&*p1, ParsedSchema::Avro(_)));
    }

    #[tokio::test]
    async fn parsed_protobuf_schema_is_cached_and_reused() {
        let sr = SchemaRegistry::new(&mock_sr_for_decode().await);
        let p1 = sr.parsed(8).await.unwrap();
        let p2 = sr.parsed(8).await.unwrap();
        assert!(
            Arc::ptr_eq(&p1, &p2),
            "parsed protobuf descriptor must be cached, not reparsed each call"
        );
        assert!(matches!(&*p1, ParsedSchema::Protobuf(_)));
    }

    /// Without negative caching, every lookup of a
    /// permanently-failing schema id pays a full HTTP round trip — under an
    /// unreachable/misbehaving registry this stampedes it on every message.
    /// Two lookups of the same failing id within the negative-cache TTL must
    /// hit the registry once.
    #[tokio::test]
    async fn failing_lookup_is_negatively_cached_within_ttl() {
        let hits = Arc::new(AtomicUsize::new(0));
        let sr = SchemaRegistry::new(&mock_sr(hits.clone()).await);
        let err1 = sr.schema(999).await.unwrap_err();
        let err2 = sr.schema(999).await.unwrap_err();
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "second lookup within TTL must not hit the registry again"
        );
        assert_eq!(err1, err2);
    }
}
