use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SchemaType { Avro, Protobuf, Json }

#[derive(Debug)]
pub struct RegisteredSchema {
    pub schema_type: SchemaType,
    pub schema: String,
}

#[derive(Deserialize)]
struct SrResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
}

pub struct SchemaRegistry {
    base: String,
    http: reqwest::Client,
    cache: RwLock<HashMap<i32, Arc<RegisteredSchema>>>,
}

impl SchemaRegistry {
    pub fn new(url: &str) -> Self {
        Self {
            base: url.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub async fn schema(&self, id: i32) -> Result<Arc<RegisteredSchema>, String> {
        if let Some(hit) = self.cache.read().await.get(&id) {
            return Ok(hit.clone());
        }
        let url = format!("{}/schemas/ids/{id}", self.base);
        let res = self.http.get(&url).send().await
            .map_err(|e| format!("schema registry unreachable fetching id {id}: {e}"))?;
        if !res.status().is_success() {
            return Err(format!("schema registry returned {} for schema id {id}", res.status()));
        }
        let body: SrResponse = res.json().await
            .map_err(|e| format!("schema registry bad body for id {id}: {e}"))?;
        let schema_type = match body.schema_type.as_deref() {
            None | Some("AVRO") => SchemaType::Avro,
            Some("PROTOBUF") => SchemaType::Protobuf,
            Some("JSON") => SchemaType::Json,
            Some(other) => return Err(format!("unsupported schema type '{other}' for id {id}")),
        };
        let entry = Arc::new(RegisteredSchema { schema_type, schema: body.schema });
        self.cache.write().await.insert(id, entry.clone());
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::Path, routing::get, Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    async fn mock_sr(hits: Arc<AtomicUsize>) -> String {
        let app = Router::new().route(
            "/schemas/ids/{id}",
            get(move |Path(id): Path<i32>| {
                let hits = hits.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    match id {
                        7 => Json(serde_json::json!({"schema": "\"string\""})).into_response(),
                        8 => Json(serde_json::json!({"schema": "syntax = \"proto3\";", "schemaType": "PROTOBUF"})).into_response(),
                        _ => axum::http::StatusCode::NOT_FOUND.into_response(),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
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
        assert_eq!(hits.load(Ordering::SeqCst), 1, "second lookup must hit the cache");
    }

    #[tokio::test]
    async fn fetches_protobuf_type() {
        let hits = Arc::new(AtomicUsize::new(0));
        let sr = SchemaRegistry::new(&mock_sr(hits).await);
        assert_eq!(sr.schema(8).await.unwrap().schema_type, SchemaType::Protobuf);
    }

    #[tokio::test]
    async fn missing_schema_is_a_string_error() {
        let hits = Arc::new(AtomicUsize::new(0));
        let sr = SchemaRegistry::new(&mock_sr(hits).await);
        let err = sr.schema(999).await.unwrap_err();
        assert!(err.contains("999"), "got: {err}");
    }
}
