use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use serde_json::json;

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub cluster: Option<String>,
    pub retriable: bool,
}

impl ApiError {
    pub fn cluster_not_found(name: &str) -> Self {
        Self { status: StatusCode::NOT_FOUND, code: "cluster_not_found",
               message: format!("no cluster named '{name}' is configured"),
               cluster: Some(name.into()), retriable: false }
    }
    pub fn topic_not_found(cluster: &str, topic: &str) -> Self {
        Self { status: StatusCode::NOT_FOUND, code: "topic_not_found",
               message: format!("topic '{topic}' does not exist"),
               cluster: Some(cluster.into()), retriable: false }
    }
    pub fn group_not_found(cluster: &str, group: &str) -> Self {
        Self { status: StatusCode::NOT_FOUND, code: "group_not_found",
               message: format!("consumer group '{group}' does not exist"),
               cluster: Some(cluster.into()), retriable: false }
    }
    /// A request under `/api/*` that matches no route — a backend routing
    /// bug or a frontend API typo, never a page to render. Owner ruling
    /// (review I3): this gets the same structured envelope as any other API
    /// error, not the SPA fallback, so it never reaches the frontend as an
    /// HTML body masquerading as a 200 JSON response.
    pub fn not_found_route(path: &str) -> Self {
        Self { status: StatusCode::NOT_FOUND, code: "not_found",
               message: format!("no such endpoint '{path}'"),
               cluster: None, retriable: false }
    }
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_REQUEST, code: "bad_request",
               message: message.into(), cluster: None, retriable: false }
    }
    pub fn kafka_timeout(cluster: &str, what: &str) -> Self {
        Self { status: StatusCode::GATEWAY_TIMEOUT, code: "kafka_timeout",
               message: format!("{what} timed out"),
               cluster: Some(cluster.into()), retriable: true }
    }
    pub fn kafka(cluster: &str, message: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_GATEWAY, code: "kafka_error",
               message: message.into(), cluster: Some(cluster.into()), retriable: true }
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self { status: StatusCode::INTERNAL_SERVER_ERROR, code: "internal",
               message: message.into(), cluster: None, retriable: false }
    }
    /// A `spawn_blocking`/`JoinHandle` failed to join (the blocking task
    /// panicked or was cancelled) — never a Kafka-side failure, so this is
    /// always `internal`, not `kafka`. Product voice: never say "task join"
    /// or `JoinError` on the wire — this is an internal fault, not anything
    /// the user's request did wrong.
    pub fn task_join(e: tokio::task::JoinError) -> Self {
        Self::internal(format!("something went wrong completing the request ({e})"))
    }
}

pub fn from_kafka(cluster: &str, what: &str, err: &KafkaError) -> ApiError {
    match err.rdkafka_error_code() {
        Some(RDKafkaErrorCode::OperationTimedOut) | Some(RDKafkaErrorCode::RequestTimedOut) => {
            ApiError::kafka_timeout(cluster, what)
        }
        _ => ApiError::kafka(cluster, format!("{what}: {err}")),
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({
            "code": self.code, "message": self.message,
            "cluster": self.cluster, "retriable": self.retriable,
        });
        (self.status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn error_serializes_to_structured_json() {
        let res = ApiError::cluster_not_found("prod").into_response();
        assert_eq!(res.status(), axum::http::StatusCode::NOT_FOUND);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["code"], "cluster_not_found");
        assert_eq!(v["cluster"], "prod");
        assert_eq!(v["retriable"], false);
        assert!(v["message"].as_str().unwrap().contains("prod"));
    }

    #[tokio::test]
    async fn timeout_is_504_and_retriable() {
        let res = ApiError::kafka_timeout("prod", "fetch metadata").into_response();
        assert_eq!(res.status(), axum::http::StatusCode::GATEWAY_TIMEOUT);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["retriable"], true);
    }
}
