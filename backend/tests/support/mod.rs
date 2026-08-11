use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use betrachtung::cluster::registry::ClusterRegistry;
use betrachtung::config::{ClusterConfig, Limits};
use betrachtung::state::AppState;
use http_body_util::BodyExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::kafka::apache::Kafka;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;
use tokio::sync::OnceCell;
use tower::ServiceExt;

static KAFKA: OnceCell<(ContainerAsync<Kafka>, String)> = OnceCell::const_new();

pub async fn start_kafka() -> String {
    let (_c, bootstrap) = KAFKA
        .get_or_init(|| async {
            let container = Kafka::default().start().await.expect("start kafka container (is Docker running?)");
            let host = container.get_host().await.unwrap();
            let port = container.get_host_port_ipv4(9092).await.unwrap();
            (container, format!("{host}:{port}"))
        })
        .await;
    bootstrap.clone()
}

pub fn cluster_cfg(name: &str, bootstrap: &str) -> ClusterConfig {
    ClusterConfig { name: name.into(), bootstrap: bootstrap.into(), sasl: None, schema_registry: None }
}

pub fn state_for(bootstrap: &str, extra: Vec<ClusterConfig>) -> AppState {
    let mut clusters = vec![cluster_cfg("test", bootstrap)];
    clusters.extend(extra);
    AppState {
        registry: Arc::new(ClusterRegistry::from_config(clusters).unwrap()),
        limits: Arc::new(Limits::default()),
    }
}

pub async fn get_json(app: Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let res = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

fn client(bootstrap: &str) -> ClientConfig {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", bootstrap);
    cc
}

pub async fn create_topic(bootstrap: &str, name: &str, partitions: i32) {
    let admin: AdminClient<_> = client(bootstrap).create().unwrap();
    admin
        .create_topics(
            &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .unwrap();
}

pub async fn produce(bootstrap: &str, topic: &str, count: usize) {
    let producer: FutureProducer = client(bootstrap).create().unwrap();
    for i in 0..count {
        let key = format!("k{i}");
        let value = format!("v{i}");
        producer
            .send(FutureRecord::to(topic).key(&key).payload(&value), Duration::from_secs(10))
            .await
            .unwrap();
    }
}
