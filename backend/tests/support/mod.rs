use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use betrachtung::cluster::registry::ClusterRegistry;
use betrachtung::config::{ClusterConfig, Limits};
use betrachtung::state::AppState;
use http_body_util::BodyExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::kafka::apache::Kafka;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt, ReuseDirective};
use tokio::sync::OnceCell;
use tower::ServiceExt;

static KAFKA: OnceCell<(ContainerAsync<Kafka>, String)> = OnceCell::const_new();

const KAFKA_CONTAINER_NAME: &str = "betrachtung-test-kafka";

/// The container lives in a `static`, which Rust never drops, so testcontainers'
/// drop-based cleanup never runs. Remove the container explicitly when the test
/// process exits — no container may outlive the test run.
extern "C" fn remove_kafka_container() {
    let _ = std::process::Command::new("docker")
        .args(["rm", "-f", KAFKA_CONTAINER_NAME])
        .output();
}

pub async fn start_kafka() -> String {
    let (_c, bootstrap) = KAFKA
        .get_or_init(|| async {
            // Fixed name + reuse: if a previous run was killed before its exit
            // hook ran, the leftover container is adopted (and reset below)
            // instead of colliding on the name — residue is bounded at one.
            let container = Kafka::default()
                .with_container_name(KAFKA_CONTAINER_NAME)
                .with_reuse(ReuseDirective::Always)
                .start()
                .await
                .expect("start kafka container (is Docker running?)");
            unsafe { libc::atexit(remove_kafka_container) };
            let host = container.get_host().await.unwrap();
            let port = container.get_host_port_ipv4(9092).await.unwrap();
            let bootstrap = format!("{host}:{port}");
            wait_until_broker_ready(&bootstrap).await;
            reset_cluster_state(&bootstrap).await;
            (container, bootstrap)
        })
        .await;
    bootstrap.clone()
}

/// The container reports ready before the broker actually serves requests.
/// Probe both the metadata path and the group-coordinator path until they
/// succeed, so tests never race a half-started broker.
async fn wait_until_broker_ready(bootstrap: &str) {
    let bootstrap = bootstrap.to_string();
    tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let ready = (|| {
                let consumer: BaseConsumer = client(&bootstrap).create().ok()?;
                consumer.fetch_metadata(None, Duration::from_secs(2)).ok()?;
                let mut cc = client(&bootstrap);
                cc.set("group.id", "readiness-probe");
                let gc: BaseConsumer = cc.create().ok()?;
                gc.committed_offsets(rdkafka::TopicPartitionList::new(), Duration::from_secs(2)).ok()?;
                Some(())
            })();
            if ready.is_some() {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "kafka broker not ready within 60s");
            std::thread::sleep(Duration::from_millis(250));
        }
    })
    .await
    .unwrap();
}

/// The container is reused across test runs, so wipe topics and consumer
/// groups left behind by the previous run — tests assume a fresh cluster.
async fn reset_cluster_state(bootstrap: &str) {
    let (topics, groups) = {
        let bootstrap = bootstrap.to_string();
        tokio::task::spawn_blocking(move || {
            let consumer: BaseConsumer = client(&bootstrap).create().unwrap();
            let md = consumer.fetch_metadata(None, Duration::from_secs(10)).unwrap();
            let topics: Vec<String> = md.topics().iter()
                .map(|t| t.name().to_string())
                .filter(|n| !n.starts_with("__"))
                .collect();
            let gl = consumer.fetch_group_list(None, Duration::from_secs(10)).unwrap();
            let groups: Vec<String> = gl.groups().iter().map(|g| g.name().to_string()).collect();
            (topics, groups)
        })
        .await
        .unwrap()
    };

    let admin: AdminClient<_> = client(bootstrap).create().unwrap();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    if !groups.is_empty() {
        let refs: Vec<&str> = groups.iter().map(String::as_str).collect();
        // per-group failures (e.g. already gone) are fine; deletion is best-effort
        let _ = admin.delete_groups(&refs, &opts).await;
    }
    if !topics.is_empty() {
        let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
        admin.delete_topics(&refs, &opts).await.unwrap();
        // topic deletion is asynchronous — wait until they are actually gone
        // so tests can recreate same-named topics without racing
        let bootstrap = bootstrap.to_string();
        tokio::task::spawn_blocking(move || {
            let consumer: BaseConsumer = client(&bootstrap).create().unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                let md = consumer.fetch_metadata(None, Duration::from_secs(5)).unwrap();
                if md.topics().iter().all(|t| t.name().starts_with("__") || !topics.contains(&t.name().to_string())) {
                    return;
                }
                assert!(std::time::Instant::now() < deadline, "stale topics not deleted within 30s");
                std::thread::sleep(Duration::from_millis(200));
            }
        })
        .await
        .unwrap();
    }
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

pub async fn consume_and_commit(bootstrap: &str, topic: &str, group: &str, count: usize) {
    let bootstrap = bootstrap.to_string();
    let topic = topic.to_string();
    let group = group.to_string();
    tokio::task::spawn_blocking(move || {
        let mut cc = client(&bootstrap);
        cc.set("group.id", &group)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false");
        let consumer: BaseConsumer = cc.create().unwrap();
        consumer.subscribe(&[&topic]).unwrap();
        let mut seen = 0;
        while seen < count {
            // transient errors (e.g. transport failures while brokers settle)
            // are informational events; keep polling
            if let Some(Ok(_)) = consumer.poll(Duration::from_secs(10)) {
                seen += 1;
            }
        }
        consumer.commit_consumer_state(CommitMode::Sync).unwrap();
    })
    .await
    .unwrap();
}

/// Start `app` on an ephemeral port and collect SSE events (name, json) from
/// `path` until a `done`/`error` event or `max` events.
pub async fn collect_sse(app: Router, path: &str, max: usize) -> Vec<(String, serde_json::Value)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let res = reqwest::get(format!("http://{addr}{path}")).await.unwrap();
    assert_eq!(res.status(), 200, "sse endpoint status");
    let mut events = Vec::new();
    let mut buf = String::new();
    let mut stream = res.bytes_stream();
    use futures_util::StreamExt;
    while let Some(chunk) = stream.next().await {
        buf.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        while let Some(pos) = buf.find("\n\n") {
            let frame = buf[..pos].to_string();
            buf.drain(..pos + 2);
            let mut name = String::new();
            let mut data = String::new();
            for line in frame.lines() {
                if let Some(v) = line.strip_prefix("event: ") { name = v.to_string(); }
                if let Some(v) = line.strip_prefix("data: ") { data.push_str(v); }
            }
            if name.is_empty() { continue; } // keep-alive comments
            let json = serde_json::from_str(&data).unwrap_or(serde_json::Value::Null);
            let terminal = name == "done" || name == "error";
            events.push((name, json));
            if terminal || events.len() >= max {
                return events;
            }
        }
    }
    events
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
