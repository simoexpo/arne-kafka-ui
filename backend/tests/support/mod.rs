use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use arne::cluster::registry::ClusterRegistry;
use arne::config::{ClusterConfig, Limits};
use arne::state::AppState;
use http_body_util::BodyExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use std::sync::Arc;
use std::time::Duration;
use std::collections::BTreeMap;
use testcontainers_modules::kafka::apache::Kafka;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt, ReuseDirective};
use tokio::sync::OnceCell;
use tower::ServiceExt;

static KAFKA: OnceCell<(ContainerAsync<Kafka>, String)> = OnceCell::const_new();

const KAFKA_CONTAINER_NAME: &str = "arne-test-kafka";

/// The container lives in a `static`, which Rust never drops, so testcontainers'
/// drop-based cleanup never runs. Remove the container explicitly when the test
/// process exits — no container may outlive the test run.
extern "C" fn remove_kafka_container() {
    let _ = std::process::Command::new("docker")
        .args(["rm", "-f", KAFKA_CONTAINER_NAME])
        .output();
}

/// Matches the dev cluster's broker (`docker-compose.dev.yml`), so tests and
/// the thing you click through are never a major version apart.
const KAFKA_IMAGE_TAG: &str = "4.3.1";

pub async fn start_kafka() -> String {
    let (_c, bootstrap) = KAFKA
        .get_or_init(|| async {
            // Fixed name + reuse: if a previous run was killed before its exit
            // hook ran, the leftover container is adopted (and reset below)
            // instead of colliding on the name — residue is bounded at one.
            let container = Kafka::default()
                // Pinned to a 4.x broker: the module's default is 3.8, which
                // has no KIP-848 consumer protocol, so no test could cover the
                // group views' behaviour on the protocol Kafka now defaults
                // new clients to.
                .with_tag(KAFKA_IMAGE_TAG)
                // The image only defaults `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`
                // to 1 for this single-broker cluster; the transaction
                // coordinator's own internal topic (`__transaction_state`)
                // still defaults to replication factor 3 and min-ISR 2,
                // which a 1-broker cluster can never satisfy — without these,
                // `init_transactions` (used by `produce_transactional`, fix
                // round 3's N4 test) hangs/times out forever, not transiently.
                .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
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

/// The one cluster a test drives. Counters live on it, so an assertion reads
/// only the traffic this test caused.
pub fn handle_of(state: &AppState) -> Arc<arne::cluster::ClusterHandle> {
    state.registry.get("test").expect("the test cluster")
}

pub fn cluster_cfg(name: &str, bootstrap: &str) -> ClusterConfig {
    ClusterConfig {
        name: name.into(),
        bootstrap: bootstrap.into(),
        sasl: None,
        schema_registry: None,
        broker_call_stats_ms: 0,
    }
}

/// A cluster that reports what it sent the brokers, so a test can assert the
/// call shape of a page instead of trusting a reading of librdkafka's source.
/// The interval is short because every assertion waits for a fresh tally.
pub fn state_with_call_stats(bootstrap: &str) -> AppState {
    let mut cfg = cluster_cfg("test", bootstrap);
    cfg.broker_call_stats_ms = 100;
    AppState {
        registry: Arc::new(ClusterRegistry::from_config(vec![cfg]).unwrap()),
        limits: Arc::new(Limits::default()),
    }
}

/// The tally as it stands, once one exists.
pub async fn broker_calls(state: &AppState) -> BTreeMap<String, u64> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let handle = state.registry.get("test").unwrap();
        handle.serve_client_events();
        let report = handle.call_stats.report();
        if report.sampled_at.is_some() {
            return report.totals;
        }
        assert!(std::time::Instant::now() < deadline, "no tally arrived — are stats enabled?");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// What has been sent since `before`. Waits for a tally recorded after THIS
/// call started — the action is already finished by then, so that tally
/// necessarily includes every request it made. (Waiting on librdkafka's own
/// stamp does not work: it has one-second granularity, so a "newer" tally can
/// still predate the action.)
pub async fn calls_since(state: &AppState, before: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    let handle = state.registry.get("test").unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    handle.serve_client_events();
    let at_entry = handle.call_stats.report().recorded;
    loop {
        handle.serve_client_events();
        let report = handle.call_stats.report();
        if report.recorded > at_entry {
            let mut delta = BTreeMap::new();
            for (api, sent) in report.totals {
                let was = before.get(&api).copied().unwrap_or(0);
                if sent > was {
                    delta.insert(api, sent - was);
                }
            }
            return delta;
        }
        assert!(std::time::Instant::now() < deadline, "no fresh tally after the action");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub fn state_for(bootstrap: &str, extra: Vec<ClusterConfig>) -> AppState {
    state_with_limits(bootstrap, extra, Limits::default())
}

/// Throughput sampling is demand-driven and rate-limited by
/// `sampler_interval_secs`; a 0 interval makes every request sample, so a
/// test can take two samples back to back instead of waiting out a window.
pub fn state_with_limits(bootstrap: &str, extra: Vec<ClusterConfig>, limits: Limits) -> AppState {
    let mut clusters = vec![cluster_cfg("test", bootstrap)];
    clusters.extend(extra);
    AppState {
        registry: Arc::new(ClusterRegistry::from_config(clusters).unwrap()),
        limits: Arc::new(limits),
    }
}

/// The raw bytes of a response, optionally telling the server what encodings
/// the caller accepts — the only way to observe compression, which `get_json`
/// hides by decoding for you.
pub async fn get_bytes(app: Router, uri: &str, accept_encoding: Option<&str>) -> Vec<u8> {
    get_with_headers(app, uri, accept_encoding).await.1
}

pub async fn get_with_headers(
    app: Router,
    uri: &str,
    accept_encoding: Option<&str>,
) -> (axum::http::HeaderMap, Vec<u8>) {
    let mut req = Request::builder().uri(uri);
    if let Some(encoding) = accept_encoding {
        req = req.header("accept-encoding", encoding);
    }
    let res = app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let headers = res.headers().clone();
    // A stream never ends on its own, so take only what is already buffered.
    let body = tokio::time::timeout(Duration::from_secs(2), res.into_body().collect())
        .await
        .map(|b| b.unwrap().to_bytes().to_vec())
        .unwrap_or_default();
    (headers, body)
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

/// A consumer that stays joined to its group (heartbeating, auto-committing)
/// until dropped — for tests that need a group in Stable state with a real
/// member assignment, unlike `consume_and_commit`'s join-commit-leave.
pub struct LiveConsumer {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for LiveConsumer {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

pub fn spawn_live_consumer(bootstrap: &str, topic: &str, group: &str) -> LiveConsumer {
    spawn_live_consumer_inner(bootstrap, topic, group, true)
}

/// Joined and assigned, but never commits — the "actively consuming, position
/// unknown until it commits" case.
pub fn spawn_live_consumer_without_commits(bootstrap: &str, topic: &str, group: &str) -> LiveConsumer {
    spawn_live_consumer_inner(bootstrap, topic, group, false)
}

/// A consumer speaking the KIP-848 protocol, which Kafka 4 defaults new
/// clients toward. Such a group carries no assignment blob and the legacy
/// DescribeGroups reports it as `Dead` with no members, so only a
/// coordinator-side describe can tell the truth about it.
pub fn spawn_live_next_protocol_consumer(bootstrap: &str, topic: &str, group: &str) -> LiveConsumer {
    spawn_live_consumer_configured(bootstrap, topic, group, true, &[("group.protocol", "consumer")])
}

fn spawn_live_consumer_inner(bootstrap: &str, topic: &str, group: &str, commit: bool) -> LiveConsumer {
    spawn_live_consumer_configured(bootstrap, topic, group, commit, &[])
}

fn spawn_live_consumer_configured(
    bootstrap: &str,
    topic: &str,
    group: &str,
    commit: bool,
    extra: &[(&str, &str)],
) -> LiveConsumer {
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_flag = stop.clone();
    let (label, bootstrap, topic, group) =
        (format!("{topic}/{group}"), bootstrap.to_string(), topic.to_string(), group.to_string());
    let extra: Vec<(String, String)> =
        extra.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    // The thread reports whether it got as far as subscribing, so a setup
    // failure surfaces here instead of leaving the caller to time out waiting
    // for a consumer that never existed.
    let (started_tx, started_rx) = std::sync::mpsc::channel::<Result<(), String>>();
    let thread = std::thread::spawn(move || {
        let mut cc = client(&bootstrap);
        cc.set("group.id", &group)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", if commit { "true" } else { "false" })
            .set("auto.commit.interval.ms", "500");
        for (k, v) in &extra {
            cc.set(k, v);
        }
        let consumer: BaseConsumer = match cc.create() {
            Ok(c) => c,
            Err(e) => {
                let _ = started_tx.send(Err(format!("create consumer: {e}")));
                return;
            }
        };
        if let Err(e) = consumer.subscribe(&[&topic]) {
            let _ = started_tx.send(Err(format!("subscribe: {e}")));
            return;
        }
        let _ = started_tx.send(Ok(()));
        while !stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = consumer.poll(Duration::from_millis(200));
        }
    });
    match started_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("live consumer {label} failed to start: {e}"),
        Err(e) => panic!("live consumer {label} never reported startup: {e}"),
    }
    LiveConsumer { stop, thread: Some(thread) }
}

/// Start `app` on an ephemeral port and collect SSE events (name, json) from
/// `path` until an `app_error` event or `max` events. ("app_error", not
/// "error" — the wire event name deliberately avoids `EventSource`'s own
/// reserved "error" type; see `TimelineEvent`/`TailEvent::name`'s own
/// comment.)
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
            let terminal = name == "app_error";
            events.push((name, json));
            if terminal || events.len() >= max {
                return events;
            }
        }
    }
    events
}

/// Produces a single message with an arbitrary raw byte payload (as opposed
/// to `produce`'s string values) — used to construct confluent-framed
/// (magic byte + schema id) payloads by hand for decode-path tests.
pub async fn produce_raw(bootstrap: &str, topic: &str, key: &str, value: &[u8]) {
    let producer: FutureProducer = client(bootstrap).create().unwrap();
    producer
        .send(FutureRecord::to(topic).key(key).payload(value), Duration::from_secs(10))
        .await
        .unwrap();
}

/// Produces a single message with an explicit partition and timestamp — used
/// by timeline tests to construct interleaved, known cross-partition
/// timestamps (mirrors `produce`'s string values, but pins `.partition()` and
/// `.timestamp()` instead of leaving them to the producer's defaults).
pub async fn produce_at(bootstrap: &str, topic: &str, partition: i32, key: &str, value: &str, ts_ms: i64) {
    let producer: FutureProducer = client(bootstrap).create().unwrap();
    producer
        .send(
            FutureRecord::to(topic).key(key).payload(value).partition(partition).timestamp(ts_ms),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
}

/// Like `produce_at`, called once per `(key, value, ts_ms)` in `records`,
/// but reusing a single producer across every send instead of building a
/// fresh one each time.
///
/// This matters for more than throughput: a test producing a few hundred
/// messages via `produce_at` in a loop (a fresh `FutureProducer` — a new
/// broker connection and metadata round trip — per message) can easily take
/// 25-30+ real wall-clock seconds. Kafka's own log-retention sweep runs its
/// *first* pass roughly 30 seconds after broker startup regardless of the
/// configured check interval; timeline tests deliberately produce with
/// small, near-epoch-1970 timestamps (`ts_ms` in the low thousands) to get
/// deterministic cross-partition ordering, and once that first retention
/// sweep fires, every message with such an ancient `CreateTime` is judged
/// far older than `log.retention.hours` and silently deleted — the log's
/// low watermark jumps forward mid-test, and messages the test just wrote
/// vanish before it ever reads them back. Reusing one producer keeps a
/// several-hundred-message setup down to low single-digit seconds, safely
/// under that window.
pub async fn produce_at_many(bootstrap: &str, topic: &str, partition: i32, records: &[(String, String, i64)]) {
    let producer: FutureProducer = client(bootstrap).create().unwrap();
    for (key, value, ts_ms) in records {
        producer
            .send(
                FutureRecord::to(topic).key(key).payload(value).partition(partition).timestamp(*ts_ms),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }
}

/// Produces `count` messages (keys/values `k0`/`v0` .. ) to `topic` inside
/// one committed Kafka transaction.
///
/// Fix round 3, N4: this reproduces a real, healthy-topic offset hole with
/// zero broker slowness or compaction involved — a committed transaction's
/// own commit *control record* consumes a real offset in the partition
/// (typically the one immediately below the high watermark) but is never
/// delivered to a consumer's `poll()` as an application message. A
/// same-partition topic produced this way has exactly `count` real records
/// at offsets `0..count`, plus one hole at offset `count` (the control
/// record) — the high watermark is `count + 1`, not `count`.
///
/// Each call uses a fresh, process-and-time-unique `transactional.id`: Kafka
/// requires transactional producers to have a stable id, and reusing one
/// across the container's reused lifetime (see `start_kafka`'s reuse
/// comment) risks fencing a still-open transaction from a previous run.
pub async fn produce_transactional(bootstrap: &str, topic: &str, count: usize) {
    let unique = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let txn_id = format!("arne-test-txn-{}-{unique}", std::process::id());
    let mut cc = client(bootstrap);
    cc.set("transactional.id", &txn_id);
    let producer: FutureProducer = cc.create().unwrap();
    // The transaction coordinator (backed by the internal
    // `__transaction_state` topic) can still be electing/loading right after
    // a fresh init or a burst of prior transactional activity in the reused
    // container; librdkafka reports that as a retriable transaction error
    // ("retry call to resume"), not a real failure — so retry `init_transactions`
    // for a bit instead of treating the first timeout as fatal.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match producer.init_transactions(Duration::from_secs(10)) {
            Ok(()) => break,
            Err(rdkafka::error::KafkaError::Transaction(e)) if e.is_retriable() && std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(e) => panic!("init_transactions failed: {e}"),
        }
    }
    producer.begin_transaction().unwrap();
    for i in 0..count {
        let key = format!("k{i}");
        let value = format!("v{i}");
        producer
            .send(FutureRecord::to(topic).key(&key).payload(&value), Duration::from_secs(10))
            .await
            .unwrap();
    }
    loop {
        match producer.commit_transaction(Duration::from_secs(10)) {
            Ok(()) => break,
            Err(rdkafka::error::KafkaError::Transaction(e)) if e.is_retriable() && std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(e) => panic!("commit_transaction failed: {e}"),
        }
    }
}

/// Builds the 2-partition, 20-record-per-partition fixture shared by several
/// timeline direction/anchor tests: p0 offset o @ ts=1000+20o, p1 offset o @
/// ts=1010+20o — all timestamps distinct, so back/forward merge order is
/// fully determined (descending ts strictly alternates p1,p0 for equal
/// offsets). Assumes `topic` already has (at least) 2 partitions.
pub async fn produce_interleaved_fixture(bootstrap: &str, topic: &str) {
    let p0: Vec<(String, String, i64)> =
        (0..20i64).map(|o| (format!("p0k{o}"), format!("p0v{o}"), 1000 + 20 * o)).collect();
    let p1: Vec<(String, String, i64)> =
        (0..20i64).map(|o| (format!("p1k{o}"), format!("p1v{o}"), 1010 + 20 * o)).collect();
    produce_at_many(bootstrap, topic, 0, &p0).await;
    produce_at_many(bootstrap, topic, 1, &p1).await;
}

/// Extracts the `(partition, offset)` set of every `match` event in an SSE
/// event list collected by `collect_sse`.
pub fn offsets_of(events: &[(String, serde_json::Value)]) -> std::collections::HashSet<(i64, i64)> {
    events.iter().filter(|(n, _)| n == "match")
        .map(|(_, m)| (m["partition"].as_i64().unwrap(), m["offset"].as_i64().unwrap()))
        .collect()
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
