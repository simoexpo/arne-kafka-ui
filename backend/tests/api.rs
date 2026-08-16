mod support;

use axum::http::StatusCode;
use betrachtung::api::app;
use betrachtung::cluster::{ClusterHandle, HealthStatus};
use std::sync::Arc;
use support::*;
use tower::ServiceExt;

#[tokio::test]
async fn clusters_endpoint_reports_health_per_cluster() {
    let bootstrap = start_kafka().await;
    // second cluster points at a closed port → must show unreachable without breaking the healthy one
    let state = state_for(&bootstrap, vec![cluster_cfg("dead", "localhost:1")]);
    let (status, body) = get_json(app(state), "/api/clusters").await;
    assert_eq!(status, 200);
    let clusters = body["clusters"].as_array().unwrap();
    assert_eq!(clusters.len(), 2);
    assert_eq!(clusters[0]["name"], "test");
    assert_eq!(clusters[0]["status"], "healthy");
    assert!(clusters[0]["broker_count"].as_u64().unwrap() >= 1);
    assert_eq!(clusters[1]["name"], "dead");
    assert_eq!(clusters[1]["status"], "unreachable");
    assert!(clusters[1]["error"].is_string());
}

/// Two unreachable clusters: sequential health checks cost ~2x HEALTH_TIMEOUT,
/// parallel ~1x. The bound sits between the two (a failure bound, not a
/// green-path sleep) — proves `/api/clusters` fans health checks out instead
/// of awaiting them one at a time.
#[tokio::test]
async fn clusters_health_checks_run_in_parallel() {
    let state = state_for("127.0.0.1:1", vec![cluster_cfg("dead-2", "127.0.0.1:2")]);
    let started = std::time::Instant::now();
    let (status, body) = get_json(app(state), "/api/clusters").await;
    let elapsed = started.elapsed();
    assert_eq!(status, StatusCode::OK);
    let clusters = body["clusters"].as_array().unwrap();
    assert_eq!(clusters.len(), 2);
    assert!(clusters.iter().all(|c| c["status"] == "unreachable"));
    assert!(
        elapsed < std::time::Duration::from_millis(3500),
        "health checks must fan out in parallel, took {elapsed:?}"
    );
}

#[tokio::test]
async fn topics_inventory_lists_topic_with_message_estimate() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "inv-topic", 3).await;
    produce(&bootstrap, "inv-topic", 12).await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/test/topics").await;
    assert_eq!(status, 200);
    let topics = body["topics"].as_array().unwrap();
    let t = topics.iter().find(|t| t["name"] == "inv-topic").expect("topic listed");
    assert_eq!(t["partitions"], 3);
    assert_eq!(t["replication_factor"], 1);
    assert_eq!(t["message_estimate"], 12);
    assert_eq!(t["internal"], false);
    assert!(body["as_of"].as_i64().unwrap() > 0);
}

/// Regression: `list_topics` used to `fetch_watermarks` every partition of
/// every topic, internal ones included — `__transaction_state` alone can
/// carry 50 partitions on a cluster with transactional producers, and any
/// single watermark failure aborted the whole request (502, blank topic
/// inventory). Internal topics are hidden by default and their estimates
/// aren't shown meaningfully, so they must be skipped entirely: null
/// estimate, no error, and — critically — the request as a whole still
/// succeeds.
#[tokio::test]
async fn topics_inventory_skips_watermarks_for_internal_topics() {
    let bootstrap = start_kafka().await;
    // force __consumer_offsets into existence via a real committed offset,
    // rather than relying on another test having already created it
    create_topic(&bootstrap, "inv-internal-topic", 1).await;
    produce(&bootstrap, "inv-internal-topic", 1).await;
    consume_and_commit(&bootstrap, "inv-internal-topic", "inv-internal-group", 1).await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/test/topics").await;
    assert_eq!(status, 200);
    let topics = body["topics"].as_array().unwrap();
    let t = topics.iter().find(|t| t["name"] == "__consumer_offsets")
        .expect("__consumer_offsets topic listed");
    assert_eq!(t["internal"], true);
    assert!(t["message_estimate"].is_null());
    assert!(t["estimate_error"].is_null());
}

#[tokio::test]
async fn topic_detail_shows_partitions_offsets_and_configs() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "detail-topic", 2).await;
    produce(&bootstrap, "detail-topic", 5).await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/test/topics/detail-topic").await;
    assert_eq!(status, 200);
    assert_eq!(body["name"], "detail-topic");
    let parts = body["partitions"].as_array().unwrap();
    assert_eq!(parts.len(), 2);
    let total: i64 = parts.iter().map(|p| p["end_offset"].as_i64().unwrap() - p["start_offset"].as_i64().unwrap()).sum();
    assert_eq!(total, 5);
    assert!(parts[0]["leader"].as_i64().is_some());
    assert!(!parts[0]["isr"].as_array().unwrap().is_empty());
    let configs = body["configs"].as_array().unwrap();
    assert!(configs.iter().any(|c| c["name"] == "retention.ms"), "expected retention.ms in configs");
}

/// Regression: `describe_configs` inside `topic_detail` must carry the same
/// explicit ADMIN_TIMEOUT as every other Kafka call. Without it, librdkafka's
/// default request timeout (~60s) applies, so a dead broker makes this
/// endpoint stall far longer than any other admin call and far longer than
/// the frontend's own 15s timeout — which then misreports "backend
/// unreachable" instead of a Kafka-side timeout/error.
#[tokio::test]
async fn topic_detail_on_dead_cluster_fails_fast() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![cluster_cfg("dead", "localhost:1")]);
    let start = std::time::Instant::now();
    let (status, body) = get_json(app(state), "/api/clusters/dead/topics/some-topic").await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < std::time::Duration::from_millis(15_000),
        "topic_detail on a dead cluster must fail within the same ADMIN_TIMEOUT bound as \
         other admin calls (< 15s), took {elapsed:?}: body={body}"
    );
    assert!(
        status == StatusCode::BAD_GATEWAY || status == StatusCode::GATEWAY_TIMEOUT,
        "expected 502 or 504, got {status}: body={body}"
    );
    assert!(
        body["code"] == "kafka_error" || body["code"] == "kafka_timeout",
        "expected kafka_error or kafka_timeout, got: {body}"
    );
}

#[tokio::test]
async fn unknown_topic_detail_is_404() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/test/topics/ghost-topic").await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], "topic_not_found");
}

#[tokio::test]
async fn groups_list_and_detail_report_lag() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "lag-topic", 1).await;
    produce(&bootstrap, "lag-topic", 10).await;
    consume_and_commit(&bootstrap, "lag-topic", "lag-group", 4).await;
    let state = state_for(&bootstrap, vec![]);

    let (status, body) = get_json(app(state.clone()), "/api/clusters/test/groups").await;
    assert_eq!(status, 200);
    let g = body["groups"].as_array().unwrap().iter()
        .find(|g| g["group_id"] == "lag-group").expect("group listed");
    assert_eq!(g["total_lag"], 6); // 10 produced - 4 committed

    let (status, body) = get_json(app(state), "/api/clusters/test/groups/lag-group").await;
    assert_eq!(status, 200);
    assert_eq!(body["group_id"], "lag-group");
    let parts = body["partitions"].as_array().unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0]["topic"], "lag-topic");
    assert_eq!(parts[0]["committed_offset"], 4);
    assert_eq!(parts[0]["end_offset"], 10);
    assert_eq!(parts[0]["lag"], 6);
}

#[tokio::test]
async fn topic_consumers_lists_groups_reading_the_topic() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tc-topic", 1).await;
    create_topic(&bootstrap, "tc-other", 1).await;
    produce(&bootstrap, "tc-topic", 6).await;
    produce(&bootstrap, "tc-other", 3).await;
    consume_and_commit(&bootstrap, "tc-topic", "tc-group", 6).await;
    consume_and_commit(&bootstrap, "tc-other", "tc-other-group", 3).await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/test/topics/tc-topic/consumers").await;
    assert_eq!(status, 200);
    let groups = body["groups"].as_array().unwrap();
    assert!(groups.iter().any(|g| g["group_id"] == "tc-group"));
    assert!(!groups.iter().any(|g| g["group_id"] == "tc-other-group"), "unrelated group must not appear");
    let g = groups.iter().find(|g| g["group_id"] == "tc-group").unwrap();
    assert_eq!(g["total_lag"], 0);
}

#[tokio::test]
async fn unknown_group_is_404() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/test/groups/ghost-group").await;
    assert_eq!(status, 404, "body: {body}");
    assert_eq!(body["code"], "group_not_found");
}

#[tokio::test]
async fn throughput_endpoint_reports_positive_rate_after_producing() {
    use betrachtung::cluster::sampler::spawn_sampler;
    use std::time::Duration;

    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tp-topic", 1).await;
    let state = state_for(&bootstrap, vec![]);
    let handle = state.registry.get("test").unwrap();
    let _task = spawn_sampler(handle, Duration::from_millis(500));

    produce(&bootstrap, "tp-topic", 20).await;
    // wait for at least two samples spanning the produce
    let mut found = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (status, body) = get_json(app(state.clone()), "/api/clusters/test/topics/tp-topic/throughput").await;
        assert_eq!(status, 200);
        let samples = body["samples"].as_array().unwrap();
        if samples.iter().any(|p| p["msgs_per_sec"].as_f64().unwrap() > 0.0) {
            assert!(body["as_of"].as_i64().unwrap() > 0);
            found = true;
            break;
        }
    }
    assert!(found, "expected a positive throughput sample within 15s");
}

#[tokio::test]
async fn overview_reports_brokers_and_counts() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "ov-topic", 2).await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/test/overview").await;
    assert_eq!(status, 200);
    assert_eq!(body["brokers"].as_array().unwrap().len(), 1);
    assert!(body["topic_count"].as_u64().unwrap() >= 1);
    assert!(body["partition_count"].as_u64().unwrap() >= 2);
    assert_eq!(body["under_replicated_partitions"], 0);
}

#[tokio::test]
async fn full_app_boots_and_serves_over_tcp() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(state)).await.unwrap();
    });
    let base = format!("http://{addr}");
    let health = reqwest::get(format!("{base}/healthz")).await.unwrap();
    assert_eq!(health.status(), 200);
    let clusters: serde_json::Value = reqwest::get(format!("{base}/api/clusters"))
        .await.unwrap().json().await.unwrap();
    assert_eq!(clusters["clusters"][0]["status"], "healthy");
}

#[tokio::test]
async fn topics_on_unknown_cluster_is_404() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/ghost/topics").await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], "cluster_not_found");
}

#[tokio::test]
async fn tail_streams_new_messages() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tail-topic", 1).await;
    produce(&bootstrap, "tail-topic", 5).await; // pre-existing, must NOT appear
    let state = state_for(&bootstrap, vec![]);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
    let res = reqwest::get(format!("http://{addr}/api/clusters/test/topics/tail-topic/tail")).await.unwrap();
    assert_eq!(res.status(), 200);

    // give the tail consumer a moment to assign at end, then produce
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    produce(&bootstrap, "tail-topic", 3).await;

    use futures_util::StreamExt;
    let mut stream = res.bytes_stream();
    let mut buf = String::new();
    let mut got = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    while got.len() < 3 && tokio::time::Instant::now() < deadline {
        let chunk = tokio::time::timeout_at(deadline, stream.next()).await;
        let Ok(Some(Ok(bytes))) = chunk else { break };
        buf.push_str(&String::from_utf8_lossy(&bytes));
        while let Some(pos) = buf.find("\n\n") {
            let frame = buf[..pos].to_string();
            buf.drain(..pos + 2);
            for line in frame.lines() {
                if let Some(data) = line.strip_prefix("data: ")
                    && let Ok(v) = serde_json::from_str::<serde_json::Value>(data)
                {
                    got.push(v["value"]["text"].as_str().unwrap_or_default().to_string());
                }
            }
        }
    }
    assert_eq!(got, vec!["v0", "v1", "v2"], "tail must stream only new messages in order");
}

/// End-to-end "never silently skipped" guard: a confluent-framed message
/// (magic byte 0x0 + a schema id) lands on a cluster with NO schema
/// registry configured. Nothing should ever drop or hide this record — it
/// must surface as `encoding: "decode_error"` with a non-empty error and
/// the raw bytes as base64, through the `/timeline` endpoint that replaced
/// both `/messages` (browse) and `/search`: once unfiltered (a plain back
/// page) and once with `filter=key_contains` matching its (decodable) key —
/// a matched record whose *value* failed to decode must never be dropped
/// from a filtered page either.
#[tokio::test]
async fn confluent_framed_message_without_registry_surfaces_as_decode_error_everywhere() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "confluent-no-sr-topic", 1).await;
    // magic byte 0x00 + fake schema id 999 (big-endian i32), then arbitrary
    // bytes that are neither valid avro/protobuf nor meant to be — there's
    // no registry to even attempt a decode against.
    let mut payload = vec![0u8, 0, 0, 3, 231];
    payload.extend_from_slice(b"not-a-real-schema-payload");
    produce_raw(&bootstrap, "confluent-no-sr-topic", "confluent-key", &payload).await;
    // The "test" cluster (state_for) has no schema_registry configured.
    let state = state_for(&bootstrap, vec![]);

    // (a) Unfiltered back page from latest must show the record with its
    // value surfaced as a decode error, never silently dropped.
    let events = collect_sse(
        app(state.clone()),
        "/api/clusters/test/topics/confluent-no-sr-topic/timeline?direction=back&limit=10&anchor=latest",
        50,
    ).await;
    let matches: Vec<_> = events.iter().filter(|(n, _)| n == "match").collect();
    assert_eq!(matches.len(), 1, "events: {events:?}");
    let v = &matches[0].1["value"];
    assert_eq!(v["encoding"], "decode_error", "timeline must surface the decode failure, not skip it: {v:?}");
    assert!(v["error"].as_str().is_some_and(|e| !e.is_empty()), "expected a non-empty error: {v:?}");
    assert!(v["text"].as_str().is_some_and(|t| !t.is_empty()), "expected base64 raw bytes: {v:?}");

    // (b) Filter on the key (which decodes fine as utf8) so the match
    // criterion doesn't depend on the value's content — the point is that a
    // record matched by its key still surfaces in a filtered timeline page
    // even though its value failed to decode.
    let events = collect_sse(
        app(state),
        "/api/clusters/test/topics/confluent-no-sr-topic/timeline?direction=back&limit=10&anchor=latest&filter=key_contains&q=confluent",
        50,
    ).await;
    let matches: Vec<_> = events.iter().filter(|(n, _)| n == "match").collect();
    assert_eq!(matches.len(), 1, "events: {events:?}");
    let (_, m) = matches[0];
    assert_eq!(m["value"]["encoding"], "decode_error", "filtered timeline must surface the decode failure too: {m:?}");
    assert!(m["value"]["error"].as_str().is_some_and(|e| !e.is_empty()), "expected a non-empty error: {m:?}");
    assert!(m["value"]["text"].as_str().is_some_and(|t| !t.is_empty()), "expected base64 raw bytes: {m:?}");
}

#[tokio::test]
async fn timeline_first_page_is_globally_ordered_and_paginates_back() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-topic", 2).await;
    // interleaved timestamps across partitions: p0 evens, p1 odds, ts = 1000+i*10
    for i in 0..30i64 {
        produce_at(&bootstrap, "tl-topic", (i % 2) as i32, &format!("k{i}"), &format!("v{i}"), 1000 + i * 10).await;
    }
    let state = state_for(&bootstrap, vec![]);
    let events = collect_sse(app(state.clone()), "/api/clusters/test/topics/tl-topic/timeline?direction=back&limit=10&anchor=latest", 200).await;
    let matches: Vec<_> = events.iter().filter(|(n, _)| n == "match").map(|(_, m)| m.clone()).collect();
    assert_eq!(matches.len(), 10);
    let ts: Vec<i64> = matches.iter().map(|m| m["timestamp_ms"].as_i64().unwrap()).collect();
    let mut sorted = ts.clone(); sorted.sort(); sorted.reverse();
    assert_eq!(ts, sorted, "page must be newest-first globally: {ts:?}");
    assert_eq!(matches[0]["value"]["text"], "v29");
    assert_eq!(matches[9]["value"]["text"], "v20");
    let (_, end) = events.iter().find(|(n, _)| n == "page_end").unwrap().clone();
    assert_eq!(end["exhausted"], false);
    let cursor = end["cursor"].as_str().unwrap().to_string();

    // page 2 continues without gap or overlap
    let events2 = collect_sse(app(state), &format!("/api/clusters/test/topics/tl-topic/timeline?direction=back&limit=10&cursor={}", urlencoding::encode(&cursor)), 200).await;
    let m2: Vec<_> = events2.iter().filter(|(n, _)| n == "match").map(|(_, m)| m["value"]["text"].as_str().unwrap().to_string()).collect();
    assert_eq!(m2.first().unwrap(), "v19");
    assert_eq!(m2.last().unwrap(), "v10");
}

#[tokio::test]
async fn timeline_beginning_forward_reaches_exhaustion() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-fwd-topic", 1).await;
    for i in 0..5i64 { produce_at(&bootstrap, "tl-fwd-topic", 0, &format!("k{i}"), &format!("v{i}"), 2000 + i).await; }
    let state = state_for(&bootstrap, vec![]);
    let events = collect_sse(app(state), "/api/clusters/test/topics/tl-fwd-topic/timeline?direction=forward&limit=10&anchor=beginning", 200).await;
    let m: Vec<_> = events.iter().filter(|(n, _)| n == "match").map(|(_, v)| v["value"]["text"].as_str().unwrap().to_string()).collect();
    assert_eq!(m, vec!["v0", "v1", "v2", "v3", "v4"]); // forward pages are oldest-first
    let (_, end) = events.iter().find(|(n, _)| n == "page_end").unwrap().clone();
    assert_eq!(end["exhausted"], true);
    assert!(end["cursor"].is_null());
}

#[tokio::test]
async fn timeline_bad_params_is_400_before_streaming() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/test/topics/x/timeline?direction=sideways&limit=10&anchor=latest").await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(body["code"], "bad_request");
}

/// Fix round 1, C1+C2 (reviewer's exact construction): p0 = [o0@100, o1@300,
/// o2@300], p1 = [o0@400, o1@400], back limit 3.
///
/// C1: within an equal-timestamp tie in one partition, `Back` must break
/// ties by offset *descending* — o2 (the higher offset, same timestamp as
/// o1) must win a spot on page 1 over o1, or the wrong record gets kept.
/// C2: because of that, page 1 (which can't fit all 5 records at limit=3)
/// must NOT report `exhausted: true` — there's a whole record (o1) still
/// pending in p0.
///
/// The pagination must be loss-free end to end: across as many pages as it
/// takes, every one of the 5 (partition, offset) pairs must show up exactly
/// (no gaps — and for this monotonic-timestamp case, no overlap either).
#[tokio::test]
async fn timeline_back_ties_are_lossless_and_not_falsely_exhausted() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-tie-topic", 2).await;
    produce_at(&bootstrap, "tl-tie-topic", 0, "k", "p0o0", 100).await; // p0 offset 0
    produce_at(&bootstrap, "tl-tie-topic", 0, "k", "p0o1", 300).await; // p0 offset 1
    produce_at(&bootstrap, "tl-tie-topic", 0, "k", "p0o2", 300).await; // p0 offset 2 (tied with o1)
    produce_at(&bootstrap, "tl-tie-topic", 1, "k", "p1o0", 400).await; // p1 offset 0
    produce_at(&bootstrap, "tl-tie-topic", 1, "k", "p1o1", 400).await; // p1 offset 1 (tied with o0)
    let state = state_for(&bootstrap, vec![]);

    let events1 = collect_sse(app(state.clone()), "/api/clusters/test/topics/tl-tie-topic/timeline?direction=back&limit=3&anchor=latest", 200).await;
    let vals1: Vec<String> = events1.iter().filter(|(n, _)| n == "match")
        .map(|(_, m)| m["value"]["text"].as_str().unwrap().to_string()).collect();
    // C1: p1's tie (o0, o1 @400) resolves o1-before-o0 (offset desc); p0's
    // tie (o1, o2 @300) resolves o2-before-o1 — o1 must NOT appear yet.
    assert_eq!(vals1, vec!["p1o1", "p1o0", "p0o2"], "back tie-break must keep the higher offset first: {vals1:?}");
    let (_, end1) = events1.iter().find(|(n, _)| n == "page_end").unwrap().clone();
    assert_eq!(end1["exhausted"], false, "C2: a page that couldn't fit everything must not claim exhaustion: {end1}");
    let cursor = end1["cursor"].as_str().unwrap().to_string();

    let events2 = collect_sse(app(state), &format!("/api/clusters/test/topics/tl-tie-topic/timeline?direction=back&limit=3&cursor={}", urlencoding::encode(&cursor)), 200).await;
    let vals2: Vec<String> = events2.iter().filter(|(n, _)| n == "match")
        .map(|(_, m)| m["value"]["text"].as_str().unwrap().to_string()).collect();
    assert_eq!(vals2, vec!["p0o1", "p0o0"]);
    let (_, end2) = events2.iter().find(|(n, _)| n == "page_end").unwrap().clone();
    assert_eq!(end2["exhausted"], true);

    let mut seen: std::collections::HashSet<String> = vals1.into_iter().collect();
    seen.extend(vals2);
    let expected: std::collections::HashSet<String> =
        ["p0o0", "p0o1", "p0o2", "p1o0", "p1o1"].iter().map(|s| s.to_string()).collect();
    assert_eq!(seen, expected, "every record must be emitted exactly once across pages");
}

/// Fix round 1, C3 (reviewer's exact construction): p0 = [o0@500, o1@300,
/// o2@300] (non-monotonic: offset 0 has the *highest* timestamp), p1 =
/// [o0@100, o1@100], forward limit 3 from beginning.
///
/// Originally (round 1) this asserted only that the SET of (partition,
/// offset) covered all 5 — round 1's dropped-offset cursor formula could
/// legitimately re-offer an already-emitted record on a later page (bounded
/// overlap was an accepted tradeoff then). Fix round 2 replaced truncation
/// with contiguous selection, which provably never re-offers an
/// already-taken offset (each partition's next window always starts
/// exactly where its last taking left off) — so this is now tightened to
/// assert zero duplicates as well: every (partition, offset) appears
/// exactly once across all pages, not just "at least once".
#[tokio::test]
async fn timeline_forward_nonmonotonic_timestamps_are_lossless() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-nonmono-topic", 2).await;
    produce_at(&bootstrap, "tl-nonmono-topic", 0, "k", "p0o0", 500).await; // p0 offset 0 — newest timestamp, oldest offset
    produce_at(&bootstrap, "tl-nonmono-topic", 0, "k", "p0o1", 300).await; // p0 offset 1
    produce_at(&bootstrap, "tl-nonmono-topic", 0, "k", "p0o2", 300).await; // p0 offset 2 (tied with o1)
    produce_at(&bootstrap, "tl-nonmono-topic", 1, "k", "p1o0", 100).await; // p1 offset 0
    produce_at(&bootstrap, "tl-nonmono-topic", 1, "k", "p1o1", 100).await; // p1 offset 1 (tied with o0)
    let state = state_for(&bootstrap, vec![]);

    let mut seen: Vec<(i64, i64)> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut exhausted = false;
    for _ in 0..10 {
        // safety bound: must terminate well before this in practice
        let path = match &cursor {
            None => "/api/clusters/test/topics/tl-nonmono-topic/timeline?direction=forward&limit=3&anchor=beginning".to_string(),
            Some(c) => format!("/api/clusters/test/topics/tl-nonmono-topic/timeline?direction=forward&limit=3&cursor={}", urlencoding::encode(c)),
        };
        let events = collect_sse(app(state.clone()), &path, 200).await;
        for (name, m) in &events {
            if name == "match" {
                seen.push((m["partition"].as_i64().unwrap(), m["offset"].as_i64().unwrap()));
            }
        }
        let (_, end) = events.iter().find(|(n, _)| n == "page_end").unwrap().clone();
        exhausted = end["exhausted"].as_bool().unwrap();
        cursor = end["cursor"].as_str().map(str::to_string);
        if exhausted { break; }
    }
    assert!(exhausted, "must reach exhaustion within the safety bound, not loop forever");
    let seen_set: std::collections::HashSet<(i64, i64)> = seen.iter().copied().collect();
    assert_eq!(seen.len(), seen_set.len(), "fix round 2: contiguous selection must never re-offer an already-taken record: {seen:?}");
    let expected: std::collections::HashSet<(i64, i64)> =
        [(0i64, 0i64), (0, 1), (0, 2), (1, 0), (1, 1)].into_iter().collect();
    assert_eq!(seen_set, expected, "every (partition, offset) must be covered across pages, even with non-monotonic timestamps");
}

/// Fix round 2, N1 (reviewer's probe H, Back direction — exact
/// construction): p0 = [o0@900, o1@100], p1 = [o0@800, o1@200], back
/// limit 2.
///
/// Round 1's cursor formula anchored on the *dropped* offsets:
/// `Back` new position = `max(dropped) + 1`. Here, for `Back`, the
/// position-adjacent offset in p0's window is o1 (the window's top,
/// highest offset) — but o1 has the lowest timestamp of all 4 records, so
/// it loses every global-timestamp comparison and is *always* the one
/// truncated away. That makes it the *only* dropped offset for p0 every
/// single page: `max({1}) + 1 == 2 == p0's old position`, forever — a
/// byte-identical, non-exhausted cursor (N1's livelock), reproduced here
/// deterministically (no real broker slowness needed, unlike the N3 short-
/// read case).
///
/// Fix round 2's contiguous selection can't reproduce this: p0's position
/// only ever moves by however many of its own contiguous records were
/// actually taken (0 here, since p1 wins every comparison until it's
/// exhausted) — so p0's cursor stays at 2 for exactly one page (correct:
/// nothing was skipped, nothing lost), then p1's stream empties and p0
/// takes its turn on page 2, reaching true exhaustion. All 4 records must
/// be served, cursors must strictly advance (never repeat), and — since
/// contiguous selection never overlaps — with zero duplicates.
#[tokio::test]
async fn timeline_back_position_adjacent_drop_does_not_livelock() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-n1-back-topic", 2).await;
    produce_at(&bootstrap, "tl-n1-back-topic", 0, "k", "p0o0", 900).await;
    produce_at(&bootstrap, "tl-n1-back-topic", 0, "k", "p0o1", 100).await;
    produce_at(&bootstrap, "tl-n1-back-topic", 1, "k", "p1o0", 800).await;
    produce_at(&bootstrap, "tl-n1-back-topic", 1, "k", "p1o1", 200).await;
    let state = state_for(&bootstrap, vec![]);

    let mut seen: Vec<(i64, i64)> = Vec::new();
    let mut seen_cursors: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut exhausted = false;
    for _ in 0..5 {
        let path = match &cursor {
            None => "/api/clusters/test/topics/tl-n1-back-topic/timeline?direction=back&limit=2&anchor=latest".to_string(),
            Some(c) => format!("/api/clusters/test/topics/tl-n1-back-topic/timeline?direction=back&limit=2&cursor={}", urlencoding::encode(c)),
        };
        let events = collect_sse(app(state.clone()), &path, 200).await;
        for (name, m) in &events {
            if name == "match" {
                seen.push((m["partition"].as_i64().unwrap(), m["offset"].as_i64().unwrap()));
            }
        }
        let (_, end) = events.iter().find(|(n, _)| n == "page_end").unwrap().clone();
        exhausted = end["exhausted"].as_bool().unwrap();
        let next_cursor = end["cursor"].as_str().map(str::to_string);
        if let Some(c) = &next_cursor {
            assert!(!seen_cursors.contains(c), "cursor must strictly advance, never repeat (N1 livelock): {seen_cursors:?} then {c}");
            seen_cursors.push(c.clone());
        }
        cursor = next_cursor;
        if exhausted { break; }
    }
    assert!(exhausted, "must reach exhaustion within the safety bound, not livelock");
    let seen_set: std::collections::HashSet<(i64, i64)> = seen.iter().copied().collect();
    assert_eq!(seen.len(), seen_set.len(), "contiguous selection must never duplicate: {seen:?}");
    let expected: std::collections::HashSet<(i64, i64)> =
        [(0i64, 0i64), (0, 1), (1, 0), (1, 1)].into_iter().collect();
    assert_eq!(seen_set, expected, "every record must be served, with no losses");
}

/// Fix round 2, N1 (reviewer's probe I, Forward direction — mirrors probe H
/// above): same data, forward from beginning, limit 2. The livelock is
/// symmetric: p0's position-adjacent offset for `Forward` is o0 (the
/// window's bottom), which has the *highest* timestamp of all 4 — so it
/// also loses every comparison and was, under round 1, the sole "dropped"
/// offset every page, making `min(dropped) == p0's old position` forever.
#[tokio::test]
async fn timeline_forward_position_adjacent_drop_does_not_livelock() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-n1-fwd-topic", 2).await;
    produce_at(&bootstrap, "tl-n1-fwd-topic", 0, "k", "p0o0", 900).await;
    produce_at(&bootstrap, "tl-n1-fwd-topic", 0, "k", "p0o1", 100).await;
    produce_at(&bootstrap, "tl-n1-fwd-topic", 1, "k", "p1o0", 800).await;
    produce_at(&bootstrap, "tl-n1-fwd-topic", 1, "k", "p1o1", 200).await;
    let state = state_for(&bootstrap, vec![]);

    let mut seen: Vec<(i64, i64)> = Vec::new();
    let mut seen_cursors: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut exhausted = false;
    for _ in 0..5 {
        let path = match &cursor {
            None => "/api/clusters/test/topics/tl-n1-fwd-topic/timeline?direction=forward&limit=2&anchor=beginning".to_string(),
            Some(c) => format!("/api/clusters/test/topics/tl-n1-fwd-topic/timeline?direction=forward&limit=2&cursor={}", urlencoding::encode(c)),
        };
        let events = collect_sse(app(state.clone()), &path, 200).await;
        for (name, m) in &events {
            if name == "match" {
                seen.push((m["partition"].as_i64().unwrap(), m["offset"].as_i64().unwrap()));
            }
        }
        let (_, end) = events.iter().find(|(n, _)| n == "page_end").unwrap().clone();
        exhausted = end["exhausted"].as_bool().unwrap();
        let next_cursor = end["cursor"].as_str().map(str::to_string);
        if let Some(c) = &next_cursor {
            assert!(!seen_cursors.contains(c), "cursor must strictly advance, never repeat (N1 livelock): {seen_cursors:?} then {c}");
            seen_cursors.push(c.clone());
        }
        cursor = next_cursor;
        if exhausted { break; }
    }
    assert!(exhausted, "must reach exhaustion within the safety bound, not livelock");
    let seen_set: std::collections::HashSet<(i64, i64)> = seen.iter().copied().collect();
    assert_eq!(seen.len(), seen_set.len(), "contiguous selection must never duplicate: {seen:?}");
    let expected: std::collections::HashSet<(i64, i64)> =
        [(0i64, 0i64), (0, 1), (1, 0), (1, 1)].into_iter().collect();
    assert_eq!(seen_set, expected, "every record must be served, with no losses");
}

/// Superseded by spec v1.6 (owner ruling): direction now belongs to the
/// REQUEST, not the cursor blob — a cursor minted by a back page must be
/// followable with `direction=forward` (and vice versa), since the sliding
/// window re-reads trimmed regions by flipping direction against an edge
/// cursor. The old I1 rejection (`decoded.direction != direction` → 400) is
/// gone; this replaces `timeline_cursor_direction_mismatch_is_400`.
///
/// Fixture: 2 partitions, 20 records each — p0 offset o @ ts=1000+20o, p1
/// offset o @ ts=1010+20o (all timestamps distinct, so back/forward merge
/// order is fully determined: descending ts strictly alternates p1,p0 for
/// equal offsets). A back page anchored at ts_ms=1200 (resolves to offset
/// 10 in both partitions) with `limit=6` takes, newest-first: p1:9(1190),
/// p0:9(1180), p1:8(1170), p0:8(1160), p1:7(1150), p0:7(1140) — so
/// M = {(0,7),(0,8),(0,9),(1,7),(1,8),(1,9)}, continuation cursor positions
/// = [(0,7),(1,7)] (the lowest offset taken per partition), NOT exhausted
/// (offsets 0..6 remain below). Following that continuation cursor with
/// `direction=forward&limit=6` must be accepted (not 400) and must read
/// forward from those same positions, oldest-first: p0:7(1140), p1:7(1150),
/// p0:8(1160), p1:8(1170), p0:9(1180), p1:9(1190) — exactly M again. This is
/// the bound-symmetry the design relies on: a Back page's ending position
/// (exclusive upper for continuing further back) is numerically identical
/// to a Forward request's starting inclusive lower bound for re-reading
/// that same span forward.
#[tokio::test]
async fn timeline_direction_flip_reads_back_pages_continuation_forward_and_gets_it_back() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-flip-topic", 2).await;
    let p0: Vec<(String, String, i64)> =
        (0..20i64).map(|o| (format!("p0k{o}"), format!("p0v{o}"), 1000 + 20 * o)).collect();
    let p1: Vec<(String, String, i64)> =
        (0..20i64).map(|o| (format!("p1k{o}"), format!("p1v{o}"), 1010 + 20 * o)).collect();
    produce_at_many(&bootstrap, "tl-flip-topic", 0, &p0).await;
    produce_at_many(&bootstrap, "tl-flip-topic", 1, &p1).await;
    let state = state_for(&bootstrap, vec![]);

    let offsets_of = |events: &[(String, serde_json::Value)]| -> std::collections::HashSet<(i64, i64)> {
        events.iter().filter(|(n, _)| n == "match")
            .map(|(_, m)| (m["partition"].as_i64().unwrap(), m["offset"].as_i64().unwrap()))
            .collect()
    };
    let expected_m: std::collections::HashSet<(i64, i64)> =
        [(0i64, 7i64), (0, 8), (0, 9), (1, 7), (1, 8), (1, 9)].into_iter().collect();

    let back = collect_sse(
        app(state.clone()),
        "/api/clusters/test/topics/tl-flip-topic/timeline?direction=back&limit=6&anchor=timestamp&ts_ms=1200",
        200,
    ).await;
    let m = offsets_of(&back);
    assert_eq!(m, expected_m, "back page: {m:?}");
    let (_, back_end) = back.iter().find(|(n, _)| n == "page_end").expect("page_end present").clone();
    assert_eq!(back_end["exhausted"], false, "20 records exist below the anchor, only 6 taken: {back_end}");
    let continuation = back_end["cursor"].as_str().expect("non-exhausted page must carry a continuation cursor").to_string();
    let decoded = betrachtung::message::timeline::Cursor::decode(&continuation).unwrap();
    assert_eq!(decoded.positions, vec![(0, 7), (1, 7)], "continuation must sit at the lowest offset taken per partition");

    // The decoded blob is still tagged `direction: Back` (minted by a back
    // page) — following it with `direction=forward` must be honored, not
    // rejected, per v1.6's direction-belongs-to-the-request ruling.
    assert_eq!(decoded.direction, betrachtung::message::timeline::Direction::Back);
    let forward = collect_sse(
        app(state),
        &format!("/api/clusters/test/topics/tl-flip-topic/timeline?direction=forward&limit=6&cursor={}", urlencoding::encode(&continuation)),
        200,
    ).await;
    assert!(forward.iter().all(|(n, _)| n != "app_error"), "direction flip must not error: {forward:?}");
    let f = offsets_of(&forward);
    assert_eq!(f, expected_m, "forward-from-the-back-page's-own-continuation-cursor must read back exactly M: {f:?}");
}

/// Anchor partition property (spec v1.6, binding acceptance test): for
/// anchor=timestamp, `back(anchor)` and `forward(anchor)` split the topic
/// disjointly and completely per partition — reading upward from a jump's
/// anchor is gap-free and overlap-free against the downward read. Same
/// fixture and anchor as the direction-flip test above (ts_ms=1200 resolves
/// to offset 10 in both partitions), but here each direction is its own
/// independent anchor request (no cursor involved) with a generous limit/
/// budget so each single page reaches its true watermark edge.
#[tokio::test]
async fn timeline_anchor_timestamp_back_and_forward_split_disjointly_and_completely() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-anchor-split-topic", 2).await;
    let p0: Vec<(String, String, i64)> =
        (0..20i64).map(|o| (format!("p0k{o}"), format!("p0v{o}"), 1000 + 20 * o)).collect();
    let p1: Vec<(String, String, i64)> =
        (0..20i64).map(|o| (format!("p1k{o}"), format!("p1v{o}"), 1010 + 20 * o)).collect();
    produce_at_many(&bootstrap, "tl-anchor-split-topic", 0, &p0).await;
    produce_at_many(&bootstrap, "tl-anchor-split-topic", 1, &p1).await;
    let state = state_for(&bootstrap, vec![]);

    let offsets_of = |events: &[(String, serde_json::Value)]| -> std::collections::HashSet<(i64, i64)> {
        events.iter().filter(|(n, _)| n == "match")
            .map(|(_, m)| (m["partition"].as_i64().unwrap(), m["offset"].as_i64().unwrap()))
            .collect()
    };
    let older_half: std::collections::HashSet<(i64, i64)> =
        (0..2i64).flat_map(|p| (0..10i64).map(move |o| (p, o))).collect();
    let newer_half: std::collections::HashSet<(i64, i64)> =
        (0..2i64).flat_map(|p| (10..20i64).map(move |o| (p, o))).collect();

    let back = collect_sse(
        app(state.clone()),
        "/api/clusters/test/topics/tl-anchor-split-topic/timeline?direction=back&limit=500&anchor=timestamp&ts_ms=1200",
        200,
    ).await;
    let (_, back_end) = back.iter().find(|(n, _)| n == "page_end").expect("page_end present").clone();
    assert_eq!(back_end["exhausted"], true, "back: {back_end}");
    let m = offsets_of(&back);
    assert_eq!(m, older_half, "back(anchor) must cover exactly the records below the anchor: {m:?}");

    let forward = collect_sse(
        app(state),
        "/api/clusters/test/topics/tl-anchor-split-topic/timeline?direction=forward&limit=500&anchor=timestamp&ts_ms=1200",
        200,
    ).await;
    let (_, forward_end) = forward.iter().find(|(n, _)| n == "page_end").expect("page_end present").clone();
    assert_eq!(forward_end["exhausted"], true, "forward: {forward_end}");
    let f = offsets_of(&forward);
    assert_eq!(f, newer_half, "forward(anchor) must cover exactly the records at/above the anchor: {f:?}");

    assert!(f.is_disjoint(&m), "back(anchor) and forward(anchor) must not overlap: back={m:?} forward={f:?}");
    let mut union = m.clone();
    union.extend(&f);
    let full: std::collections::HashSet<(i64, i64)> = older_half.union(&newer_half).copied().collect();
    assert_eq!(union, full, "back(anchor) ∪ forward(anchor) must cover every fixture record exactly once: {union:?}");
}

/// Owner ruling 2026-08-15: a forward offset-anchor jump aligns EVERY
/// partition at the anchored message's own timestamp instead of pinning
/// everyone else at their high watermark (`Anchor::Offset`'s old,
/// deliberately-simpler behavior, which the owner flagged as reading
/// broken). Same fixture as the timestamp-anchor split test above (ts_ms=
/// 1200 resolves to offset 10 in both partitions) — anchoring at partition
/// 0 offset 10 (mid-topic, ts=1200) directly, forward, must land EXACTLY
/// the same set of matches as `anchor=timestamp&ts_ms=1200&direction=
/// forward` does: the anchored partition's target offset as its own oldest
/// row, every other partition aligned at-or-after that instant, and
/// nothing lost relative to the timestamp-anchor equivalent.
#[tokio::test]
async fn timeline_offset_anchor_forward_aligns_all_partitions_at_the_targets_timestamp() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-offset-forward-align-topic", 2).await;
    let p0: Vec<(String, String, i64)> =
        (0..20i64).map(|o| (format!("p0k{o}"), format!("p0v{o}"), 1000 + 20 * o)).collect();
    let p1: Vec<(String, String, i64)> =
        (0..20i64).map(|o| (format!("p1k{o}"), format!("p1v{o}"), 1010 + 20 * o)).collect();
    produce_at_many(&bootstrap, "tl-offset-forward-align-topic", 0, &p0).await;
    produce_at_many(&bootstrap, "tl-offset-forward-align-topic", 1, &p1).await;
    let state = state_for(&bootstrap, vec![]);

    let offsets_of = |events: &[(String, serde_json::Value)]| -> std::collections::HashSet<(i64, i64)> {
        events.iter().filter(|(n, _)| n == "match")
            .map(|(_, m)| (m["partition"].as_i64().unwrap(), m["offset"].as_i64().unwrap()))
            .collect()
    };
    // Same "newer half" the timestamp-anchor split test proves forward(ts=
    // 1200) covers exactly: offsets 10..19 in both partitions.
    let expected: std::collections::HashSet<(i64, i64)> =
        (0..2i64).flat_map(|p| (10..20i64).map(move |o| (p, o))).collect();

    let events = collect_sse(
        app(state),
        "/api/clusters/test/topics/tl-offset-forward-align-topic/timeline?direction=forward&limit=500&anchor=offset&partition=0&offset=10",
        200,
    ).await;
    assert!(events.iter().all(|(n, _)| n != "app_error"), "offset-anchor forward must not error: {events:?}");
    let (_, end) = events.iter().find(|(n, _)| n == "page_end").expect("page_end present").clone();
    assert_eq!(end["exhausted"], true, "{end}");
    let m = offsets_of(&events);
    assert_eq!(m, expected, "must align exactly like a timestamp anchor at the target's own ts (nothing lost, nothing extra): {m:?}");

    // The anchored partition's own target is its OLDEST row: no partition-0
    // offset below 10 appears anywhere in the page.
    let p0_offsets: Vec<i64> = m.iter().filter(|&&(p, _)| p == 0).map(|&(_, o)| o).collect();
    assert_eq!(*p0_offsets.iter().min().unwrap(), 10, "partition 0's target offset must be the oldest row for its own partition");
}

/// Review fix (M1+M2, 2026-08-15): a bad forward offset anchor (no message
/// at the exact target) used to 400 BEFORE the SSE stream opened —
/// `EventSource` discards a non-200 body wholesale, so the frontend showed
/// generic "connection lost" instead of the real, honest reason. It must
/// now arrive as an IN-STREAM `error` event instead: status 200, a real SSE
/// response, whose terminal event is `error` (not `page_end`), carrying the
/// same ApiError envelope (`code`/`message`/`cluster`/`retriable`) any
/// mid-scan failure already uses. This case targets a genuine compaction-
/// style hole: `produce_transactional` leaves a real, unaddressable control
/// record at offset `count` (the commit marker) — anchoring exactly there
/// has no message to resolve a timestamp from.
#[tokio::test]
async fn timeline_offset_anchor_forward_at_a_hole_reports_an_in_stream_error_not_a_400() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-offset-forward-hole-topic", 1).await;
    produce_transactional(&bootstrap, "tl-offset-forward-hole-topic", 5).await;
    let state = state_for(&bootstrap, vec![]);

    // `collect_sse` itself asserts status 200 — a real SSE response, never
    // a pre-stream error status (see its own doc comment/assertion).
    let events = collect_sse(
        app(state),
        "/api/clusters/test/topics/tl-offset-forward-hole-topic/timeline?direction=forward&limit=100&anchor=offset&partition=0&offset=5",
        20,
    ).await;
    assert!(events.iter().all(|(n, _)| n != "match"), "no message exists at the hole: {events:?}");
    assert!(!events.iter().any(|(n, _)| n == "page_end"), "must end in error, not page_end: {events:?}");
    let (_, err) = events.iter().find(|(n, _)| n == "app_error").expect("an in-stream error event: {events:?}").clone();
    assert_eq!(err["code"], "bad_request", "{err}");
    assert!(
        err["message"].as_str().unwrap().contains("partition 0 offset 5"),
        "message must honestly name the exact target: {err}",
    );
    assert_eq!(err["retriable"], false, "{err}");
}

/// Review fix (M1+M2, continued): the "unexercised past-watermark path" —
/// an offset anchor pointing entirely past the topic's current tail (not a
/// mid-topic hole). Confirms `fetch_one_record_blocking` doesn't quietly
/// resolve to some OTHER real record via `auto.offset.reset` (not set
/// anywhere in this codebase — its default only ever applies to
/// subscribe()-based consumption, never to `assign()`'s explicit offsets,
/// but this pins the behavior empirically rather than by inference): must
/// report the SAME honest in-stream error, never a `match` for a record the
/// user never asked for.
#[tokio::test]
async fn timeline_offset_anchor_forward_past_the_high_watermark_reports_an_in_stream_error_not_a_wrong_record() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-offset-forward-past-tail-topic", 1).await;
    produce(&bootstrap, "tl-offset-forward-past-tail-topic", 5).await; // offsets 0..4, high watermark 5
    let state = state_for(&bootstrap, vec![]);

    let events = collect_sse(
        app(state),
        "/api/clusters/test/topics/tl-offset-forward-past-tail-topic/timeline?direction=forward&limit=100&anchor=offset&partition=0&offset=5000",
        20,
    ).await;
    assert!(events.iter().all(|(n, _)| n != "match"), "no wrong record may be silently returned: {events:?}");
    let (_, err) = events.iter().find(|(n, _)| n == "app_error").expect("an in-stream error event: {events:?}").clone();
    assert_eq!(err["code"], "bad_request", "{err}");
    assert!(
        err["message"].as_str().unwrap().contains("partition 0 offset 5000"),
        "message must honestly name the exact target: {err}",
    );
}

/// Documents and locks down the cursor's wire-format contract (spec v1.6:
/// "a documented, client-constructible format"): base64 of the compact JSON
/// `{"direction":"back"|"forward","positions":[[partition,offset],...]}`.
/// This builds that JSON *by hand* — the way an external client would, with
/// no access to the Rust `Cursor` type — and asserts the backend accepts
/// and honors it. Single-partition topic, 10 records (offsets 0..9, values
/// "v0".."v9"): a hand-built forward cursor at position 5 must yield offsets
/// 5..9 and reach the high watermark.
#[tokio::test]
async fn timeline_accepts_a_client_constructed_cursor() {
    use base64::Engine as _;

    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-client-cursor-topic", 1).await;
    produce(&bootstrap, "tl-client-cursor-topic", 10).await;
    let state = state_for(&bootstrap, vec![]);

    let client_json = serde_json::json!({ "direction": "forward", "positions": [[0, 5]] });
    let client_cursor = base64::engine::general_purpose::STANDARD.encode(client_json.to_string());

    let events = collect_sse(
        app(state),
        &format!("/api/clusters/test/topics/tl-client-cursor-topic/timeline?direction=forward&limit=10&cursor={}", urlencoding::encode(&client_cursor)),
        200,
    ).await;
    assert!(events.iter().all(|(n, _)| n != "app_error"), "hand-built cursor must be accepted: {events:?}");
    let values: Vec<String> = events.iter().filter(|(n, _)| n == "match")
        .map(|(_, m)| m["value"]["text"].as_str().unwrap().to_string()).collect();
    assert_eq!(values, vec!["v5", "v6", "v7", "v8", "v9"], "must honor the hand-built position exactly: {values:?}");
    let (_, end) = events.iter().find(|(n, _)| n == "page_end").expect("page_end present").clone();
    assert_eq!(end["exhausted"], true, "{end}");
}

/// M1 fix (review finding): `Cursor.direction` is documented as
/// "informational only" but was a serde-required field with no default —
/// a client following that doc and omitting `direction` entirely would get
/// a 400 on a perfectly valid cursor. Now `#[serde(default)]`'d: a
/// hand-built cursor carrying ONLY `positions` (no `direction` key at all)
/// must decode and work end-to-end, identically to
/// `timeline_accepts_a_client_constructed_cursor` above.
#[tokio::test]
async fn timeline_accepts_a_client_cursor_without_direction_field() {
    use base64::Engine as _;

    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-client-cursor-no-direction-topic", 1).await;
    produce(&bootstrap, "tl-client-cursor-no-direction-topic", 10).await;
    let state = state_for(&bootstrap, vec![]);

    // No "direction" key at all — just the payload that actually matters.
    let client_json = serde_json::json!({ "positions": [[0, 5]] });
    let client_cursor = base64::engine::general_purpose::STANDARD.encode(client_json.to_string());

    let events = collect_sse(
        app(state),
        &format!("/api/clusters/test/topics/tl-client-cursor-no-direction-topic/timeline?direction=forward&limit=10&cursor={}", urlencoding::encode(&client_cursor)),
        200,
    ).await;
    assert!(events.iter().all(|(n, _)| n != "app_error"), "cursor without a direction field must be accepted: {events:?}");
    let values: Vec<String> = events.iter().filter(|(n, _)| n == "match")
        .map(|(_, m)| m["value"]["text"].as_str().unwrap().to_string()).collect();
    assert_eq!(values, vec!["v5", "v6", "v7", "v8", "v9"], "must honor the position exactly even without direction: {values:?}");
    let (_, end) = events.iter().find(|(n, _)| n == "page_end").expect("page_end present").clone();
    assert_eq!(end["exhausted"], true, "{end}");
}

/// Fix round 1, M2: `limit=0` is nonsensical (an empty page forever) and
/// must 400, not silently return zero records forever.
#[tokio::test]
async fn timeline_limit_zero_is_400() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/test/topics/x/timeline?direction=back&limit=0&anchor=latest").await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(body["code"], "bad_request");
}

/// Params must be validated before the cluster registry lookup, so a bad
/// request against an *unknown* cluster is still 400 (not a 404 that masks
/// the real problem): a client typo'ing both the cluster name and a param
/// should see the param error, not a 404 that looks like the cluster itself
/// is the problem.
#[tokio::test]
async fn timeline_bad_params_on_unknown_cluster_is_400_not_404() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(
        app(state),
        "/api/clusters/ghost/topics/x/timeline?direction=sideways&limit=10&anchor=latest",
    ).await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(body["code"], "bad_request");
}

/// Fix round 3, N4 + N6 (reviewer's exact reproduction): a real Kafka
/// transactional producer commits `count` records to a single-partition
/// topic. The commit leaves a legitimate offset hole (the transaction's own
/// control record, never delivered by `poll()`) at the offset immediately
/// below the high watermark — a perfectly healthy topic, zero broker
/// slowness, zero compaction.
///
/// Before this fix, `back`/`latest` on this topic terminated in a
/// `kafka_error` with zero rows (round 2's N3 short-read guard mistook the
/// hole at the position-adjacent offset for a short read on *every*
/// partition, tripping the "no progress" guard), and `forward`/`beginning`
/// returned all the records but with `exhausted: false` (round 2's
/// count-based cursor math, N6, landed 1 offset short of the true low/high
/// watermark because it subtracted a record *count* across a range that
/// actually spanned one more offset than that — the hole).
///
/// N4 (fetch completeness) and N6 (offset-exact cursor math) fixed together:
/// a fully-scanned (complete) partition's holes are trusted as legitimate,
/// and the cursor advances to the exact min/max *offset* actually taken,
/// not a record count — so a single page correctly reaches `exhausted:
/// true` with no error, in both directions.
#[tokio::test]
async fn timeline_transactional_commit_hole_is_not_a_short_read() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-txn-topic", 1).await;
    produce_transactional(&bootstrap, "tl-txn-topic", 4).await;
    let state = state_for(&bootstrap, vec![]);

    let back = collect_sse(app(state.clone()), "/api/clusters/test/topics/tl-txn-topic/timeline?direction=back&limit=10&anchor=latest", 200).await;
    assert!(back.iter().all(|(n, _)| n != "app_error"), "back must not error on a legitimate transaction-commit hole: {back:?}");
    let back_matches: Vec<_> = back.iter().filter(|(n, _)| n == "match").collect();
    assert_eq!(back_matches.len(), 4, "back: {back:?}");
    let (_, back_end) = back.iter().find(|(n, _)| n == "page_end").expect("page_end present").clone();
    assert_eq!(back_end["exhausted"], true, "back: {back_end}");
    assert!(back_end["cursor"].is_null());

    let forward = collect_sse(app(state), "/api/clusters/test/topics/tl-txn-topic/timeline?direction=forward&limit=10&anchor=beginning", 200).await;
    assert!(forward.iter().all(|(n, _)| n != "app_error"), "forward must not error on a legitimate transaction-commit hole: {forward:?}");
    let forward_matches: Vec<_> = forward.iter().filter(|(n, _)| n == "match").collect();
    assert_eq!(forward_matches.len(), 4, "forward: {forward:?}");
    let (_, forward_end) = forward.iter().find(|(n, _)| n == "page_end").expect("page_end present").clone();
    assert_eq!(forward_end["exhausted"], true, "forward: {forward_end}");
    assert!(forward_end["cursor"].is_null());
}

/// Fix round 3, N5: a cursor decoded to positions `[(0, 0), (99, 5)]`
/// against a 1-partition topic (no watermark entry for partition 99) must
/// behave as if positions were just `[(0, 0)]` — the unknown partition is
/// dropped, not carried through forever with nothing to clamp or exhaust it
/// against (which, before this fix, could keep the page's overall
/// `exhausted` stuck at `false` even once the real partition finished).
#[tokio::test]
async fn timeline_cursor_with_unknown_partition_terminates_properly() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-unknown-partition-topic", 1).await;
    produce(&bootstrap, "tl-unknown-partition-topic", 3).await;
    let state = state_for(&bootstrap, vec![]);

    let cursor = betrachtung::message::timeline::Cursor {
        direction: betrachtung::message::timeline::Direction::Forward,
        positions: vec![(0, 0), (99, 5)],
    }.encode();
    let events = collect_sse(
        app(state),
        &format!("/api/clusters/test/topics/tl-unknown-partition-topic/timeline?direction=forward&limit=10&cursor={}", urlencoding::encode(&cursor)),
        200,
    ).await;
    assert!(events.iter().all(|(n, _)| n != "app_error"), "unknown partition in cursor must not error: {events:?}");
    let matches: Vec<_> = events.iter().filter(|(n, _)| n == "match").collect();
    assert_eq!(matches.len(), 3, "events: {events:?}");
    let (_, end) = events.iter().find(|(n, _)| n == "page_end").expect("page_end present").clone();
    assert_eq!(end["exhausted"], true, "must terminate properly, not get stuck on the phantom partition: {end}");
    assert!(end["cursor"].is_null());
}

/// Filter params are validated the same way direction/limit/cursor already
/// are: 400 before any Kafka round trip (even against a topic/cluster that
/// doesn't exist) — an unknown filter kind, a missing `q`, or `json_eq`
/// missing its `path`.
#[tokio::test]
async fn timeline_bad_filter_params_are_400_before_streaming() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);

    let (status, body) = get_json(
        app(state.clone()),
        "/api/clusters/test/topics/x/timeline?direction=back&limit=10&anchor=latest&filter=sideways&q=x",
    ).await;
    assert_eq!(status, 400, "unknown filter kind: {body}");
    assert_eq!(body["code"], "bad_request");

    let (status, body) = get_json(
        app(state.clone()),
        "/api/clusters/test/topics/x/timeline?direction=back&limit=10&anchor=latest&filter=contains",
    ).await;
    assert_eq!(status, 400, "missing q: {body}");
    assert_eq!(body["code"], "bad_request");

    let (status, body) = get_json(
        app(state),
        "/api/clusters/test/topics/x/timeline?direction=back&limit=10&anchor=latest&filter=json_eq&q=42",
    ).await;
    assert_eq!(status, 400, "json_eq missing path: {body}");
    assert_eq!(body["code"], "bad_request");
}

#[tokio::test]
async fn timeline_filter_scans_until_limit_with_progress() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-filter-topic", 1).await;
    let records: Vec<(String, String, i64)> = (0..200i64)
        .map(|i| {
            let v = if i % 20 == 0 { format!("special-{i}") } else { format!("noise-{i}") };
            (format!("k{i}"), v, 3000 + i)
        })
        .collect();
    produce_at_many(&bootstrap, "tl-filter-topic", 0, &records).await;
    let state = state_for(&bootstrap, vec![]);
    let events = collect_sse(app(state), "/api/clusters/test/topics/tl-filter-topic/timeline?direction=back&limit=5&anchor=latest&filter=contains&q=special", 300).await;
    let m: Vec<_> = events.iter().filter(|(n, _)| n == "match").map(|(_, v)| v["value"]["text"].as_str().unwrap().to_string()).collect();
    assert_eq!(m.len(), 5);
    assert!(m.iter().all(|t| t.starts_with("special-")));
    assert!(events.iter().any(|(n, _)| n == "progress"), "expected progress events");
    let (_, end) = events.iter().find(|(n, _)| n == "page_end").unwrap().clone();
    assert_eq!(end["exhausted"], false);
    assert!(end["cursor"].is_string());
}

#[tokio::test]
async fn timeline_filter_budget_exhaustion_reports_and_continues() {
    use betrachtung::config::Limits;
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-budget-topic", 1).await;
    let records: Vec<(String, String, i64)> = (0..300i64)
        .map(|i| {
            let v = if i == 10 { "needle".to_string() } else { format!("hay-{i}") };
            (format!("k{i}"), v, 4000 + i)
        })
        .collect();
    produce_at_many(&bootstrap, "tl-budget-topic", 0, &records).await;
    let mut state = state_for(&bootstrap, vec![]);
    state.limits = std::sync::Arc::new(Limits { timeline_scan_budget: 100, ..(*state.limits).clone() });
    // back from latest with budget 100: scans offsets 200..300, finds nothing, stops un-exhausted
    let events = collect_sse(app(state.clone()), "/api/clusters/test/topics/tl-budget-topic/timeline?direction=back&limit=5&anchor=latest&filter=contains&q=needle", 300).await;
    let m = events.iter().filter(|(n, _)| n == "match").count();
    assert_eq!(m, 0);
    let (_, end) = events.iter().find(|(n, _)| n == "page_end").unwrap().clone();
    assert_eq!(end["exhausted"], false, "budget stop is not edge: {end}");
    let c1 = end["cursor"].as_str().unwrap().to_string();
    // two more budgeted continues reach the needle at offset 10
    let events2 = collect_sse(app(state.clone()), &format!("/api/clusters/test/topics/tl-budget-topic/timeline?direction=back&limit=5&cursor={}&filter=contains&q=needle", urlencoding::encode(&c1)), 300).await;
    let c2 = events2.iter().find(|(n, _)| n == "page_end").unwrap().1["cursor"].as_str().unwrap().to_string();
    let events3 = collect_sse(app(state), &format!("/api/clusters/test/topics/tl-budget-topic/timeline?direction=back&limit=5&cursor={}&filter=contains&q=needle", urlencoding::encode(&c2)), 300).await;
    let found: Vec<_> = events3.iter().filter(|(n, _)| n == "match").map(|(_, v)| v["value"]["text"].as_str().unwrap().to_string()).collect();
    assert_eq!(found, vec!["needle"]);
}

/// Code review fix: with multiple partitions, `per_partition_share`'s
/// `.max(1)` floor (once `remaining_budget < active_partitions`) let a
/// chunk's *potential* charge (`span * windows.len()`) exceed the actual
/// remaining budget by up to `active_partitions - 1` records — the budget
/// is a hard per-request cap, not a rough target, so a page must never
/// report having scanned more than it was allowed to. Three partitions,
/// plenty of real data in each (so none reach their own edge for many
/// chunks), and a tiny budget of 4 force exactly the failure mode: chunk 0
/// legitimately spends 3 (1 offset per partition), leaving 1 — but chunk
/// 1's naive per-partition floor of 1 again times 3 partitions would spend
/// 3 more, landing on 6 total against a budget of 4.
#[tokio::test]
async fn timeline_budget_never_overshoots_with_multiple_partitions() {
    use betrachtung::config::Limits;
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-budget-multi-topic", 3).await;
    for p in 0..3i32 {
        let records: Vec<(String, String, i64)> = (0..20i64)
            .map(|i| (format!("k{p}-{i}"), format!("v{p}-{i}"), 1000 + i))
            .collect();
        produce_at_many(&bootstrap, "tl-budget-multi-topic", p, &records).await;
    }
    let mut state = state_for(&bootstrap, vec![]);
    state.limits = std::sync::Arc::new(Limits { timeline_scan_budget: 4, ..(*state.limits).clone() });

    let events = collect_sse(app(state), "/api/clusters/test/topics/tl-budget-multi-topic/timeline?direction=back&limit=100&anchor=latest", 300).await;
    let max_scanned = events.iter()
        .filter(|(n, _)| n == "progress")
        .map(|(_, v)| v["scanned"].as_u64().unwrap())
        .max()
        .unwrap_or(0);
    assert!(max_scanned <= 4, "budget must never be overshot: max reported scanned = {max_scanned}, events: {events:?}");
    let (_, end) = events.iter().find(|(n, _)| n == "page_end").unwrap().clone();
    assert_eq!(end["exhausted"], false, "3x20 records over a budget of 4 cannot be exhausted: {end}");
}

/// Empty-page contract amendment: the scan budget bounds unfiltered hole
/// traversal too, so a page whose first window is entirely a legitimate
/// offset hole must keep chunk-scanning *within the same request* rather
/// than handing back an empty, non-exhausted page. A single committed
/// transaction with exactly one real message leaves exactly this shape: the
/// topic's very last offset is the transaction's own control record (never
/// delivered by `poll()`), with the one real message sitting immediately
/// below it. `back`/`latest`/`limit=1` makes the very *first* chunk (span
/// 1, an unfiltered page's chunk-0 span always equals `limit`) land
/// exactly on that trailing hole — zero matches, but not exhausted, since
/// the real message is still there — so the only way this test can pass is
/// if `run_page` keeps scanning into a second, larger chunk within this
/// same request instead of returning the hole as an empty page.
#[tokio::test]
async fn timeline_unfiltered_page_crosses_a_hole_in_one_request() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "tl-hole-topic", 1).await;
    produce_transactional(&bootstrap, "tl-hole-topic", 1).await;
    let state = state_for(&bootstrap, vec![]);

    let events = collect_sse(app(state), "/api/clusters/test/topics/tl-hole-topic/timeline?direction=back&limit=1&anchor=latest", 200).await;
    assert!(events.iter().all(|(n, _)| n != "app_error"), "must not error crossing a legitimate hole region: {events:?}");
    let m: Vec<_> = events.iter().filter(|(n, _)| n == "match").map(|(_, v)| v["value"]["text"].as_str().unwrap().to_string()).collect();
    assert_eq!(m, vec!["v0"], "a single request must cross the trailing hole and return the real record beyond it: {events:?}");
    let (_, end) = events.iter().find(|(n, _)| n == "page_end").unwrap().clone();
    assert_eq!(end["exhausted"], true, "the one real record is everything in the topic: {end}");
    assert!(end["cursor"].is_null());
}

/// Final-review C1 (MUST-FIX, pairs with the ledger's own deferred item): a
/// client that disconnects mid-scan must actually stop the scan, not leave a
/// detached task spinning. Before the fix, `run_page`'s outer chunk loop
/// never checked its own `cancelled` flag and swallowed `Progress` send
/// errors — once the receiver dropped (client gone), the loop kept iterating
/// forever, minting a fresh `BaseConsumer` every pass (`fetch_call_count`
/// growing without bound) because a cancelled fetch always returns
/// zero-progress, so budget/edge never advance either.
///
/// Two partitions on purpose: partition 1 is small enough to hit its own low
/// watermark within the first chunk, freeing up leftover scan budget for a
/// genuine second chunk on partition 0 — that's what guarantees the scan
/// still has real work pending (not yet exhausted, budget not yet spent)
/// at the moment the client is dropped, so only the missing cancellation
/// check stands between "stop" and "spin forever".
#[tokio::test]
async fn timeline_scan_stops_on_client_disconnect() {
    use betrachtung::config::Limits;
    use betrachtung::message::fetch::fetch_call_count;
    use futures_util::StreamExt;

    let bootstrap = start_kafka().await;
    let topic = "tl-disconnect-topic";
    create_topic(&bootstrap, topic, 2).await;
    let big: Vec<(String, String, i64)> =
        (0..60i64).map(|i| (format!("k{i}"), format!("noise-{i}"), 6000 + i)).collect();
    produce_at_many(&bootstrap, topic, 0, &big).await;
    let small: Vec<(String, String, i64)> =
        (0..5i64).map(|i| (format!("k{i}"), format!("noise-{i}"), 6000 + i)).collect();
    produce_at_many(&bootstrap, topic, 1, &small).await;

    let mut state = state_for(&bootstrap, vec![]);
    // Small enough that chunk 0 leaves genuine slack (some budget unspent,
    // not exhausted) once partition 1 hits its edge — see doc comment above.
    state.limits = std::sync::Arc::new(Limits { timeline_scan_budget: 30, ..(*state.limits).clone() });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });

    let before = fetch_call_count(topic);
    let res = reqwest::get(format!(
        "http://{addr}/api/clusters/test/topics/{topic}/timeline?direction=back&limit=1000000&anchor=latest&filter=contains&q=never-matches-anything"
    ))
    .await
    .unwrap();
    assert_eq!(res.status(), 200);

    // Read exactly one SSE frame so the scan has genuinely started (at least
    // one real fetch happened) before pulling the rug — a real "mid-scan"
    // disconnect, not a connection that never got used.
    let mut stream = res.bytes_stream();
    let mut buf = String::new();
    while !buf.contains("\n\n") {
        let chunk = stream.next().await.expect("expected at least one SSE frame before disconnect").unwrap();
        buf.push_str(&String::from_utf8_lossy(&chunk));
    }
    drop(stream); // client disconnects mid-scan

    // Settle, then sample the fetch-call counter twice a beat apart: a
    // zombie scan keeps minting fresh consumers every iteration (fast,
    // unbounded growth); a properly cancelled one goes flat immediately.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let mid = fetch_call_count(topic);
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let after = fetch_call_count(topic);

    assert!(mid > before, "sanity: the scan must have made at least one real fetch call: before={before} mid={mid}");
    assert_eq!(
        after, mid,
        "scan must stop creating fetch consumers after client disconnect (zombie scan): before={before} mid={mid} after={after}"
    );
}

/// The messages-timeline design supersedes both `/messages` (browse) and
/// `/search` — "both DELETED — one code path wins" (the timeline). Neither
/// route exists in the router any more, so a request to either now falls
/// through to the SPA fallback like any other unmatched path: 200,
/// text/html, not a JSON 404 from an API handler.
#[tokio::test]
async fn old_endpoints_are_gone() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);

    let res = app(state.clone())
        .oneshot(axum::http::Request::builder()
            .uri("/api/clusters/test/topics/x/messages?anchor=latest")
            .body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    assert!(ct.starts_with("text/html"), "/messages must be gone (SPA fallback), got content-type {ct}");

    let res = app(state)
        .oneshot(axum::http::Request::builder()
            .uri("/api/clusters/test/topics/x/search?range=last_n&n=10&filter=value_contains&q=x")
            .body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    assert!(ct.starts_with("text/html"), "/search must be gone (SPA fallback), got content-type {ct}");
}

#[tokio::test]
async fn health_self_heals_a_stale_client() {
    // support::start_kafka() returns the shared reused test broker's
    // bootstrap; support::cluster_cfg builds a ClusterConfig for it
    let bootstrap = start_kafka().await;
    let handle = Arc::new(ClusterHandle::connect(cluster_cfg("self-heal", &bootstrap)).unwrap());
    assert_eq!(handle.health().await.status, HealthStatus::Healthy, "sanity");

    // resident clients now point at a dead port; config still points at the
    // live broker — the stale-client wedge in miniature
    handle.replace_clients_with_bootstrap("127.0.0.1:1").unwrap();

    // first failure stays honest (below threshold)
    let first = handle.health().await;
    assert_eq!(first.status, HealthStatus::Unreachable);
    assert!(first.error.is_some());

    // second failure reaches RECOVERY_THRESHOLD: probe fresh client from
    // config, swap, and report Healthy in the same call
    let second = handle.health().await;
    assert_eq!(second.status, HealthStatus::Healthy, "self-heal must land in-call");
    assert!(second.broker_count.is_some());

    // healed for real: subsequent checks stay healthy
    assert_eq!(handle.health().await.status, HealthStatus::Healthy);
}

#[tokio::test]
async fn spa_fallback_serves_html_for_unknown_paths() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let res = app(state)
        .oneshot(axum::http::Request::builder().uri("/c/local/topics").body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap().to_string();
    assert!(ct.starts_with("text/html"), "got content-type {ct}");
}
