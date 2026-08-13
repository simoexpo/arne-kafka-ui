mod support;

use axum::http::StatusCode;
use betrachtung::api::app;
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
async fn browse_latest_returns_newest_messages_decoded() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "browse-topic", 2).await;
    produce(&bootstrap, "browse-topic", 10).await; // keys k0..k9, values v0..v9
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/test/topics/browse-topic/messages?anchor=latest&limit=4").await;
    assert_eq!(status, 200, "body: {body}");
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 4);
    for m in messages {
        assert_eq!(m["value"]["encoding"], "utf8");
        assert!(m["value"]["text"].as_str().unwrap().starts_with('v'));
        assert!(m["offset"].as_i64().is_some());
        assert!(m["timestamp_ms"].as_i64().unwrap() > 0);
    }
    assert!(body["as_of"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn browse_by_offset_is_exact() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "browse-offset-topic", 1).await;
    produce(&bootstrap, "browse-offset-topic", 10).await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(
        app(state),
        "/api/clusters/test/topics/browse-offset-topic/messages?anchor=offset&partition=0&offset=3&limit=2",
    ).await;
    assert_eq!(status, 200, "body: {body}");
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    let offsets: Vec<i64> = messages.iter().map(|m| m["offset"].as_i64().unwrap()).collect();
    assert!(offsets.contains(&3) && offsets.contains(&4), "got offsets {offsets:?}");
}

#[tokio::test]
async fn browse_bad_anchor_is_400_and_unknown_topic_404() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state.clone()), "/api/clusters/test/topics/x/messages?anchor=sideways").await;
    assert_eq!(status, 400, "body: {body}");
    let (status, body) = get_json(app(state), "/api/clusters/test/topics/ghost-topic/messages?anchor=latest").await;
    assert_eq!(status, 404, "body: {body}");
    assert_eq!(body["code"], "topic_not_found");
}

#[tokio::test]
async fn search_streams_matches_progress_and_done() {
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "search-topic", 2).await;
    produce(&bootstrap, "search-topic", 30).await; // v0..v29
    let state = state_for(&bootstrap, vec![]);
    let events = collect_sse(
        app(state),
        "/api/clusters/test/topics/search-topic/search?range=last_n&n=30&filter=value_contains&q=v2",
        200,
    ).await;

    let matches: Vec<_> = events.iter().filter(|(n, _)| n == "match").collect();
    // v2, v20..v29 = 11 matches
    assert_eq!(matches.len(), 11, "events: {events:?}");
    for (_, m) in &matches {
        assert!(m["value"]["text"].as_str().unwrap().contains("v2"));
    }
    let (last_name, last) = events.last().unwrap();
    assert_eq!(last_name, "done");
    assert_eq!(last["reason"], "complete");
    let progress: Vec<_> = events.iter().filter(|(n, _)| n == "progress").collect();
    assert!(!progress.is_empty(), "expected progress events");
    let (_, p) = progress.last().unwrap();
    assert_eq!(p["total"], 30);
    assert_eq!(p["scanned"], 30);
    assert_eq!(p["matches"], 11);
}

#[tokio::test]
async fn search_stops_at_max_matches() {
    use betrachtung::config::Limits;
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "search-cap-topic", 1).await;
    produce(&bootstrap, "search-cap-topic", 20).await;
    let mut state = state_for(&bootstrap, vec![]);
    state.limits = std::sync::Arc::new(Limits { max_search_matches: 3, ..Limits::default() });
    let events = collect_sse(
        app(state),
        "/api/clusters/test/topics/search-cap-topic/search?range=last_n&n=20&filter=value_contains&q=v",
        200,
    ).await;
    let matches = events.iter().filter(|(n, _)| n == "match").count();
    assert_eq!(matches, 3, "events: {events:?}");
    assert_eq!(events.last().unwrap().1["reason"], "max_matches");
}

#[tokio::test]
async fn search_max_matches_with_deep_partitions() {
    use betrachtung::config::Limits;
    let bootstrap = start_kafka().await;
    create_topic(&bootstrap, "search-deep-topic", 2).await;
    produce(&bootstrap, "search-deep-topic", 200).await;
    let mut state = state_for(&bootstrap, vec![]);
    state.limits = std::sync::Arc::new(Limits { max_search_matches: 3, ..Limits::default() });
    // A worker that hits the match cap breaks out of its own recv loop while
    // its partition's scanner thread may still be parked mid-send; if that
    // scanner is never woken, the stream never reaches `done` and this hangs
    // forever. Bound it explicitly so a regression fails fast instead of
    // wedging the test run. (The exact lock-step race is captured
    // deterministically, independent of real Kafka timing, by
    // `message::search::tests::worker_giving_up_early_wakes_a_parked_scanner`
    // in src/message/search.rs; this test covers the same defect end to end.)
    let events = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        collect_sse(
            app(state),
            "/api/clusters/test/topics/search-deep-topic/search?range=last_n&n=200&filter=value_contains&q=v",
            500,
        ),
    )
    .await
    .expect("search must terminate with done{max_matches} within 10s instead of hanging");
    let matches = events.iter().filter(|(n, _)| n == "match").count();
    assert_eq!(matches, 3, "events: {events:?}");
    assert_eq!(events.last().unwrap().1["reason"], "max_matches");
}

#[tokio::test]
async fn search_bad_filter_is_error_event_or_400() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(
        app(state),
        "/api/clusters/test/topics/x/search?range=last_n&n=10&filter=sideways&q=x",
    ).await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(body["code"], "bad_request");
}

/// I5 regression: `range` must be validated before any Kafka round trip —
/// same as browse's `anchor` — so a malformed `range` on a topic that
/// doesn't even exist is a fast 400, not a 404 (and not a broker round
/// trip at all).
#[tokio::test]
async fn search_bad_range_on_unknown_topic_is_400_not_404() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(
        app(state),
        "/api/clusters/test/topics/ghost-topic/search?range=sideways&filter=value_contains&q=x",
    ).await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(body["code"], "bad_request");
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
/// the raw bytes as base64, in both browse (poll) and search (streaming)
/// results.
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

    let (status, body) = get_json(
        app(state.clone()),
        "/api/clusters/test/topics/confluent-no-sr-topic/messages?anchor=latest",
    ).await;
    assert_eq!(status, 200, "body: {body}");
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1, "messages: {messages:?}");
    let v = &messages[0]["value"];
    assert_eq!(v["encoding"], "decode_error", "browse must surface the decode failure, not skip it: {v:?}");
    assert!(v["error"].as_str().is_some_and(|e| !e.is_empty()), "expected a non-empty error: {v:?}");
    assert!(v["text"].as_str().is_some_and(|t| !t.is_empty()), "expected base64 raw bytes: {v:?}");

    // Filter on the key (which decodes fine as utf8) so the match doesn't
    // depend on the value's content — the point here is that the record
    // still surfaces in search results at all, with its value correctly
    // labeled as a decode error.
    let events = collect_sse(
        app(state),
        "/api/clusters/test/topics/confluent-no-sr-topic/search?range=last_n&n=10&filter=key_contains&q=confluent",
        50,
    ).await;
    let matches: Vec<_> = events.iter().filter(|(n, _)| n == "match").collect();
    assert_eq!(matches.len(), 1, "events: {events:?}");
    let (_, m) = matches[0];
    assert_eq!(m["value"]["encoding"], "decode_error", "search must surface the decode failure too: {m:?}");
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

/// Fix round 1, I1: a cursor's own `direction` must match the request's
/// `direction` param, or it's rejected with 400 before any streaming — a
/// stale or foreign cursor used with the wrong direction must not silently
/// paginate in a way its stored positions never meant.
#[tokio::test]
async fn timeline_cursor_direction_mismatch_is_400() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let back_cursor = betrachtung::message::timeline::Cursor {
        direction: betrachtung::message::timeline::Direction::Back,
        positions: vec![(0, 5)],
    }.encode();
    let (status, body) = get_json(
        app(state),
        &format!("/api/clusters/test/topics/x/timeline?direction=forward&limit=10&cursor={}", urlencoding::encode(&back_cursor)),
    ).await;
    assert_eq!(status, 400, "body: {body}");
    assert_eq!(body["code"], "bad_request");
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

/// Fix round 1, M3: params must be validated before the cluster registry
/// lookup, so a bad request against an *unknown* cluster is still 400 (not
/// a 404 that masks the real problem) — mirrors
/// `search_bad_range_on_unknown_topic_is_400_not_404`'s rationale, applied
/// to an unknown cluster instead of an unknown topic.
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
