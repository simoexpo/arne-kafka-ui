mod support;

use betrachtung::api::app;
use support::*;

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
        std::time::Duration::from_secs(30),
        collect_sse(
            app(state),
            "/api/clusters/test/topics/search-deep-topic/search?range=last_n&n=200&filter=value_contains&q=v",
            500,
        ),
    )
    .await
    .expect("search must terminate with done{max_matches} within 30s instead of hanging");
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
