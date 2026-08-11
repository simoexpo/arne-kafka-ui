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
async fn topics_on_unknown_cluster_is_404() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/ghost/topics").await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], "cluster_not_found");
}
