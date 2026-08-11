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
async fn topics_on_unknown_cluster_is_404() {
    let bootstrap = start_kafka().await;
    let state = state_for(&bootstrap, vec![]);
    let (status, body) = get_json(app(state), "/api/clusters/ghost/topics").await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], "cluster_not_found");
}
