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
