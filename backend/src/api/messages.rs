use crate::cluster::{ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use crate::message::fetch;
use crate::message::range::{self, PartitionRange};
use crate::state::AppState;
use crate::util::now_ms;
use axum::extract::{Path, Query, State};
use axum::Json;
use rdkafka::consumer::Consumer;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
pub struct BrowseParams {
    pub anchor: String,
    pub limit: Option<u64>,
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    pub ts_ms: Option<i64>,
}

pub fn watermarks_blocking(handle: &ClusterHandle, topic: &str) -> Result<Vec<(i32, i64, i64)>, ApiError> {
    let md = handle.consumer()
        .fetch_metadata(Some(topic), ADMIN_TIMEOUT)
        .map_err(|e| error::from_kafka(&handle.name, "fetch metadata", &e))?;
    let t = md.topics().iter()
        .find(|t| t.name() == topic && !t.partitions().is_empty())
        .ok_or_else(|| ApiError::topic_not_found(&handle.name, topic))?;
    let mut wm = Vec::new();
    for p in t.partitions() {
        let (lo, hi) = handle.consumer()
            .fetch_watermarks(topic, p.id(), ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "fetch watermarks", &e))?;
        wm.push((p.id(), lo, hi));
    }
    Ok(wm)
}

pub fn ts_starts_blocking(
    handle: &ClusterHandle,
    topic: &str,
    watermarks: &[(i32, i64, i64)],
    ts_ms: i64,
) -> Result<Vec<(i32, Option<i64>)>, ApiError> {
    use rdkafka::topic_partition_list::TopicPartitionList;
    use rdkafka::Offset;
    let mut tpl = TopicPartitionList::new();
    for &(p, _, _) in watermarks {
        tpl.add_partition_offset(topic, p, Offset::Offset(ts_ms))
            .map_err(|e| error::from_kafka(&handle.name, "offsets_for_times", &e))?;
    }
    let resolved = handle.consumer()
        .offsets_for_times(tpl, ADMIN_TIMEOUT)
        .map_err(|e| error::from_kafka(&handle.name, "offsets_for_times", &e))?;
    Ok(resolved.elements().iter().map(|e| {
        let start = match e.offset() {
            Offset::Offset(o) => Some(o),
            _ => None, // no message at/after ts in this partition
        };
        (e.partition(), start)
    }).collect())
}

/// Validated once, before any Kafka round-trip, so a malformed request
/// (bad anchor or missing param) is rejected with 400 even against a topic
/// that doesn't exist — param validation must outrank topic lookup.
enum Anchor {
    Latest,
    Offset { partition: i32, offset: i64 },
    Timestamp { ts_ms: i64 },
}

impl Anchor {
    fn parse(params: &BrowseParams) -> Result<Self, ApiError> {
        match params.anchor.as_str() {
            "latest" => Ok(Anchor::Latest),
            "offset" => Ok(Anchor::Offset {
                partition: params.partition
                    .ok_or_else(|| ApiError::bad_request("anchor=offset requires partition"))?,
                offset: params.offset
                    .ok_or_else(|| ApiError::bad_request("anchor=offset requires offset"))?,
            }),
            "timestamp" => Ok(Anchor::Timestamp {
                ts_ms: params.ts_ms
                    .ok_or_else(|| ApiError::bad_request("anchor=timestamp requires ts_ms"))?,
            }),
            other => Err(ApiError::bad_request(format!("unknown anchor '{other}'"))),
        }
    }
}

pub async fn browse(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
    Query(params): Query<BrowseParams>,
) -> Result<Json<Value>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    let limit = params.limit.unwrap_or(50).min(500);
    let anchor = Anchor::parse(&params)?;

    let ranges: Vec<PartitionRange> = {
        let handle = handle.clone();
        let topic = topic.clone();
        tokio::task::spawn_blocking(move || -> Result<_, ApiError> {
            let wm = watermarks_blocking(&handle, &topic)?;
            match anchor {
                Anchor::Latest => Ok(range::latest_ranges(&wm, limit)),
                Anchor::Offset { partition, offset } => {
                    Ok(range::offset_range(&wm, partition, offset, limit).into_iter().collect())
                }
                Anchor::Timestamp { ts_ms } => {
                    let starts = ts_starts_blocking(&handle, &topic, &wm, ts_ms)?;
                    let mut ranges = range::window_ranges(&wm, &starts, None);
                    for r in &mut ranges {
                        r.end = r.end.min(r.start + limit as i64);
                    }
                    Ok(ranges)
                }
            }
        }).await.map_err(|e| ApiError::internal(format!("task join: {e}")))??
    };

    let records = {
        let cfg = handle.config.clone();
        let topic = topic.clone();
        let ranges = ranges.clone();
        tokio::task::spawn_blocking(move || {
            fetch::fetch_ranges_blocking(&cfg, &topic, &ranges, (limit as usize) * ranges.len().max(1))
        }).await.map_err(|e| ApiError::internal(format!("task join: {e}")))??
    };

    let mut messages = fetch::to_message_out(records, handle.schema_registry.as_deref()).await;
    messages.sort_by(|a, b| b.timestamp_ms.cmp(&a.timestamp_ms).then(b.offset.cmp(&a.offset)));
    messages.truncate(limit as usize);
    Ok(Json(json!({ "messages": messages, "as_of": now_ms() })))
}
