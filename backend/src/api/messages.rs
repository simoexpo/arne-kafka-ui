use crate::cluster::{ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use crate::message::fetch;
use crate::message::filter::Filter;
use crate::message::range::{self, PartitionRange};
use crate::message::search::{self, SearchEvent};
use crate::message::tail;
use crate::state::AppState;
use crate::util::now_ms;
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::Json;
use futures_util::stream::Stream;
use rdkafka::consumer::Consumer;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as _;

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

/// `latest_ranges` casts `n` to `i64` unchecked; capping here keeps that cast
/// from ever wrapping on an adversarial `n`.
const MAX_SEARCH_N: u64 = 1_000_000;

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub range: String,
    pub n: Option<u64>,
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub partition: Option<i32>,
    pub filter: String,
    pub q: String,
    pub path: Option<String>,
}

/// Validated once, before any Kafka round-trip — mirrors `Anchor::parse` for
/// browse — so a malformed `range` (or a missing required param) is rejected
/// with 400 even against a topic that doesn't exist; param validation must
/// outrank topic lookup and never costs a broker round trip.
enum Range {
    LastN { n: u64 },
    Offsets { from: i64, to: i64, partition: Option<i32> },
    Ts { from_ms: i64, to_ms: Option<i64> },
}

impl Range {
    fn parse(params: &SearchParams) -> Result<Self, ApiError> {
        match params.range.as_str() {
            "last_n" => Ok(Range::LastN {
                n: params.n.ok_or_else(|| ApiError::bad_request("range=last_n requires n"))?,
            }),
            "offsets" => Ok(Range::Offsets {
                from: params.from.ok_or_else(|| ApiError::bad_request("range=offsets requires from"))?,
                to: params.to.ok_or_else(|| ApiError::bad_request("range=offsets requires to"))?,
                partition: params.partition,
            }),
            "ts" => Ok(Range::Ts {
                from_ms: params.from_ms.ok_or_else(|| ApiError::bad_request("range=ts requires from_ms"))?,
                to_ms: params.to_ms,
            }),
            other => Err(ApiError::bad_request(format!("unknown range '{other}'"))),
        }
    }
}

/// Sets the cancel flag when the SSE stream is dropped (client disconnected),
/// stopping every partition scanner at its next poll iteration — no zombie
/// scans survive a client that goes away mid-search.
struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

pub async fn search(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
    Query(params): Query<SearchParams>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    let filter = Filter::parse(&params.filter, &params.q, params.path.as_deref())
        .map_err(ApiError::bad_request)?;

    if let Some(n) = params.n
        && n > MAX_SEARCH_N
    {
        return Err(ApiError::bad_request(format!("n must be <= {MAX_SEARCH_N}")));
    }

    let range = Range::parse(&params)?;

    let ranges = {
        let handle = handle.clone();
        let topic = topic.clone();
        tokio::task::spawn_blocking(move || -> Result<_, ApiError> {
            let wm = watermarks_blocking(&handle, &topic)?;
            match range {
                Range::LastN { n } => Ok(range::latest_ranges(&wm, n)),
                Range::Offsets { from, to, partition } => {
                    let parts: Vec<i32> = match partition {
                        Some(p) => vec![p],
                        None => wm.iter().map(|&(p, _, _)| p).collect(),
                    };
                    let starts: Vec<(i32, Option<i64>)> = parts.iter().map(|&p| (p, Some(from))).collect();
                    let ends: Vec<(i32, i64)> = parts.iter().map(|&p| (p, to)).collect();
                    Ok(range::window_ranges(&wm, &starts, Some(&ends)))
                }
                Range::Ts { from_ms, to_ms } => {
                    let starts = ts_starts_blocking(&handle, &topic, &wm, from_ms)?;
                    let ends = match to_ms {
                        Some(to_ms) => {
                            let end_starts = ts_starts_blocking(&handle, &topic, &wm, to_ms)?;
                            Some(end_starts.into_iter()
                                .map(|(p, o)| (p, o.unwrap_or_else(|| {
                                    wm.iter().find(|(wp, _, _)| *wp == p).map(|&(_, _, hi)| hi).unwrap_or(0)
                                })))
                                .collect::<Vec<_>>())
                        }
                        None => None,
                    };
                    Ok(range::window_ranges(&wm, &starts, ends.as_deref()))
                }
            }
        }).await.map_err(|e| ApiError::internal(format!("task join: {e}")))??
    };

    let max = u64::from(state.limits.max_search_matches);
    let (rx, cancel) = search::run(handle, topic, ranges, filter, max);
    let guard = CancelOnDrop(cancel);
    let stream = ReceiverStream::new(rx).map(move |event: SearchEvent| {
        let _hold = &guard; // move the guard into the stream: dropped on disconnect
        Ok(Event::default().event(event.name()).data(serde_json::to_string(&event).unwrap_or_default()))
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

pub async fn tail_sse(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    let (rx, cancel) = tail::run(handle, topic).await?;
    let guard = CancelOnDrop(cancel);
    let stream = ReceiverStream::new(rx).map(move |msg| {
        let _hold = &guard; // move the guard into the stream: dropped on disconnect
        Ok(Event::default().event("message").data(serde_json::to_string(&msg).unwrap_or_default()))
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
