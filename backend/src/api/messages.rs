use crate::cluster::{ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use crate::message::fetch;
use crate::message::filter::Filter;
use crate::message::range::{self, PartitionRange};
use crate::message::search::{self, SearchEvent};
use crate::message::tail;
use crate::message::timeline::{self, TimelineEvent};
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
            let wm = fetch::watermarks_blocking(&handle, &topic)?;
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

    // browse() doesn't need per-partition completeness (fix round 3's
    // `FetchOutcome::complete` — that distinction only matters to
    // `timeline::run_page`'s cursor math), just the records themselves.
    let records = {
        let cfg = handle.config.clone();
        let topic = topic.clone();
        let ranges = ranges.clone();
        tokio::task::spawn_blocking(move || {
            // browse() is a plain polling GET with no client-cancellation
            // concept (unlike the SSE endpoints' `CancelOnDrop`), so it
            // always passes a flag that's never set.
            fetch::fetch_ranges_blocking(&cfg, &topic, &ranges, (limit as usize) * ranges.len().max(1), &AtomicBool::new(false))
        }).await.map_err(|e| ApiError::internal(format!("task join: {e}")))??.records
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
            let wm = fetch::watermarks_blocking(&handle, &topic)?;
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

/// Default per-request scan budget for timeline pages. Only exercised by
/// filtered pages (task 4); unfiltered pages (this task) never scan past
/// their `limit`-sized windows, so this is currently a fixed constant rather
/// than a `Limits` field — matching the "filter: None" scope of this task.
const DEFAULT_TIMELINE_SCAN_BUDGET: u64 = 250_000;

#[derive(Debug, Deserialize)]
pub struct TimelineParams {
    pub direction: String,
    pub limit: Option<u64>,
    pub anchor: Option<String>,
    pub cursor: Option<String>,
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    pub ts_ms: Option<i64>,
}

/// Validated before any Kafka round-trip, same rationale as `Anchor::parse`
/// and `Range::parse`: a malformed request must 400 fast, even against a
/// topic that doesn't exist and even on a request that would otherwise
/// stream.
enum TimelineAnchorInput {
    Latest,
    Beginning,
    Offset { partition: i32, offset: i64 },
    Timestamp { ts_ms: i64 },
}

impl TimelineAnchorInput {
    fn parse(params: &TimelineParams) -> Result<Self, ApiError> {
        match params.anchor.as_deref() {
            Some("latest") => Ok(TimelineAnchorInput::Latest),
            Some("beginning") => Ok(TimelineAnchorInput::Beginning),
            Some("offset") => Ok(TimelineAnchorInput::Offset {
                partition: params.partition
                    .ok_or_else(|| ApiError::bad_request("anchor=offset requires partition"))?,
                offset: params.offset
                    .ok_or_else(|| ApiError::bad_request("anchor=offset requires offset"))?,
            }),
            Some("timestamp") => Ok(TimelineAnchorInput::Timestamp {
                ts_ms: params.ts_ms
                    .ok_or_else(|| ApiError::bad_request("anchor=timestamp requires ts_ms"))?,
            }),
            Some(other) => Err(ApiError::bad_request(format!("unknown anchor '{other}'"))),
            None => unreachable!("cursor xor anchor already validated by the caller"),
        }
    }
}

/// Where this page's starting positions come from, resolved *after* pure
/// param validation but *before* the (single) watermarks round trip below —
/// an anchor still needs watermarks to resolve into positions; a cursor's
/// positions are already known from decoding alone.
enum PositionSource {
    FromCursor(Vec<(i32, i64)>),
    FromAnchor(TimelineAnchorInput),
}

pub async fn timeline_sse(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
    Query(params): Query<TimelineParams>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    // Fix round 1, M3: every param is validated — direction, limit, the
    // cursor/anchor xor, the anchor's own shape, and (I1) a cursor's
    // direction against the request's — strictly before `registry.get`, so
    // a bad request against an *unknown* cluster is still 400, never a 404
    // that masks the real problem.
    let direction = match params.direction.as_str() {
        "back" => timeline::Direction::Back,
        "forward" => timeline::Direction::Forward,
        other => return Err(ApiError::bad_request(format!("unknown direction '{other}'"))),
    };

    // M2: 0 is nonsensical (an empty page forever); M1: cap restored to 500,
    // matching browse's cap — 1000 was never intentional, just not yet
    // reviewed.
    let limit = params.limit.unwrap_or(100);
    if limit == 0 {
        return Err(ApiError::bad_request("limit must be >= 1"));
    }
    let limit = limit.min(500) as usize;

    if params.cursor.is_some() == params.anchor.is_some() {
        return Err(ApiError::bad_request("exactly one of cursor or anchor is required"));
    }

    let source = match &params.cursor {
        Some(cursor) => {
            let decoded = timeline::Cursor::decode(cursor)
                .map_err(|e| ApiError::bad_request(format!("bad cursor: {e}")))?;
            // I1: a cursor carries the direction it continues in; a request
            // that flips `direction` against a stale/foreign cursor must
            // fail fast, not silently paginate the wrong way.
            if decoded.direction != direction {
                return Err(ApiError::bad_request("cursor direction does not match direction param"));
            }
            PositionSource::FromCursor(decoded.positions)
        }
        None => PositionSource::FromAnchor(TimelineAnchorInput::parse(&params)?),
    };

    let handle = state.registry.get(&cluster)?;

    // M4: exactly one watermarks round trip per page request, whether this
    // is an anchor page (which needs watermarks to resolve positions) or a
    // cursor page (which needs them anyway, for `run_page`'s own windowing
    // and exhaustion check) — never two for the same request.
    type PositionsAndWatermarks = (Vec<(i32, i64)>, Vec<(i32, i64, i64)>);
    let (positions, watermarks) = {
        let handle = handle.clone();
        let topic = topic.clone();
        tokio::task::spawn_blocking(move || -> Result<PositionsAndWatermarks, ApiError> {
            let wm = fetch::watermarks_blocking(&handle, &topic)?;
            let positions = match source {
                PositionSource::FromCursor(positions) => positions,
                PositionSource::FromAnchor(input) => {
                    let anchor = match input {
                        TimelineAnchorInput::Latest => timeline::Anchor::Latest,
                        TimelineAnchorInput::Beginning => timeline::Anchor::Beginning,
                        TimelineAnchorInput::Offset { partition, offset } => {
                            timeline::Anchor::Offset { partition, offset }
                        }
                        TimelineAnchorInput::Timestamp { ts_ms } => {
                            let resolved = ts_starts_blocking(&handle, &topic, &wm, ts_ms)?;
                            timeline::Anchor::TimestampResolved(resolved)
                        }
                    };
                    timeline::initial_positions(&wm, &anchor)
                }
            };
            Ok((positions, wm))
        }).await.map_err(|e| ApiError::internal(format!("task join: {e}")))??
    };

    let (rx, cancel) = timeline::run_page(handle, topic, positions, watermarks, direction, limit, None, DEFAULT_TIMELINE_SCAN_BUDGET);
    let guard = CancelOnDrop(cancel);
    let stream = ReceiverStream::new(rx).map(move |event: TimelineEvent| {
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
    let stream = ReceiverStream::new(rx).map(move |event: tail::TailEvent| {
        let _hold = &guard; // move the guard into the stream: dropped on disconnect
        Ok(Event::default().event(event.name()).data(serde_json::to_string(&event).unwrap_or_default()))
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
