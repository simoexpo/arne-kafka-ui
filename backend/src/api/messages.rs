use crate::cluster::{ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use crate::message::fetch;
use crate::message::filter::Filter;
use crate::message::tail;
use crate::message::timeline::{self, TimelineEvent};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::Stream;
use rdkafka::consumer::Consumer;
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as _;

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

/// Sets the cancel flag when the SSE stream is dropped (client disconnected),
/// stopping every partition scanner at its next poll iteration — no zombie
/// scans survive a client that goes away mid-scan.
struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[derive(Debug, Deserialize)]
pub struct TimelineParams {
    pub direction: String,
    pub limit: Option<u64>,
    pub anchor: Option<String>,
    pub cursor: Option<String>,
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    pub ts_ms: Option<i64>,
    pub filter: Option<String>,
    pub q: Option<String>,
    pub path: Option<String>,
}

/// Validated before any Kafka round-trip: a malformed request must 400
/// fast, even against a topic that doesn't exist and even on a request that
/// would otherwise stream.
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
    // cursor/anchor xor, and the anchor's own shape — strictly before
    // `registry.get`, so a bad request against an *unknown* cluster is
    // still 400, never a 404 that masks the real problem. (I1's old check,
    // rejecting a cursor whose own `direction` field disagreed with the
    // request's `direction` param, was removed in spec v1.6 — see the
    // `source` match below.)
    let direction = match params.direction.as_str() {
        "back" => timeline::Direction::Back,
        "forward" => timeline::Direction::Forward,
        other => return Err(ApiError::bad_request(format!("unknown direction '{other}'"))),
    };

    // M2: 0 is nonsensical (an empty page forever); M1: cap restored to 500
    // — 1000 was never intentional, just not yet reviewed.
    let limit = params.limit.unwrap_or(100);
    if limit == 0 {
        return Err(ApiError::bad_request("limit must be >= 1"));
    }
    let limit = limit.min(500) as usize;

    if params.cursor.is_some() == params.anchor.is_some() {
        return Err(ApiError::bad_request("exactly one of cursor or anchor is required"));
    }

    // Filter params validated up front too — same rationale as direction/
    // limit/cursor above: a bad filter kind, a missing `q`, or a `json_eq`
    // missing its `path` must 400 before any Kafka round trip, even against
    // an unknown cluster.
    let filter = match &params.filter {
        Some(kind) => {
            let q = params.q.as_deref().ok_or_else(|| ApiError::bad_request("filter requires q"))?;
            Some(Filter::parse(kind, q, params.path.as_deref()).map_err(ApiError::bad_request)?)
        }
        None => None,
    };

    let source = match &params.cursor {
        Some(cursor) => {
            let decoded = timeline::Cursor::decode(cursor)
                .map_err(|e| ApiError::bad_request(format!("bad cursor: {e}")))?;
            // v1.6 owner ruling: direction belongs to the REQUEST, not the
            // cursor blob. `decoded.direction` (the direction the cursor was
            // *minted* in) is intentionally never compared against `direction`
            // here — the request's own `direction` param is authoritative for
            // how `decoded.positions` are read this time (see `Cursor`'s doc
            // comment for the exact bound semantics). This is what makes the
            // sliding window's "re-read a trimmed region by following an edge
            // cursor in the opposite direction" a supported, well-defined
            // request rather than a version mismatch to reject.
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
                            // Owner ruling 2026-08-15: `direction=back` keeps
                            // the original, deliberately simpler `Offset`
                            // behavior (pins every other partition at its
                            // high watermark — documented deferral, see
                            // `Anchor::Offset`'s own doc comment). Only a
                            // forward read gets the new alignment: resolve
                            // the anchored message's own timestamp (a
                            // bounded single-record fetch — same consumer
                            // discipline as everything else here) and align
                            // every other partition at that instant, the
                            // same way a timestamp anchor would.
                            if direction == timeline::Direction::Forward {
                                let anchor_ts = fetch::fetch_one_record_blocking(&handle.config, &topic, partition, offset)?
                                    .and_then(|r| r.timestamp_ms)
                                    .ok_or_else(|| ApiError::bad_request(format!(
                                        "no message with a timestamp at partition {partition} offset {offset}"
                                    )))?;
                                let aligned = ts_starts_blocking(&handle, &topic, &wm, anchor_ts)?;
                                timeline::Anchor::OffsetForwardAligned { partition, offset, aligned }
                            } else {
                                timeline::Anchor::Offset { partition, offset }
                            }
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

    let budget = state.limits.timeline_scan_budget;
    let (rx, cancel) = timeline::run_page(handle, topic, positions, watermarks, direction, limit, filter, budget);
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
