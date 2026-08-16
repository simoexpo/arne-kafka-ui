use crate::error::ApiError;
use crate::message::fetch;
use crate::message::filter::Filter;
use crate::message::tail;
use crate::message::timeline::{self, TimelineAnchorInput, TimelineEvent};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::Stream;
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as _;

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
/// would otherwise stream. Parses into `timeline::TimelineAnchorInput`,
/// which `timeline::resolve_positions_blocking` resolves into positions.
fn parse_anchor_input(params: &TimelineParams) -> Result<TimelineAnchorInput, ApiError> {
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

/// Where this page's starting positions come from, resolved *after* pure
/// param validation but *before* the (single) watermarks round trip below —
/// an anchor still needs watermarks to resolve into positions; a cursor's
/// positions are already known from decoding alone.
enum PositionSource {
    FromCursor(Vec<(i32, i64)>),
    FromAnchor(TimelineAnchorInput),
}

fn parse_direction(params: &TimelineParams) -> Result<timeline::Direction, ApiError> {
    match params.direction.as_str() {
        "back" => Ok(timeline::Direction::Back),
        "forward" => Ok(timeline::Direction::Forward),
        other => Err(ApiError::bad_request(format!("unknown direction '{other}'"))),
    }
}

/// A page's own hard ceiling, independent of the caller's request: v1 does
/// not offer unbounded pages.
const MAX_LIMIT: u64 = 500;

fn parse_limit(params: &TimelineParams) -> Result<usize, ApiError> {
    let limit = params.limit.unwrap_or(100);
    if limit == 0 {
        return Err(ApiError::bad_request("limit must be >= 1"));
    }
    Ok(limit.min(MAX_LIMIT) as usize)
}

fn parse_filter(params: &TimelineParams) -> Result<Option<Filter>, ApiError> {
    match &params.filter {
        Some(kind) => {
            let q = params.q.as_deref().ok_or_else(|| ApiError::bad_request("filter requires q"))?;
            Ok(Some(Filter::parse(kind, q, params.path.as_deref()).map_err(ApiError::bad_request)?))
        }
        None => Ok(None),
    }
}

pub async fn timeline_sse(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
    Query(params): Query<TimelineParams>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    // Every param is validated strictly before `registry.get`, so a bad
    // request against an unknown cluster stays a 400, never a 404 that
    // masks the real problem.
    let direction = parse_direction(&params)?;
    let limit = parse_limit(&params)?;
    if params.cursor.is_some() == params.anchor.is_some() {
        return Err(ApiError::bad_request("exactly one of cursor or anchor is required"));
    }
    let filter = parse_filter(&params)?;

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
        None => PositionSource::FromAnchor(parse_anchor_input(&params)?),
    };

    let handle = state.registry.get(&cluster)?;

    // One watermarks round trip per page request either way: an anchor page
    // needs them to resolve positions, a cursor page needs them anyway for
    // `run_page`'s own windowing and exhaustion check.
    //
    // `resolved` is kept as a `Result` rather than `?`-unwrapped here so an
    // anchor resolution failure (e.g. "no message at that offset") becomes
    // an IN-STREAM `TimelineEvent::Error` below instead of a pre-stream HTTP
    // error: `EventSource` discards a non-200 response body wholesale, so
    // the frontend would otherwise see generic "connection lost" instead of
    // the real reason. The one exception is a `spawn_blocking` `JoinError`
    // (the task itself panicked) — a genuine internal fault, not a
    // rejection of anything the user did — which still short-circuits as a
    // pre-stream 500 via the `?` on the join itself.
    type PositionsAndWatermarks = (Vec<(i32, i64)>, Vec<(i32, i64, i64)>);
    let resolved: Result<PositionsAndWatermarks, ApiError> = {
        let handle = handle.clone();
        let topic = topic.clone();
        tokio::task::spawn_blocking(move || -> Result<PositionsAndWatermarks, ApiError> {
            let wm = fetch::watermarks_blocking(&handle, &topic)?;
            let positions = match source {
                PositionSource::FromCursor(positions) => positions,
                PositionSource::FromAnchor(input) => {
                    timeline::resolve_positions_blocking(&handle, &topic, &wm, input, direction)?
                }
            };
            Ok((positions, wm))
        }).await.map_err(ApiError::task_join)?
    };

    // Both branches feed the same `mpsc::Receiver<TimelineEvent>` shape, so
    // `Sse<impl Stream<...>>`'s single concrete return type is satisfied
    // either way. `CancelOnDrop` is tied to the stream's own `.map()`
    // closure lifetime, so a client disconnect flips `cancel` the instant
    // the stream is dropped, not only once some in-flight send happens to
    // fail — the "no zombie scans" guarantee. The error path never started a
    // scan, so its guard wraps a fresh, inert flag: nothing to cancel.
    let (rx, guard): (mpsc::Receiver<TimelineEvent>, CancelOnDrop) = match resolved {
        Ok((positions, watermarks)) => {
            let budget = state.limits.timeline_scan_budget;
            let req = timeline::PageRequest { positions, watermarks, direction, limit, filter, budget };
            let (rx, cancel) = timeline::run_page(handle, topic, req);
            (rx, CancelOnDrop(cancel))
        }
        Err(e) => {
            // A fresh, single-purpose channel carrying exactly one event:
            // `try_send` on a brand-new, empty channel can't fail for
            // capacity reasons, and there is nothing else to send after
            // it — the sender drops at the end of this arm, closing the
            // channel once this one buffered event has been delivered.
            let (tx, rx) = mpsc::channel::<TimelineEvent>(1);
            let _ = tx.try_send(e.into());
            (rx, CancelOnDrop(Arc::new(AtomicBool::new(false))))
        }
    };
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

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> TimelineParams {
        TimelineParams {
            direction: "forward".into(),
            limit: None,
            anchor: None,
            cursor: None,
            partition: None,
            offset: None,
            ts_ms: None,
            filter: None,
            q: None,
            path: None,
        }
    }

    #[test]
    fn parse_direction_accepts_back_and_forward() {
        assert_eq!(parse_direction(&TimelineParams { direction: "back".into(), ..params() }).unwrap(), timeline::Direction::Back);
        assert_eq!(parse_direction(&TimelineParams { direction: "forward".into(), ..params() }).unwrap(), timeline::Direction::Forward);
    }

    #[test]
    fn parse_direction_rejects_anything_else() {
        let err = parse_direction(&TimelineParams { direction: "sideways".into(), ..params() }).unwrap_err();
        assert_eq!(err.code, "bad_request");
    }

    #[test]
    fn parse_limit_defaults_to_100() {
        assert_eq!(parse_limit(&params()).unwrap(), 100);
    }

    #[test]
    fn parse_limit_rejects_zero() {
        let err = parse_limit(&TimelineParams { limit: Some(0), ..params() }).unwrap_err();
        assert_eq!(err.code, "bad_request");
    }

    #[test]
    fn parse_limit_caps_at_500() {
        assert_eq!(parse_limit(&TimelineParams { limit: Some(10_000), ..params() }).unwrap(), 500);
    }

    #[test]
    fn parse_filter_none_when_absent() {
        assert!(parse_filter(&params()).unwrap().is_none());
    }

    #[test]
    fn parse_filter_requires_q() {
        let err = parse_filter(&TimelineParams { filter: Some("contains".into()), ..params() }).unwrap_err();
        assert_eq!(err.code, "bad_request");
    }

    #[test]
    fn parse_filter_builds_a_real_filter() {
        let parsed = parse_filter(&TimelineParams { filter: Some("contains".into()), q: Some("needle".into()), ..params() }).unwrap();
        assert!(parsed.is_some());
    }
}
