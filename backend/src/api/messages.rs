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

/// I2: the wire shape actually extracted from the query string — every
/// field a plain, optional string, so axum's `Query<RawTimelineParams>`
/// extraction itself can (almost) never fail to deserialize. A typed
/// `Query<TimelineParams>` (a required `String` plus `Option<u64>`/
/// `Option<i32>`/`Option<i64>` fields) rejects a malformed value (`limit=
/// abc`) or a missing required field *before* the handler body ever runs,
/// via axum's own `QueryRejection` — a bare `text/plain` body with serde's
/// own vocabulary, bypassing the `{code,message,cluster,retriable}`
/// envelope every other error uses. Parsing into `TimelineParams` is done
/// by `parse_raw` below instead, inside the handler, so a bad numeric value
/// becomes an ordinary `ApiError::bad_request` — which for this
/// SSE-consumed endpoint flows through the same in-stream `app_error` path
/// as every other pre-stream validation failure (see I4 / `timeline_sse`).
#[derive(Debug, Deserialize)]
pub struct RawTimelineParams {
    pub direction: Option<String>,
    pub limit: Option<String>,
    pub anchor: Option<String>,
    pub cursor: Option<String>,
    pub partition: Option<String>,
    pub offset: Option<String>,
    pub ts_ms: Option<String>,
    pub filter: Option<String>,
    pub q: Option<String>,
    pub path: Option<String>,
}

/// Parses one optional numeric string field, producing a precise
/// `bad_request` (naming the field) rather than the generic serde message
/// axum's own `Query` rejection would have produced.
fn parse_numeric_field<T: std::str::FromStr>(field: &str, raw: Option<&str>) -> Result<Option<T>, ApiError> {
    match raw {
        None => Ok(None),
        Some(s) => s.parse::<T>().map(Some).map_err(|_| {
            ApiError::bad_request(format!("{field}: invalid number '{s}'"))
        }),
    }
}

fn parse_raw(raw: &RawTimelineParams) -> Result<TimelineParams, ApiError> {
    Ok(TimelineParams {
        // A missing `direction` is reported by `parse_direction`'s own
        // dedicated "" branch below (empty string is what a missing param
        // becomes here), so no separate required-field check is needed.
        direction: raw.direction.clone().unwrap_or_default(),
        limit: parse_numeric_field("limit", raw.limit.as_deref())?,
        anchor: raw.anchor.clone(),
        cursor: raw.cursor.clone(),
        partition: parse_numeric_field("partition", raw.partition.as_deref())?,
        offset: parse_numeric_field("offset", raw.offset.as_deref())?,
        ts_ms: parse_numeric_field("ts_ms", raw.ts_ms.as_deref())?,
        filter: raw.filter.clone(),
        q: raw.q.clone(),
        path: raw.path.clone(),
    })
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
        // A missing `direction` reaches here as an empty string (see
        // `parse_raw`'s comment) — "direction is required" says what's
        // actually wrong; "unknown direction ''" would read as confusing.
        "" => Err(ApiError::bad_request("direction is required")),
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

/// A fresh, single-purpose channel carrying exactly one event: for a
/// request that can't stream (param validation or anchor resolution
/// failed) but must still reach the client as a 200 SSE response — see
/// `timeline_sse`'s own comment for why a pre-stream error body is
/// invisible to `EventSource`. `try_send` on a brand-new, empty channel
/// can't fail for capacity reasons, and there is nothing else to send
/// after it — the sender drops at the end of this call, closing the
/// channel once this one buffered event has been delivered. The guard
/// wraps a fresh, inert flag: no scan ever started, so there's nothing to
/// cancel.
fn single_event_stream(event: TimelineEvent) -> (mpsc::Receiver<TimelineEvent>, CancelOnDrop) {
    let (tx, rx) = mpsc::channel::<TimelineEvent>(1);
    let _ = tx.try_send(event);
    (rx, CancelOnDrop(Arc::new(AtomicBool::new(false))))
}

pub async fn timeline_sse(
    State(state): State<AppState>,
    Path((cluster, topic)): Path<(String, String)>,
    Query(raw): Query<RawTimelineParams>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    // I4: every one of these is a plain, synchronous validation — no I/O,
    // no cluster handle needed — but the fix bundles them into one `Result`
    // computed up front (rather than `?`-returning straight out of the
    // handler) for the same reason `resolved` below is: a pre-stream HTTP
    // error body is invisible to `EventSource` (it discards a non-200
    // response wholesale), so the frontend would see generic "connection
    // lost" instead of the real validation reason. Validated strictly
    // before `registry.get` either way, so a bad request against an
    // unknown cluster still reports the param error, never a
    // `cluster_not_found` that masks it.
    //
    // I2: `parse_raw` (turning the query string's plain strings into
    // typed fields) is INSIDE this same validated block, not a separate
    // `Query<TimelineParams>` extractor argument — see `RawTimelineParams`'s
    // doc comment for why: an extractor-level rejection bypasses this
    // whole mechanism and answers with axum's own `text/plain` body.
    let validated: Result<(timeline::Direction, usize, PositionSource, Option<Filter>), ApiError> = (|| {
        let params = parse_raw(&raw)?;
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
        Ok((direction, limit, source, filter))
    })();

    // Both branches below feed the same `mpsc::Receiver<TimelineEvent>`
    // shape, so `Sse<impl Stream<...>>`'s single concrete return type is
    // satisfied either way. `CancelOnDrop` is tied to the stream's own
    // `.map()` closure lifetime, so a client disconnect flips `cancel` the
    // instant the stream is dropped, not only once some in-flight send
    // happens to fail — the "no zombie scans" guarantee.
    let (rx, guard): (mpsc::Receiver<TimelineEvent>, CancelOnDrop) = match validated {
        Err(e) => single_event_stream(e.into()),
        Ok((direction, limit, source, filter)) => {
            // Residual pre-stream error, deliberately not folded into
            // `resolved` below: an unknown cluster still short-circuits
            // here as a pre-stream 404, same as any other handler — a
            // client with an otherwise-valid request but a bad cluster name
            // sees it as a generic "connection lost" from `EventSource`
            // (no `text/event-stream` response body was ever produced to
            // carry an in-stream event), unlike every OTHER validation
            // failure in this handler, which surfaces as a structured
            // in-stream error instead. Left as-is.
            let handle = state.registry.get(&cluster)?;

            // One watermarks round trip per page request either way: an anchor
            // page needs them to resolve positions, a cursor page needs them
            // anyway for `run_page`'s own windowing and exhaustion check.
            //
            // `resolved` is kept as a `Result` rather than `?`-unwrapped here so
            // an anchor resolution failure (e.g. "no message at that offset")
            // becomes an IN-STREAM `TimelineEvent::Error` below instead of a
            // pre-stream HTTP error. The one exception is a `spawn_blocking`
            // `JoinError` (the task itself panicked) — a genuine internal
            // fault, not a rejection of anything the user did — which still
            // short-circuits as a pre-stream 500 via the `?` on the join
            // itself.
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

            match resolved {
                Ok((positions, watermarks)) => {
                    let budget = state.limits.timeline_scan_budget;
                    let req = timeline::PageRequest { positions, watermarks, direction, limit, filter, budget };
                    let (rx, cancel) = timeline::run_page(handle, topic, req);
                    (rx, CancelOnDrop(cancel))
                }
                Err(e) => single_event_stream(e.into()),
            }
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
        assert_eq!(err.message, "unknown direction 'sideways'");
    }

    /// A missing `direction` reaches `parse_direction` as an empty string
    /// (see `parse_raw`'s own comment) — "unknown direction ''" is a
    /// confusing way to say that; "direction is required" says exactly
    /// what's wrong.
    #[test]
    fn parse_direction_names_a_missing_direction_as_required_not_unknown() {
        let err = parse_direction(&TimelineParams { direction: String::new(), ..params() }).unwrap_err();
        assert_eq!(err.code, "bad_request");
        assert_eq!(err.message, "direction is required");
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
