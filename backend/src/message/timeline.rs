//! Cursor codec, window math, and the paging engine for the messages
//! timeline.
//!
//! `Direction`/`Cursor`/`Anchor`/`initial_positions`/`page_windows`/`advance`
//! are pure functions: no I/O, no Kafka client, nothing async. `run_page`
//! (this task) is the engine that drives them against a real cluster: it
//! fetches fresh watermarks, computes windows, scans and decodes them, and
//! emits `TimelineEvent`s over an mpsc channel — the SSE handler in
//! `api::messages` just maps those to wire events.

use base64::Engine as _;
use serde::{Deserialize, Serialize};

use super::fetch::{self, RawRecord};
use super::filter::Filter;
use super::range::{self, PartitionRange};
use super::MessageOut;
use crate::cluster::{ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use rdkafka::consumer::Consumer;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Which way a page reads relative to its cursor's positions.
///
/// Per the design doc: for `Back`, a position is the *exclusive upper
/// bound* of the next page (the next record read is strictly below it);
/// for `Forward`, a position is the *inclusive lower bound* (the next
/// record read is at-or-above it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Back,
    Forward,
}

/// An opaque, per-partition offset map plus the direction it continues in.
/// Serialized as compact JSON, then base64, so it round-trips as a single
/// URL-safe-ish query-string token without the client needing to know its
/// shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cursor {
    pub direction: Direction,
    pub positions: Vec<(i32, i64)>,
}

impl Cursor {
    pub fn encode(&self) -> String {
        // `serde_json::to_vec` on a struct made only of enums/tuples/Vecs
        // never fails (no maps with non-string keys, no floats), so this
        // is infallible in practice.
        let json = serde_json::to_vec(self).expect("Cursor always serializes");
        base64::engine::general_purpose::STANDARD.encode(json)
    }

    pub fn decode(s: &str) -> Result<Cursor, String> {
        let bytes = base64::engine::general_purpose::STANDARD.decode(s).map_err(|e| e.to_string())?;
        serde_json::from_slice(&bytes).map_err(|e| e.to_string())
    }
}

/// Where a fresh (non-cursor) page request starts from.
#[derive(Debug, Clone, PartialEq)]
pub enum Anchor {
    /// Start at the high watermark of every partition (open the tab: latest
    /// N, live prepend on).
    Latest,
    /// Start at the low watermark of every partition (jump to beginning).
    Beginning,
    /// Jump to a specific message. Only the named partition is positioned
    /// at that message; every other partition starts at its own high
    /// watermark.
    ///
    /// This is a deliberate simplification, not an oversight: an offset
    /// anchor is a *partition-local* jump (the user picked one message in
    /// one partition), and there's no principled cross-partition offset to
    /// derive from it — a message's timestamp doesn't imply a comparable
    /// offset in another partition. The global timeline is produced by the
    /// merge-sort-by-timestamp step downstream, which is what actually
    /// reconciles the anchored partition's neighborhood against everyone
    /// else's tail; `initial_positions` just has to give every partition a
    /// *valid* starting point.
    Offset { partition: i32, offset: i64 },
    /// Timestamp anchor, already resolved upstream (one Kafka
    /// `OffsetsForTimes` call per partition) to either the offset of the
    /// first message at-or-after the timestamp, or `None` if no such
    /// message exists (the timestamp is past the newest message in that
    /// partition). `None` is treated the same as "nothing more to find
    /// here": position at the high watermark, matching `Latest`'s
    /// behavior for that partition.
    TimestampResolved(Vec<(i32, Option<i64>)>),
}

/// Computes the starting cursor positions for a fresh (non-cursor) page
/// request. Returned in `watermarks`' partition order.
pub fn initial_positions(watermarks: &[(i32, i64, i64)], anchor: &Anchor) -> Vec<(i32, i64)> {
    match anchor {
        Anchor::Latest => watermarks.iter().map(|&(p, _, hi)| (p, hi)).collect(),
        Anchor::Beginning => watermarks.iter().map(|&(p, lo, _)| (p, lo)).collect(),
        Anchor::Offset { partition, offset } => watermarks
            .iter()
            .map(|&(p, _, hi)| if p == *partition { (p, offset + 1) } else { (p, hi) })
            .collect(),
        Anchor::TimestampResolved(resolved) => watermarks
            .iter()
            .map(|&(p, _, hi)| {
                let found = resolved.iter().find(|&&(rp, _)| rp == p).and_then(|&(_, o)| o);
                (p, found.unwrap_or(hi))
            })
            .collect(),
    }
}

/// Computes, per partition, a window of up to `span` records adjacent to
/// `positions` in `direction`, clamped to that partition's watermarks.
/// Partitions with nothing available in that direction (already at the
/// edge) are omitted, matching `range::window_ranges`'s convention.
pub fn page_windows(
    positions: &[(i32, i64)],
    watermarks: &[(i32, i64, i64)],
    direction: Direction,
    span: u64,
) -> Vec<range::PartitionRange> {
    let span = span as i64;
    positions
        .iter()
        .filter_map(|&(partition, pos)| {
            let &(_, lo, hi) = watermarks.iter().find(|(p, _, _)| *p == partition)?;
            let pos = pos.clamp(lo, hi);
            let (start, end) = match direction {
                Direction::Back => ((pos - span).max(lo), pos),
                Direction::Forward => (pos, (pos + span).min(hi)),
            };
            (end > start).then_some(range::PartitionRange { partition, start, end })
        })
        .collect()
}

/// Advances `positions` past the given `windows` (the page just consumed),
/// returning the new positions and whether every partition has reached the
/// true topic edge in `direction`.
///
/// Fix round 1 (see task-1 report): the original 3-argument signature
/// (`positions`, `windows`, `direction`) could not distinguish "clamped by
/// the watermark" from "given a full `span`-sized page, more data beyond"
/// using only the consumed windows — the shipped proxy for that was a
/// tautology (`window.len() <= max(all window lens)` is true of every
/// window, including the widest one, unconditionally), so it reported
/// `exhausted: true` on every page. `watermarks` is now passed explicitly
/// and the edge test is exact: for `Back`, a partition is at its edge once
/// its new position equals that partition's low watermark; for `Forward`,
/// once it equals the high watermark. A partition absent from `windows`
/// (its window was empty — `page_windows` only omits a partition when its
/// position is already sitting at that edge) is at its edge by definition,
/// with its position left unchanged. `exhausted` is true iff every
/// partition is at its edge.
pub fn advance(
    positions: &[(i32, i64)],
    windows: &[range::PartitionRange],
    watermarks: &[(i32, i64, i64)],
    direction: Direction,
) -> (Vec<(i32, i64)>, bool) {
    let find_window = |p: i32| windows.iter().find(|w| w.partition == p);
    let find_watermark = |p: i32| watermarks.iter().find(|(wp, _, _)| *wp == p);

    let new_positions: Vec<(i32, i64)> = positions
        .iter()
        .map(|&(p, pos)| match find_window(p) {
            Some(w) => (p, match direction { Direction::Back => w.start, Direction::Forward => w.end }),
            None => (p, pos),
        })
        .collect();

    let exhausted = new_positions.iter().all(|&(p, pos)| match find_watermark(p) {
        Some(&(_, lo, hi)) => match direction {
            Direction::Back => pos == lo,
            Direction::Forward => pos == hi,
        },
        // No watermark on record for this partition: nothing to compare
        // against, so it can't be asserted at an edge.
        None => false,
    });

    (new_positions, exhausted)
}

/// One event of a `run_page` SSE stream. Serialized untagged (like
/// `search::SearchEvent`): the SSE `event:` field carries the discriminant
/// (`.name()`), so the JSON `data:` payload is just the variant's own
/// fields.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum TimelineEvent {
    Match(Box<MessageOut>),
    Progress { scanned: u64, matches: u64, budget: u64 },
    PageEnd { cursor: Option<String>, exhausted: bool },
    Error { code: String, message: String, cluster: Option<String>, retriable: bool },
}

impl TimelineEvent {
    pub fn name(&self) -> &'static str {
        match self {
            TimelineEvent::Match(_) => "match",
            TimelineEvent::Progress { .. } => "progress",
            TimelineEvent::PageEnd { .. } => "page_end",
            TimelineEvent::Error { .. } => "error",
        }
    }
}

impl From<ApiError> for TimelineEvent {
    fn from(e: ApiError) -> Self {
        TimelineEvent::Error { code: e.code.to_string(), message: e.message, cluster: e.cluster, retriable: e.retriable }
    }
}

/// Fetches fresh per-partition watermarks for `topic`. Deliberately separate
/// from `api::messages::watermarks_blocking` (which the browse/search
/// handlers use to resolve an anchor *before* calling into this engine):
/// `run_page` always needs its own fresh watermarks regardless of whether
/// this page started from an anchor or a cursor, so it cannot rely on
/// whatever the caller may or may not have already fetched.
fn watermarks_blocking(handle: &ClusterHandle, topic: &str) -> Result<Vec<(i32, i64, i64)>, ApiError> {
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

/// Runs one unfiltered timeline page (`filter: None` — the only path this
/// task implements; a filtered page is Task 4's job and reports an explicit
/// `error` rather than silently behaving like an unfiltered scan).
///
/// Unfiltered semantics: `span = limit` per partition (no budget-driven
/// scanning needed — every window record is a candidate, so there's nothing
/// to keep scanning past), scan every window, decode, merge-sort globally by
/// `(timestamp_ms, partition, offset)` (desc for `Back`, asc for `Forward`),
/// then truncate to `limit` keeping the records nearest the position (the
/// front of that sorted order).
///
/// The subtle part is the cursor: it must advance only past what was
/// *actually emitted*, not past the full requested window. If truncation
/// dropped some of a partition's window (because other partitions had
/// records closer to the position), naively advancing to that partition's
/// full window boundary would silently skip the dropped records on the next
/// page. Instead, per-partition extremes are recomputed from the emitted set
/// itself: for `Back`, the new position is the lowest offset actually
/// emitted for that partition; for `Forward`, one past the highest. A
/// partition with nothing in the emitted set keeps its old position
/// unchanged (its window, if any, was entirely displaced by other
/// partitions' records — nothing has been consumed from it yet).
pub fn run_page(
    handle: Arc<ClusterHandle>,
    topic: String,
    positions: Vec<(i32, i64)>,
    direction: Direction,
    limit: usize,
    filter: Option<Filter>,
    budget: u64,
) -> (mpsc::Receiver<TimelineEvent>, Arc<AtomicBool>) {
    let (tx, rx) = mpsc::channel::<TimelineEvent>(256);
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel = cancelled.clone();
    // Budget only bounds filtered scans (task 4): an unfiltered page's window
    // span is exactly `limit`, so there is nothing left to keep scanning
    // past once the window is consumed.
    let _ = budget;

    tokio::spawn(async move {
        if filter.is_some() {
            // Filtered timeline pages (budget-driven scanning with a
            // `continue` affordance) are Task 4's responsibility. Reporting
            // a loud `error` here — rather than quietly ignoring the filter
            // and returning an unfiltered page — matches the product
            // charter's "never silently skip" rule.
            let _ = tx.send(TimelineEvent::Error {
                code: "not_implemented".into(),
                message: "filtered timeline pages are not yet implemented".into(),
                cluster: Some(handle.name.clone()),
                retriable: false,
            }).await;
            return;
        }

        type ScanResult = Result<(Vec<RawRecord>, Vec<(i32, i64, i64)>), ApiError>;
        let result = {
            let handle = handle.clone();
            let topic = topic.clone();
            let positions = positions.clone();
            tokio::task::spawn_blocking(move || -> ScanResult {
                let watermarks = watermarks_blocking(&handle, &topic)?;
                let windows = page_windows(&positions, &watermarks, direction, limit as u64);
                let cap = range::total(&windows) as usize;
                let records = fetch::fetch_ranges_blocking(&handle.config, &topic, &windows, cap)?;
                Ok((records, watermarks))
            }).await
        };

        let (records, watermarks) = match result {
            Ok(Ok(pair)) => pair,
            Ok(Err(e)) => {
                let _ = tx.send(e.into()).await;
                return;
            }
            Err(join_err) => {
                let _ = tx.send(ApiError::internal(format!("task join: {join_err}")).into()).await;
                return;
            }
        };

        let mut merged = fetch::to_message_out(records, handle.schema_registry.as_deref()).await;
        match direction {
            Direction::Back => merged.sort_by(|a, b| {
                b.timestamp_ms.unwrap_or(i64::MIN).cmp(&a.timestamp_ms.unwrap_or(i64::MIN))
                    .then(a.partition.cmp(&b.partition))
                    .then(a.offset.cmp(&b.offset))
            }),
            Direction::Forward => merged.sort_by(|a, b| {
                a.timestamp_ms.unwrap_or(i64::MIN).cmp(&b.timestamp_ms.unwrap_or(i64::MIN))
                    .then(a.partition.cmp(&b.partition))
                    .then(a.offset.cmp(&b.offset))
            }),
        }
        merged.truncate(limit);

        // Recompute per-partition extremes from what was *actually emitted*
        // (see the doc comment above) — this is what makes the cursor safe
        // against truncation, whether or not truncation actually cut
        // anything from a given partition.
        let emitted_windows: Vec<PartitionRange> = positions
            .iter()
            .filter_map(|&(partition, _)| {
                let offsets: Vec<i64> = merged.iter().filter(|m| m.partition == partition).map(|m| m.offset).collect();
                match direction {
                    Direction::Back => offsets.iter().min().map(|&lo| PartitionRange { partition, start: lo, end: lo }),
                    Direction::Forward => offsets.iter().max().map(|&hi| PartitionRange { partition, start: hi + 1, end: hi + 1 }),
                }
            })
            .collect();
        let (new_positions, exhausted) = advance(&positions, &emitted_windows, &watermarks, direction);
        let cursor = if exhausted { None } else { Some(Cursor { direction, positions: new_positions }.encode()) };

        for m in merged {
            if tx.send(TimelineEvent::Match(Box::new(m))).await.is_err() {
                return; // client disconnected: no point sending page_end
            }
        }
        let _ = tx.send(TimelineEvent::PageEnd { cursor, exhausted }).await;
    });

    (rx, cancel)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WM: &[(i32, i64, i64)] = &[(0, 10, 110), (1, 0, 5)];

    #[test]
    fn cursor_roundtrips() {
        let c = Cursor { direction: Direction::Back, positions: vec![(0, 60), (1, 5)] };
        let c2 = Cursor::decode(&c.encode()).unwrap();
        assert_eq!(c2.direction, Direction::Back);
        assert_eq!(c2.positions, vec![(0, 60), (1, 5)]);
        assert!(Cursor::decode("garbage!").is_err());
    }

    #[test]
    fn latest_positions_are_high_watermarks() {
        let p = initial_positions(WM, &Anchor::Latest);
        assert_eq!(p, vec![(0, 110), (1, 5)]);
    }

    #[test]
    fn beginning_positions_are_low_watermarks() {
        let p = initial_positions(WM, &Anchor::Beginning);
        assert_eq!(p, vec![(0, 10), (1, 0)]);
    }

    #[test]
    fn back_windows_take_span_below_position() {
        let w = page_windows(&[(0, 110), (1, 5)], WM, Direction::Back, 50);
        assert_eq!(w, vec![
            range::PartitionRange { partition: 0, start: 60, end: 110 },
            range::PartitionRange { partition: 1, start: 0, end: 5 },
        ]);
    }

    #[test]
    fn forward_windows_take_span_above_position() {
        let w = page_windows(&[(0, 10), (1, 0)], WM, Direction::Forward, 3);
        assert_eq!(w, vec![
            range::PartitionRange { partition: 0, start: 10, end: 13 },
            range::PartitionRange { partition: 1, start: 0, end: 3 },
        ]);
    }

    #[test]
    fn advance_back_moves_down_and_detects_exhaustion() {
        let w = page_windows(&[(0, 60), (1, 5)], WM, Direction::Back, 100);
        let (p, exhausted) = advance(&[(0, 60), (1, 5)], &w, WM, Direction::Back);
        assert_eq!(p, vec![(0, 10), (1, 0)]);
        assert!(exhausted); // both hit low watermark
    }

    #[test]
    fn advance_forward_detects_edge_against_high() {
        let w = page_windows(&[(0, 100), (1, 5)], WM, Direction::Forward, 50);
        let (p, exhausted) = advance(&[(0, 100), (1, 5)], &w, WM, Direction::Forward);
        assert_eq!(p, vec![(0, 110), (1, 5)]);
        assert!(exhausted);
    }

    #[test]
    fn first_page_of_deep_partition_is_not_exhausted() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 1_000_000)];
        let positions = initial_positions(wm, &Anchor::Latest);
        let w = page_windows(&positions, wm, Direction::Back, 100);
        let (p, exhausted) = advance(&positions, &w, wm, Direction::Back);
        assert_eq!(p, vec![(0, 999_900)]);
        assert!(!exhausted);
    }

    #[test]
    fn imbalanced_partitions_exhaust_independently() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 1000), (1, 0, 5)];
        let positions = initial_positions(wm, &Anchor::Latest);
        let w = page_windows(&positions, wm, Direction::Back, 100);
        let (p, exhausted) = advance(&positions, &w, wm, Direction::Back);
        assert_eq!(p, vec![(0, 900), (1, 0)]);
        assert!(!exhausted); // partition 0 still has plenty of room left

        let w2 = page_windows(&p, wm, Direction::Back, 1000);
        let (p2, exhausted2) = advance(&p, &w2, wm, Direction::Back);
        assert_eq!(p2, vec![(0, 0), (1, 0)]);
        assert!(exhausted2); // both partitions now at their low watermark
    }
}
