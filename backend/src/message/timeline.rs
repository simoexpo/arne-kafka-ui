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
use crate::cluster::ClusterHandle;
use crate::error::ApiError;
use std::collections::{HashMap, HashSet};
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

/// Clamps decoded cursor positions into each partition's *current*
/// `[lo, hi]` watermark range before they drive anything else.
///
/// Fix round 1, C4: a forged cursor, or a legitimate one whose partition has
/// since been trimmed by retention, can carry a position outside today's
/// watermarks (e.g. an offset below the new low watermark). `page_windows`
/// already clamps its internal `pos` when computing a window's bounds — but
/// `advance`'s fallback for an at-edge partition (no window at all) hands
/// back the position *unchanged*. If that position was never clamped, it's
/// the raw out-of-range value, `advance`'s edge check (`pos == lo`/`hi`)
/// never matches it, and the same non-advancing cursor gets re-encoded and
/// handed back forever: identical cursor, `exhausted: false`, nothing new,
/// no error — a silent infinite loop from the client's point of view.
/// Clamping once, up front, before `page_windows`, `advance`, or the cursor
/// encoder ever see `positions`, makes "already at the edge" actually equal
/// `lo`/`hi`, so it's recognized and reported as exhausted instead.
fn clamp_positions(positions: &[(i32, i64)], watermarks: &[(i32, i64, i64)]) -> Vec<(i32, i64)> {
    positions
        .iter()
        .map(|&(partition, pos)| match watermarks.iter().find(|(p, _, _)| *p == partition) {
            Some(&(_, lo, hi)) => (partition, pos.clamp(lo, hi)),
            None => (partition, pos),
        })
        .collect()
}

/// A page "stalled" if it had real windows to scan (Kafka reported data
/// there when watermarks were fetched) but emitted nothing at all and no
/// partition's position moved.
///
/// Fix round 1, C4 (progress guarantee): ordinarily this can't happen —
/// non-empty windows mean at least one partition has records, so at least
/// one gets emitted and/or advances. The one case it doesn't hold is a race
/// against compaction or retention between the watermark fetch and the
/// scan: every offset `page_windows` expected to exist has since vanished.
/// Handing back `page_end` with an unchanged cursor there would make the
/// client loop forever — identical cursor, `exhausted: false`, no new data,
/// ever. That must surface as a terminal `error` instead.
fn page_stalled(
    windows: &[PartitionRange],
    emitted_count: usize,
    positions: &[(i32, i64)],
    new_positions: &[(i32, i64)],
) -> bool {
    !windows.is_empty() && emitted_count == 0 && new_positions == positions
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
/// The cursor must advance only past what was *actually emitted*, not past
/// the full requested window — and (fix round 1) it must do so in a way
/// that survives non-monotonic (producer-stamped) timestamps, not just
/// simple truncation.
///
/// Fix round 1, C1: within an equal-timestamp tie in one partition, `Back`
/// breaks ties by offset *descending* — the higher offset is the one
/// actually nearer the position (later in the partition, whatever its
/// timestamp says), so it must be the one kept when truncation has to
/// choose. The original ascending tie-break could keep a lower offset over
/// a higher one from the same partition/timestamp, which fed directly into
/// C2 (falsely reporting a partial page as exhausted) once combined with
/// the old emitted-extremes cursor formula.
///
/// Fix round 1, C3: that old formula — new position = the emitted set's own
/// min (`Back`)/max+1 (`Forward`) offset per partition — assumed the
/// emitted subset of a partition's window is always a clean prefix/suffix
/// by offset. Non-monotonic timestamps break that assumption: a record can
/// sort to the far end of the global merge (and get truncated away) while
/// sitting at a *low* offset in its partition, with a *higher*-offset,
/// same-or-later-timestamp sibling from the same partition making the cut.
/// Advancing to the emitted set's own extreme would then silently step past
/// the dropped low-offset record forever. The fix anchors the new position
/// on what was *dropped* from each partition's window instead of what was
/// emitted: `Back` position = `max(dropped) + 1` (or `window.start` if
/// nothing was dropped); `Forward` position = `min(dropped)` (or
/// `window.end` if nothing was dropped). This is safe by construction — the
/// new position always excludes every dropped offset — at the cost of
/// bounded overlap: a partition that still has a dropped record pending can
/// re-offer an already-emitted sibling on a later page. That's an accepted
/// trade (the frontend already dedups on `(partition, offset)`; the product
/// charter forbids losing records, not repeating one at a page boundary).
#[allow(clippy::too_many_arguments)] // mirrors search.rs's `drive_partition`
pub fn run_page(
    handle: Arc<ClusterHandle>,
    topic: String,
    positions: Vec<(i32, i64)>,
    watermarks: Vec<(i32, i64, i64)>,
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

        // C4: clamp before anything else sees `positions` (see the doc
        // comment on `clamp_positions`).
        let positions = clamp_positions(&positions, &watermarks);

        type ScanResult = Result<(Vec<RawRecord>, Vec<PartitionRange>), ApiError>;
        let result = {
            let cfg = handle.config.clone();
            let topic = topic.clone();
            let positions = positions.clone();
            let watermarks = watermarks.clone();
            // I3: `run_page`'s own `cancelled` flag (checked by
            // `CancelOnDrop` on client disconnect) now actually reaches the
            // blocking scan loop, the same way `search.rs`'s does — a
            // dropped SSE stream stops the in-flight Kafka poll loop instead
            // of only ever toggling a flag nothing reads.
            let cancelled = cancelled.clone();
            tokio::task::spawn_blocking(move || -> ScanResult {
                let windows = page_windows(&positions, &watermarks, direction, limit as u64);
                let cap = range::total(&windows) as usize;
                let records = fetch::fetch_ranges_blocking(&cfg, &topic, &windows, cap, &cancelled)?;
                Ok((records, windows))
            }).await
        };

        let (mut records, windows) = match result {
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

        // I2: sort + truncate the raw, undecoded records first — only the
        // page that's actually emitted gets decoded (which can be
        // expensive: schema lookups, Avro/Protobuf parsing), not every
        // candidate across every partition's full window.
        match direction {
            Direction::Back => records.sort_by(|a, b| {
                b.timestamp_ms.unwrap_or(i64::MIN).cmp(&a.timestamp_ms.unwrap_or(i64::MIN))
                    .then(a.partition.cmp(&b.partition))
                    .then(b.offset.cmp(&a.offset)) // C1: offset DESC within a tie
            }),
            Direction::Forward => records.sort_by(|a, b| {
                a.timestamp_ms.unwrap_or(i64::MIN).cmp(&b.timestamp_ms.unwrap_or(i64::MIN))
                    .then(a.partition.cmp(&b.partition))
                    .then(a.offset.cmp(&b.offset))
            }),
        }

        // Every offset actually fetched per partition, captured *before*
        // truncation — the "window" that C3's dropped-offset formula below
        // is computed against.
        let mut fetched_by_partition: HashMap<i32, Vec<i64>> = HashMap::new();
        for r in &records {
            fetched_by_partition.entry(r.partition).or_default().push(r.offset);
        }

        records.truncate(limit);

        let mut emitted_by_partition: HashMap<i32, HashSet<i64>> = HashMap::new();
        for r in &records {
            emitted_by_partition.entry(r.partition).or_default().insert(r.offset);
        }

        // C3: anchor the new position on what was *dropped*, not on the
        // emitted extremes (see the doc comment above `run_page`).
        let empty_offsets: Vec<i64> = Vec::new();
        let empty_emitted: HashSet<i64> = HashSet::new();
        let cursor_windows: Vec<PartitionRange> = windows
            .iter()
            .map(|w| {
                let fetched = fetched_by_partition.get(&w.partition).unwrap_or(&empty_offsets);
                let emitted = emitted_by_partition.get(&w.partition).unwrap_or(&empty_emitted);
                let dropped: Vec<i64> = fetched.iter().copied().filter(|o| !emitted.contains(o)).collect();
                let pos = match direction {
                    Direction::Back => dropped.iter().max().map_or(w.start, |m| m + 1),
                    Direction::Forward => dropped.iter().min().copied().unwrap_or(w.end),
                };
                PartitionRange { partition: w.partition, start: pos, end: pos }
            })
            .collect();

        let (new_positions, exhausted) = advance(&positions, &cursor_windows, &watermarks, direction);

        // C4 progress guarantee: never hand back an identical,
        // non-exhausted cursor (see the doc comment on `page_stalled`).
        if page_stalled(&windows, records.len(), &positions, &new_positions) {
            let _ = tx.send(TimelineEvent::Error {
                code: "no_progress".into(),
                message: "page produced no records and could not advance past its window \
                          (likely a compaction or retention race)".into(),
                cluster: Some(handle.name.clone()),
                retriable: true,
            }).await;
            return;
        }

        let cursor = if exhausted { None } else { Some(Cursor { direction, positions: new_positions }.encode()) };

        let decoded = fetch::to_message_out(records, handle.schema_registry.as_deref()).await;
        for m in decoded {
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

    /// Fix round 1, C4 (reviewer's exact construction): a forged or
    /// retention-raced cursor position (-5) on watermarks (0, 0, 10) must be
    /// clamped before it drives `page_windows`/`advance` — otherwise
    /// `advance`'s "no window for this partition" fallback hands back the
    /// raw, unclamped -5 forever: `-5 == lo (0)` never holds, so `exhausted`
    /// never becomes true and the identical cursor loops forever. Clamped to
    /// 0 first, the partition is correctly recognized as already at its low
    /// watermark: no window, and `exhausted` becomes true immediately.
    #[test]
    fn clamped_position_reaches_exhaustion_instead_of_looping() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 10)];
        let forged = vec![(0, -5)];

        let positions = clamp_positions(&forged, wm);
        assert_eq!(positions, vec![(0, 0)], "position must be clamped into [lo, hi]");

        let windows = page_windows(&positions, wm, Direction::Back, 5);
        assert!(windows.is_empty(), "already at the low watermark: nothing to fetch");

        let (new_positions, exhausted) = advance(&positions, &windows, wm, Direction::Back);
        assert_eq!(new_positions, vec![(0, 0)]);
        assert!(exhausted, "clamped position at the edge must report exhausted, not loop forever");
    }

    /// Sanity check: a position already inside range is left untouched.
    #[test]
    fn clamp_positions_is_a_no_op_in_range() {
        let wm: &[(i32, i64, i64)] = &[(0, 10, 110), (1, 0, 5)];
        let positions = vec![(0, 60), (1, 3)];
        assert_eq!(clamp_positions(&positions, wm), positions);
    }

    /// Fix round 1, C4 (progress guarantee, second half): a page whose
    /// windows were non-empty but which emitted nothing and advanced no
    /// position must be recognized as stalled — this is the "identical
    /// cursor forever" case the reviewer flagged (realistically triggered by
    /// a compaction/retention race between the watermark fetch and the
    /// scan). Exercised here as a pure predicate since reproducing an actual
    /// compaction race deterministically against a real broker isn't
    /// practical; `run_page` wires this in directly (see its `page_stalled`
    /// call).
    #[test]
    fn page_stalled_detects_no_emitted_records_and_no_advance() {
        let windows = vec![range::PartitionRange { partition: 0, start: 0, end: 5 }];
        let positions = vec![(0, 5)];
        let new_positions = vec![(0, 5)]; // unchanged
        assert!(page_stalled(&windows, 0, &positions, &new_positions));
    }

    #[test]
    fn page_stalled_is_false_when_records_were_emitted() {
        let windows = vec![range::PartitionRange { partition: 0, start: 0, end: 5 }];
        let positions = vec![(0, 5)];
        let new_positions = vec![(0, 5)]; // unchanged, but records *were* emitted
        assert!(!page_stalled(&windows, 3, &positions, &new_positions));
    }

    #[test]
    fn page_stalled_is_false_when_position_advanced() {
        let windows = vec![range::PartitionRange { partition: 0, start: 0, end: 5 }];
        let positions = vec![(0, 5)];
        let new_positions = vec![(0, 2)]; // advanced, even with 0 emitted
        assert!(!page_stalled(&windows, 0, &positions, &new_positions));
    }

    #[test]
    fn page_stalled_is_false_when_no_windows_were_requested() {
        // Already fully at the edge: nothing to scan, not a stall.
        assert!(!page_stalled(&[], 0, &[(0, 0)], &[(0, 0)]));
    }
}
