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
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
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
/// Amended during Task 1's own review round (see task-1-report.md's "Fix
/// round 1" section — predating, and unrelated to, this file's *own* later
/// Task 2 fix rounds referenced elsewhere in this module): the original
/// 3-argument signature (`positions`, `windows`, `direction`) could not
/// distinguish "clamped by the watermark" from "given a full `span`-sized
/// page, more data beyond" using only the consumed windows — the shipped
/// proxy for that was a tautology (`window.len() <= max(all window lens)`
/// is true of every window, including the widest one, unconditionally), so
/// it reported `exhausted: true` on every page. `watermarks` is now passed
/// explicitly and the edge test is exact: for `Back`, a partition is at its
/// edge once its new position equals that partition's low watermark; for
/// `Forward`, once it equals the high watermark. A partition absent from
/// `windows` (its window was empty — `page_windows` only omits a partition
/// when its position is already sitting at that edge) is at its edge by
/// definition, with its position left unchanged. `exhausted` is true iff
/// every partition is at its edge.
///
/// Note: `run_page` (below) no longer calls this function directly as of
/// Task 2 fix round 2 — its contiguous-selection cursor math (`pos ±
/// taken_count`) is simple enough to compute inline, and doing so avoids
/// re-deriving positions from a synthetic `windows` list. `advance` remains
/// here as tested, independent public API (nothing about it was wrong; it
/// just stopped being the right tool for `run_page`'s new algorithm).
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
/// `[lo, hi]` watermark range before they drive anything else, and
/// (fix round 3, N5) drops any partition that has no watermark entry at
/// all.
///
/// Fix round 1, C4 (clamping): a forged cursor, or a legitimate one whose
/// partition has since been trimmed by retention, can carry a position
/// outside today's watermarks (e.g. an offset below the new low
/// watermark). `page_windows` already clamps its internal `pos` when
/// computing a window's bounds — but a partition that ends up with no
/// window this page (already at its edge) needs its position left
/// unchanged so it can be recognized as at-edge on the *next* comparison
/// too; if that position was never clamped, it's the raw out-of-range
/// value, the edge check (`pos == lo`/`hi`) never matches it, and the same
/// non-advancing cursor gets re-encoded and handed back forever: identical
/// cursor, `exhausted: false`, nothing new, no error — a silent infinite
/// loop from the client's point of view. Clamping once, up front, before
/// `page_windows` or the cursor encoder ever see `positions`, makes
/// "already at the edge" actually equal `lo`/`hi`.
///
/// Fix round 3, N5 (dropping): a partition id with *no* watermark entry at
/// all isn't a clamping problem (there's nothing to clamp against) — it
/// means this partition doesn't exist in the topic today. Partition counts
/// only ever grow for a given topic, so this can only be a forged cursor or
/// one carried over from a different topic; there's no valid position to
/// fall back to, so it's dropped from the tracked positions entirely rather
/// than carried through unchanged. This matters beyond cosmetics: a
/// carried-through phantom partition has no watermark to compare against,
/// so `run_page`'s exhaustion check (which requires *every* tracked
/// partition to be at its edge) could never be satisfied for it — the
/// whole page's `exhausted` would be stuck at `false` forever, even once
/// every real partition genuinely finished.
fn clamp_positions(positions: &[(i32, i64)], watermarks: &[(i32, i64, i64)]) -> Vec<(i32, i64)> {
    positions
        .iter()
        .filter_map(|&(partition, pos)| {
            watermarks.iter().find(|(p, _, _)| *p == partition).map(|&(_, lo, hi)| (partition, pos.clamp(lo, hi)))
        })
        .collect()
}

/// The offset, within a partition's window `w`, that sits immediately
/// adjacent to the position this page started from — the very first record
/// `run_page`'s contiguous selection would consume from that partition.
///
/// For `Back`, the position is the window's *exclusive upper bound*
/// (`w.end`), so the adjacent record is the highest offset in the window,
/// `w.end - 1` — the *top* of the window. For `Forward`, the position is
/// the window's *inclusive lower bound* (`w.start`), so the adjacent record
/// is `w.start` itself — the *bottom*. This is what fix round 2's N3
/// short-read check (see `run_page`'s doc comment) tests for.
fn adjacent_offset(w: &PartitionRange, direction: Direction) -> i64 {
    match direction {
        Direction::Back => w.end - 1,
        Direction::Forward => w.start,
    }
}

/// True if `candidate` should be selected over `current_best` as the next
/// record taken in `run_page`'s k-way merge (see its doc comment): `Back`
/// prefers the higher timestamp, `Forward` the lower. Ties break by
/// partition ascending, then — per the design — offset in the direction's
/// own sense (`Back`: higher wins; `Forward`: lower wins). In practice a
/// partition-level tie can't arise within one merge step (each partition
/// contributes at most one candidate at a time, so equal partition already
/// means it's the same stream), but the offset tie-break is kept for a
/// fully deterministic order and to mirror the same convention used
/// elsewhere (e.g. the final display sort below).
fn merge_prefers(direction: Direction, candidate: &RawRecord, current_best: &RawRecord) -> bool {
    let (ct, bt) = (candidate.timestamp_ms.unwrap_or(i64::MIN), current_best.timestamp_ms.unwrap_or(i64::MIN));
    let ts_favors_candidate = match direction {
        Direction::Back => ct.cmp(&bt).is_gt(),
        Direction::Forward => ct.cmp(&bt).is_lt(),
    };
    if ct != bt {
        return ts_favors_candidate;
    }
    match candidate.partition.cmp(&current_best.partition) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Greater => false,
        std::cmp::Ordering::Equal => match direction {
            Direction::Back => candidate.offset > current_best.offset,
            Direction::Forward => candidate.offset < current_best.offset,
        },
    }
}

/// Fix round 3, N4 (refines round 2's N2/N3 guard): a page "made no
/// progress" only when nothing was taken, the scan wasn't cancelled, *and*
/// at least one window belonged to a partition that was **not** completely
/// scanned (a genuine short read — fetch's internal deadline or `cap`, not
/// a legitimate Kafka hole).
///
/// This is narrower than round 2's version, which fired on `windows
/// non-empty && taken empty` alone. That was wrong given N4: a partition
/// whose window is entirely legitimate holes (e.g. only a transaction
/// control record) is completely scanned, contributes zero taken records,
/// *and that's correct* — its position still advances to the window
/// boundary in `run_page` (nothing was missed, we just confirmed there was
/// nothing there). Firing this guard on that case would turn ordinary,
/// hole-only pages into spurious errors. The real failure mode this guards
/// against is unchanged from round 2: a short read (or a compaction/
/// retention race) leaving a partition's data genuinely unknown, which must
/// never be silently reported as "nothing here" nor as an unadvanced,
/// non-exhausted `page_end` (both would either lose data or loop the client
/// forever) — it must surface as a terminal `error` instead.
fn page_made_no_progress(taken_is_empty: bool, any_incomplete_window: bool, cancelled: bool) -> bool {
    taken_is_empty && any_incomplete_window && !cancelled
}

/// Runs one unfiltered timeline page (`filter: None` — the only path this
/// task implements; a filtered page is Task 4's job and reports an explicit
/// `error` rather than silently behaving like an unfiltered scan).
///
/// Fix round 2 replaced simple truncation with **contiguous selection**:
/// each partition can only ever contribute a *contiguous prefix* of its
/// window, walked strictly outward from the position, so a partition's own
/// progress and "how much of its window got used" can't come apart (this
/// is what killed round 1's N1 livelock). Fix round 3 (N4) extends that
/// model to real Kafka: a partition's window frequently has legitimate
/// offset holes with no message at all — a committed transaction's own
/// commit marker consumes a real offset (typically the one right below the
/// high watermark), as do aborted-transaction ranges and compacted
/// tombstones — and round 2 could not tell those apart from a genuine short
/// read (fetch's own deadline cutting a scan short), producing spurious
/// errors or `exhausted: false` on perfectly healthy, hole-having topics.
///
/// 1. Per partition, order its fetched window records *adjacent-first*:
///    `Back` sorts offset descending (nearest-to-position first, since the
///    position is the window's upper bound); `Forward` sorts offset
///    ascending (nearest-to-position first, since the position is the
///    window's lower bound — this is simply the records' natural fetch
///    order).
/// 2. **N4 completeness, not adjacency, gates trust**: `fetch_ranges_blocking`
///    now reports (`FetchOutcome::complete`) which partitions it scanned to
///    the *end of their requested range* (`PartitionEOF` or offset ≥ end),
///    as opposed to stopping early for an unrelated reason (its own
///    deadline, `cap`, cancellation). A **complete** partition's delivered
///    records are trusted as-is, holes and all — a missing offset there is
///    confirmed, not suspicious, since the whole range was scanned. Only
///    for an **incomplete** partition does round 2's original guard still
///    apply: if its fetched records don't include the position-adjacent
///    offset (`adjacent_offset`), the entire window is discarded for this
///    page (contributes nothing, position doesn't move) — because for an
///    incomplete partition we genuinely can't tell a hole from data that
///    just hasn't arrived yet.
/// 3. **k-way merge**: repeatedly compare the current head (next
///    unconsumed, adjacent-most record) of every partition's stream and
///    take the one `merge_prefers` (best timestamp for the direction; see
///    its doc comment for the tie-break), advancing only that partition's
///    stream — until `limit` records are taken or every stream is empty.
/// 4. **Cursor (N6: by offset, not count)**: a *complete* partition whose
///    stream ends this page fully drained — every delivered record was
///    taken, or it started empty (a window that was nothing but holes) —
///    jumps straight to the **window boundary** (`w.start`/`w.end`):
///    completeness already confirms the *entire* range, real records and
///    holes alike, has been accounted for, so there's nothing left to wait
///    for, including any trailing hole past the last real record (a
///    committed transaction's control record, say). Otherwise, a partition
///    with records taken this page advances only to `min(taken offsets)`
///    (`Back`) or `max(taken offsets) + 1` (`Forward`) — the position bound
///    is exclusive-upper for `Back`, so the lowest offset actually taken
///    *is* the new bound; inclusive-lower for `Forward`, so one past the
///    highest. This must be offset-exact, not a record *count*: with holes,
///    "4 records taken" can span a 5-offset range (one hole in it), and
///    subtracting the count would strand the cursor 1 offset short of where
///    it actually got to — reporting `exhausted: false` on a fully-drained
///    window and, worse, re-serving already-taken records on the next
///    page. A partition with nothing taken and *not* complete-and-drained
///    keeps its old position unchanged — the safe default, whether it lost
///    every merge comparison (real data still pending) or is incomplete
///    (unknown state).
/// 5. The taken set (already ≤ `limit`, already loss-free, already
///    overlap-free) is sorted into **display** order —
///    `(timestamp_ms, partition, offset)`, desc/asc to match `direction` —
///    before decoding and emitting; the merge's *selection* order can
///    differ from display order under non-monotonic timestamps, which is
///    exactly the page-boundary "fuzz" the design doc already licenses.
/// 6. **Progress guarantee** (`page_made_no_progress`, narrowed by N4): a
///    page that took nothing, wasn't cancelled, and had at least one
///    genuinely *incomplete* window (a true short read) reports a terminal
///    `error` rather than an unadvanced, non-exhausted `page_end`. A page
///    that took nothing only because every window was complete-and-holes
///    is *not* an error — see step 4(b).
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

        // C4/N5: clamp before anything else sees `positions` (see the doc
        // comment on `clamp_positions`).
        let positions = clamp_positions(&positions, &watermarks);

        type ScanResult = Result<(fetch::FetchOutcome, Vec<PartitionRange>), ApiError>;
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
                let outcome = fetch::fetch_ranges_blocking(&cfg, &topic, &windows, cap, &cancelled)?;
                Ok((outcome, windows))
            }).await
        };

        let (outcome, windows) = match result {
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
        let complete = outcome.complete;

        // Group fetched records by partition, ascending by offset (their
        // natural fetch order — `fetch_ranges_blocking` scans low-to-high
        // per partition regardless of `direction`; sorting explicitly here
        // is a cheap defensive guarantee, not a load-bearing assumption).
        let mut by_partition: HashMap<i32, Vec<RawRecord>> = HashMap::new();
        for r in outcome.records {
            by_partition.entry(r.partition).or_default().push(r);
        }
        for v in by_partition.values_mut() {
            v.sort_by_key(|r| r.offset);
        }

        // Step 1 + 2: build each partition's adjacent-first stream. `streams`
        // and `is_complete` are indexed in parallel with `windows` (minor: a
        // plain `Vec` here, not a `HashMap<i32, _>` keyed by partition,
        // avoids a lookup-then-`.expect()` chain in the merge loop below —
        // indices are always in-bounds by construction).
        let is_complete: Vec<bool> = windows.iter().map(|w| complete.contains(&w.partition)).collect();
        let mut streams: Vec<VecDeque<RawRecord>> = Vec::with_capacity(windows.len());
        for (w, &partition_complete) in windows.iter().zip(&is_complete) {
            let fetched = by_partition.remove(&w.partition).unwrap_or_default();
            // N4: a complete partition's holes (if any) are confirmed
            // legitimate — trust its delivered records as-is. Only an
            // incomplete partition still needs the N3 short-read guard:
            // without the position-adjacent offset, we can't tell a real
            // hole from data that simply hasn't arrived yet.
            let trust_as_is = partition_complete || fetched.iter().any(|r| r.offset == adjacent_offset(w, direction));
            let ordered: VecDeque<RawRecord> = if trust_as_is {
                match direction {
                    Direction::Back => fetched.into_iter().rev().collect(),
                    Direction::Forward => fetched.into_iter().collect(),
                }
            } else {
                VecDeque::new()
            };
            streams.push(ordered);
        }

        // Step 3: k-way merge, taking at most `limit` records total. `best`
        // carries the current-best candidate's reference along with its
        // index (minor: hoisted out of a HashMap re-lookup every iteration).
        let mut taken: Vec<RawRecord> = Vec::new();
        let mut taken_min: Vec<Option<i64>> = vec![None; windows.len()];
        let mut taken_max: Vec<Option<i64>> = vec![None; windows.len()];
        while taken.len() < limit {
            let mut best: Option<(usize, &RawRecord)> = None;
            for (i, stream) in streams.iter().enumerate() {
                let Some(candidate) = stream.front() else { continue };
                best = match best {
                    None => Some((i, candidate)),
                    Some((bi, current_best)) => {
                        if merge_prefers(direction, candidate, current_best) { Some((i, candidate)) } else { Some((bi, current_best)) }
                    }
                };
            }
            let Some((i, _)) = best else { break };
            let rec = streams[i].pop_front().expect("index i was just selected from a non-empty front() above");
            let offset = rec.offset;
            taken_min[i] = Some(taken_min[i].map_or(offset, |m| m.min(offset)));
            taken_max[i] = Some(taken_max[i].map_or(offset, |m| m.max(offset)));
            taken.push(rec);
        }

        // Step 4: N6 exact offset-based cursor math (see doc comment above).
        // A complete partition whose stream is now fully drained (every
        // delivered record taken, or it started empty — a window that was
        // nothing but holes) has had its *entire* window accounted for, real
        // records and holes alike, so it jumps straight to the window
        // boundary — not just to the edge of what got taken, which could
        // strand it just short of a trailing hole forever. A partition
        // that's complete but still has real records left in its stream
        // (the merge loop hit `limit` first) must NOT take that shortcut —
        // there's pending data it would skip past.
        let new_positions: Vec<(i32, i64)> = positions
            .iter()
            .map(|&(p, pos)| {
                let Some(i) = windows.iter().position(|w| w.partition == p) else { return (p, pos) };
                let w = &windows[i];
                if is_complete[i] && streams[i].is_empty() {
                    return (p, match direction { Direction::Back => w.start, Direction::Forward => w.end });
                }
                match direction {
                    Direction::Back => match taken_min[i] {
                        Some(min_offset) => (p, min_offset),
                        None => (p, pos),
                    },
                    Direction::Forward => match taken_max[i] {
                        Some(max_offset) => (p, max_offset + 1),
                        None => (p, pos),
                    },
                }
            })
            .collect();

        // Step 6: progress guarantee — before computing/sending anything
        // else, so a stalled page never reaches the normal page_end path.
        let any_incomplete_window = is_complete.iter().any(|&c| !c);
        if page_made_no_progress(taken.is_empty(), any_incomplete_window, cancelled.load(Ordering::SeqCst)) {
            let _ = tx.send(ApiError::kafka(
                &handle.name,
                "page made no progress: broker returned no records for non-empty windows",
            ).into()).await;
            return;
        }

        let exhausted = new_positions.iter().all(|&(p, pos)| {
            watermarks.iter().find(|(wp, _, _)| *wp == p).is_some_and(|&(_, lo, hi)| match direction {
                Direction::Back => pos == lo,
                Direction::Forward => pos == hi,
            })
        });
        let cursor = if exhausted { None } else { Some(Cursor { direction, positions: new_positions }.encode()) };

        // Step 5: display order, then decode only the taken (≤ limit) set.
        match direction {
            Direction::Back => taken.sort_by(|a, b| {
                b.timestamp_ms.unwrap_or(i64::MIN).cmp(&a.timestamp_ms.unwrap_or(i64::MIN))
                    .then(a.partition.cmp(&b.partition))
                    .then(b.offset.cmp(&a.offset))
            }),
            Direction::Forward => taken.sort_by(|a, b| {
                a.timestamp_ms.unwrap_or(i64::MIN).cmp(&b.timestamp_ms.unwrap_or(i64::MIN))
                    .then(a.partition.cmp(&b.partition))
                    .then(a.offset.cmp(&b.offset))
            }),
        }

        let decoded = fetch::to_message_out(taken, handle.schema_registry.as_deref()).await;
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

    /// Fix round 3, N5 (reviewer's exact construction): a cursor decoded to
    /// `[(0, 0), (99, 5)]` on a 1-partition topic (watermarks only cover
    /// partition 0) must behave as `[(0, 0)]` — partition 99 doesn't exist,
    /// so it's dropped rather than carried through with no watermark to
    /// clamp or exhaust it against.
    #[test]
    fn clamp_positions_drops_unknown_partitions() {
        let wm: &[(i32, i64, i64)] = &[(0, 0, 10)];
        let positions = vec![(0, 0), (99, 5)];
        assert_eq!(clamp_positions(&positions, wm), vec![(0, 0)]);
    }

    fn raw(partition: i32, offset: i64, ts: Option<i64>) -> RawRecord {
        RawRecord { partition, offset, timestamp_ms: ts, key: None, value: Some(b"x".to_vec()), headers: vec![] }
    }

    #[test]
    fn adjacent_offset_back_is_top_of_window() {
        let w = range::PartitionRange { partition: 0, start: 5, end: 10 };
        assert_eq!(adjacent_offset(&w, Direction::Back), 9);
    }

    #[test]
    fn adjacent_offset_forward_is_bottom_of_window() {
        let w = range::PartitionRange { partition: 0, start: 5, end: 10 };
        assert_eq!(adjacent_offset(&w, Direction::Forward), 5);
    }

    #[test]
    fn merge_prefers_back_picks_higher_timestamp() {
        let newer = raw(0, 1, Some(300));
        let older = raw(1, 1, Some(200));
        assert!(merge_prefers(Direction::Back, &newer, &older));
        assert!(!merge_prefers(Direction::Back, &older, &newer));
    }

    #[test]
    fn merge_prefers_forward_picks_lower_timestamp() {
        let older = raw(0, 1, Some(200));
        let newer = raw(1, 1, Some(300));
        assert!(merge_prefers(Direction::Forward, &older, &newer));
        assert!(!merge_prefers(Direction::Forward, &newer, &older));
    }

    #[test]
    fn merge_prefers_ties_break_by_offset_per_direction() {
        // Same partition, same timestamp: only one of these two records can
        // ever be a merge candidate at once in practice (see `merge_prefers`'s
        // doc comment), but the tie-break must still be well-defined.
        let higher_offset = raw(0, 5, Some(100));
        let lower_offset = raw(0, 3, Some(100));
        assert!(merge_prefers(Direction::Back, &higher_offset, &lower_offset), "Back prefers the higher offset on a tie");
        assert!(merge_prefers(Direction::Forward, &lower_offset, &higher_offset), "Forward prefers the lower offset on a tie");
    }

    /// Fix round 3, N4 (narrows round 2's N2/N3 guard): nothing taken, a
    /// genuinely incomplete (short-read) window, not cancelled ⇒ a real "no
    /// progress" condition that must surface as an error.
    #[test]
    fn page_made_no_progress_true_when_nothing_taken_and_a_window_is_incomplete() {
        assert!(page_made_no_progress(true, true, false));
    }

    #[test]
    fn page_made_no_progress_false_when_something_was_taken() {
        assert!(!page_made_no_progress(false, true, false));
    }

    /// N4's key new case: nothing taken, but every window was scanned to
    /// *completion* — e.g. a page whose only content was legitimate holes
    /// (transaction control records, compaction). That's confirmed, not
    /// suspicious, and must never be reported as an error.
    #[test]
    fn page_made_no_progress_false_when_every_window_is_complete() {
        assert!(!page_made_no_progress(true, false, false));
    }

    #[test]
    fn page_made_no_progress_false_when_cancelled() {
        // A client disconnect mid-scan can legitimately leave nothing taken;
        // that's not a broker anomaly and must not be reported as one.
        assert!(!page_made_no_progress(true, true, true));
    }
}
