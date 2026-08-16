use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::cluster::ClusterHandle;
use crate::error::ApiError;
use crate::message::fetch::{self, RawRecord};
use crate::message::filter::{self, Filter};
use crate::message::range::{self, PartitionRange};
use crate::message::MessageOut;

use super::cursor::{Cursor, Direction};
use super::event::TimelineEvent;
use super::merge::{chunk_display_order, merge_prefers};
use super::window::{adjacent_offset, at_edge, cap_windows_to_budget, clamp_positions, page_windows};

/// A chunk "made no progress" only when nothing was taken, the scan wasn't
/// cancelled, *and* at least one window belonged to a partition that was
/// **not** scanned to completion — a genuine short read (fetch's internal
/// deadline or `cap`), not a legitimate Kafka hole. That case leaves a
/// partition's data genuinely unknown, and must never be reported as
/// "nothing here" nor as an unadvanced, non-exhausted `page_end` (the first
/// loses data, the second loops the client forever): `run_page` turns it
/// into a terminal `error` instead.
///
/// The two narrowing conditions are what keep healthy topics quiet, and
/// both are load-bearing:
///
/// - **Nothing taken, by itself, is not an error.** A window that is
///   entirely legitimate holes (say, only a transaction control record) is
///   scanned to completion, contributes zero taken records, and is correct
///   — its position still advances to the window boundary in `run_page`,
///   because nothing was missed: the range was confirmed to hold no
///   messages. Firing here on that case would turn ordinary hole-only
///   chunks into spurious errors.
/// - **Cancellation is carved out.** A client disconnect mid-scan can
///   legitimately leave nothing taken; that's not a broker anomaly.
fn page_made_no_progress(taken_is_empty: bool, any_incomplete_window: bool, cancelled: bool) -> bool {
    taken_is_empty && any_incomplete_window && !cancelled
}

/// Per-partition span of one scan iteration ("chunk"): used by every chunk
/// of a filtered page, and by every chunk after the first on an unfiltered
/// one (whose first chunk uses `limit` instead — see `run_page`).
/// Deliberately much larger than a typical `limit` (100-500), because those
/// are exactly the cases that must keep looking past one window's worth of
/// records: hunting a sparse filter match, or crossing a hole-dominated
/// region. A chunk's job is to make real progress without round-tripping to
/// the broker for every few records.
const CHUNK_SPAN: u64 = 5_000;

/// How often (in records popped) a mid-chunk `progress` event goes out —
/// see `mid_chunk_progress`.
const PROGRESS_INTERVAL: u64 = 2_000;

/// Whether the `records_popped_this_chunk`-th record popped in the current
/// chunk is due a mid-chunk `progress` emission, and if so, the `scanned`
/// value to report: every `PROGRESS_INTERVAL` records, reporting
/// `total_scanned` (every earlier chunk's already-committed, offset-based
/// charge) plus how far *this* chunk has gotten so far.
///
/// Invariant: consecutive emissions within one chunk are strictly
/// increasing. That depends on `records_popped_this_chunk` being a running
/// count for the whole chunk (1-indexed, reset only when a new chunk
/// starts) — a counter reset after each emission would report
/// `total_scanned + PROGRESS_INTERVAL` every time, i.e. a progress bar that
/// freezes mid-chunk and jumps only at chunk boundaries.
fn mid_chunk_progress(total_scanned: u64, records_popped_this_chunk: u64) -> Option<u64> {
    (records_popped_this_chunk > 0 && records_popped_this_chunk.is_multiple_of(PROGRESS_INTERVAL))
        .then_some(total_scanned + records_popped_this_chunk)
}

/// Runs one timeline page — filtered or not, `filter: None` behaving as
/// match-everything (the design's "filter-off = match-all, same code
/// path"). Scans and decodes windows of `topic` starting from `positions`,
/// emitting `TimelineEvent`s over an mpsc channel until `limit` matches are
/// found, the topic edge is truly reached, or the per-request scan `budget`
/// is spent. `watermarks` and `positions` are resolved by the caller
/// (`api::messages`); nothing here re-reads them mid-page.
///
/// A page is a loop of **chunks**. One chunk scans one window of up to
/// `CHUNK_SPAN` records per still-active partition — except the first chunk
/// of an *unfiltered* page, which uses at most `limit`, so an ordinary
/// "give me the latest 100" costs one small fetch instead of 5,000 records
/// per partition on the off chance a hole is nearby. Every span is
/// additionally capped by that partition's share of the remaining budget,
/// and the chunk's windows as a set are capped to the remaining budget
/// outright (`cap_windows_to_budget`), so a chunk can never overspend it.
/// One chunk does the following.
///
/// 1. **Adjacent-first ordering.** Each partition's fetched records are
///    ordered nearest-to-position first: `Back` descending by offset (the
///    position is the window's exclusive upper bound), `Forward` ascending
///    (the position is its inclusive lower bound — also the records'
///    natural fetch order). Because the merge below always pops from the
///    front, a partition can only ever contribute a *contiguous prefix* of
///    its delivered records, walked strictly outward from the position.
///    That is what keeps a partition's own progress and "how much of its
///    window got used" from coming apart, which the cursor math in step 4
///    relies on.
/// 2. **Completeness, not adjacency, gates trust.** Kafka ranges
///    legitimately contain offsets carrying no message at all: a committed
///    transaction's commit marker consumes a real offset (typically the one
///    just below the high watermark), as do aborted-transaction ranges and
///    compacted tombstones. So a missing offset is only meaningful together
///    with how far the fetch actually got. `fetch_ranges_blocking` reports
///    (`FetchOutcome::complete`) which partitions it scanned to the *end of
///    their requested range* (`PartitionEOF` or offset ≥ end) rather than
///    stopping early for its own deadline, `cap`, or cancellation. A
///    **complete** partition's records are trusted as-is, holes and all —
///    the whole range was scanned, so a gap is confirmed, not suspicious.
///    For an **incomplete** partition a hole is indistinguishable from data
///    that simply hasn't arrived, so the short-read guard applies: unless
///    its records include the position-adjacent offset (`adjacent_offset`),
///    the entire window is discarded for this chunk — it contributes
///    nothing and its position does not move.
/// 3. **k-way merge, decode as you go.** Repeatedly compare the head (next
///    unconsumed, adjacent-most record) of every partition's stream and take
///    the one `merge_prefers` picks — best timestamp for the direction, see
///    its doc comment for the tie-break — decoding and filtering that record
///    immediately. Decoding record-by-record rather than batch-at-the-end is
///    what lets the loop's stopping condition depend on the filter's
///    verdict. Every popped record advances that partition's cursor
///    bookkeeping whether it matched or not; only matches are queued for
///    emission. The loop stops once accumulated matches (this chunk's plus
///    every earlier chunk's in this request) reach `limit`, or every stream
///    is empty.
/// 4. **Cursor advance is offset-exact, never a record count.** A
///    *complete* partition whose stream ends the chunk fully drained —
///    every delivered record taken, or it started empty because the window
///    was nothing but holes — jumps straight to the **window boundary**
///    (`w.start` for `Back`, `w.end` for `Forward`): completeness confirms
///    the entire range is accounted for, real records and holes alike, so
///    there is nothing left to wait for, including a trailing hole past the
///    last real record. A partition that is complete but still has records
///    left in its stream (the merge hit the match target first) must NOT
///    take that shortcut — it would skip pending data. Otherwise, a
///    partition with records taken this chunk advances to
///    `min(taken offsets)` for `Back` (the position is an exclusive upper
///    bound, so the lowest offset actually taken *is* the new bound) or
///    `max(taken offsets) + 1` for `Forward` (inclusive lower bound, so one
///    past the highest). Counting records instead of using offsets would
///    break on holes: "4 records taken" can span 5 offsets, and the cursor
///    would land 1 offset short of where the scan really got — reporting
///    `exhausted: false` on a fully drained window and re-serving
///    already-taken records on the next page. A partition with nothing
///    taken and not complete-and-drained keeps its old position: the safe
///    default, whether it lost every merge comparison (real data still
///    pending) or was incomplete (unknown state).
///
///    The **scan budget** is charged by the exact offset span each
///    partition's position advanced this chunk (`old.abs_diff(new)`,
///    summed) — not by matches, not even by delivered records — so that a
///    chunk whose window is all holes (zero records delivered) still spends
///    budget proportional to the ground it covered. Otherwise an unfiltered
///    page could cross an unbounded hole region for free, defeating the
///    point of a *scan* budget.
/// 5. **Display order, then emit.** Each chunk's matches (already ≤ the
///    remaining `limit`, already loss-free and overlap-free by steps 1-4)
///    go through `chunk_display_order` before being sent: a k-way merge per
///    spec v1.2's Out-of-order policy — within one partition, offset order
///    ALWAYS; across partitions, merge by timestamp (ties: smaller
///    partition id; null ts = `i64::MIN`). Unlike a pure-timestamp sort,
///    this cannot invert a partition's own offset order under non-monotonic
///    producer timestamps. Ordering holds *within* a chunk only; across
///    chunks it is as good as each chunk's own merge — the same
///    page-boundary "fuzz" the design doc licenses, applying equally to one
///    request's chunks.
/// 6. **Progress guarantee** (`page_made_no_progress`): a chunk that took
///    nothing, wasn't cancelled, and had at least one genuinely *incomplete*
///    window reports a terminal `error` instead of an unadvanced,
///    non-exhausted `page_end`. Taking nothing because every window was
///    complete-and-holes is not an error — the positions advance past the
///    holes and the outer loop tries another chunk.
///
/// The **outer loop** keeps running chunks until accumulated matches reach
/// `limit`; the scan budget is spent; or every partition has truly reached
/// its low/high watermark edge in `direction`. Only that last case ends the
/// page with `exhausted: true` and no cursor — it is the sole end-of-data
/// signal. The other two end with `exhausted: false` and a cursor to resume
/// from, and a page ending that way may legitimately carry ZERO matches
/// (budget spent crossing holes or non-matching records): an empty page is
/// never a silent stop, and the client continues from the returned cursor.
/// Chunking is what lets a hole-dominated or match-sparse region — filtered
/// or not — be crossed within one request instead of one near-empty page
/// per `limit` offsets.
///
/// Failures never leave the page half-reported: a fetch error, a
/// `spawn_blocking` join failure, or the no-progress condition all go out as
/// a terminal `TimelineEvent::Error` and end the stream *without* a
/// `page_end`, so a client can tell "this page finished" from "this page
/// broke". A client disconnect ends the task silently instead — there is
/// nobody left to tell.
#[allow(clippy::too_many_arguments)] // one request's worth of parameters, six of eight are the request itself
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

    tokio::spawn(async move {
        // Harden the caller's positions before anything else sees them —
        // they may come from a client-minted cursor (see the doc comment on
        // `clamp_positions`).
        let mut cur_positions = clamp_positions(&positions, &watermarks);
        let limit_u64 = limit as u64;
        let budget = budget.max(1);

        let mut total_matches: u64 = 0;
        let mut total_scanned: u64 = 0;
        let mut chunk_index: u64 = 0;
        let mut exhausted = false;

        type ScanResult = Result<(fetch::FetchOutcome, Vec<PartitionRange>), ApiError>;

        'chunks: loop {
            if total_matches >= limit_u64 || total_scanned >= budget {
                break;
            }
            // Stop on client disconnect (`CancelOnDrop` sets this flag). A
            // cancelled fetch always reports zero progress (see
            // `page_made_no_progress`'s cancellation carve-out), so
            // positions, budget, and `exhausted` all stay frozen — without
            // this check the loop would spin forever, minting a fresh
            // `BaseConsumer` every pass. No zombie scans.
            if cancelled.load(Ordering::SeqCst) {
                return;
            }

            let remaining_budget = budget - total_scanned;
            // Only partitions that would actually get a window this chunk
            // (not already sitting at their edge in `direction`) compete
            // for a share of the remaining budget — an edge partition
            // contributes nothing and must not dilute everyone else's
            // share.
            let active_partitions = cur_positions
                .iter()
                .filter(|&&(p, pos)| {
                    watermarks.iter().find(|(wp, _, _)| *wp == p).is_some_and(|&(_, lo, hi)| !at_edge(pos, lo, hi, direction))
                })
                .count() as u64;
            let active_partitions = active_partitions.max(1);
            let per_partition_share = (remaining_budget / active_partitions).max(1);
            let base_span = if filter.is_none() && chunk_index == 0 { limit_u64 } else { CHUNK_SPAN };
            let span = base_span.min(per_partition_share).max(1);

            let windows = page_windows(&cur_positions, &watermarks, direction, span);
            // Belt-and-braces against `per_partition_share`'s `.max(1)`
            // floor: even after the per-partition span cap above, several
            // partitions each getting a floored span of 1 can still add up
            // to more than what's actually left — cap the chunk's total
            // charge to the hard budget ceiling directly (see doc comment
            // on `cap_windows_to_budget`).
            let windows = cap_windows_to_budget(windows, remaining_budget);
            if windows.is_empty() {
                // No partition has anything left in `direction`: the true
                // topic edge, independent of the budget.
                exhausted = true;
                break;
            }

            let result: ScanResult = {
                let cfg = handle.config.clone();
                let topic = topic.clone();
                let windows = windows.clone();
                // `run_page`'s own `cancelled` flag (checked by
                // `CancelOnDrop` on client disconnect) reaches the blocking
                // scan loop, so a dropped SSE stream stops the in-flight
                // Kafka poll loop instead of only ever toggling a flag
                // nothing reads.
                let cancelled = cancelled.clone();
                tokio::task::spawn_blocking(move || -> ScanResult {
                    let cap = range::total(&windows) as usize;
                    let outcome = fetch::fetch_ranges_blocking(&cfg, &topic, &windows, cap, &cancelled)?;
                    Ok((outcome, windows))
                }).await
                .unwrap_or_else(|join_err| Err(ApiError::task_join(join_err)))
            };

            let (outcome, windows) = match result {
                Ok(pair) => pair,
                Err(e) => {
                    let _ = tx.send(e.into()).await;
                    return;
                }
            };
            let complete = outcome.complete;

            // Group fetched records by partition, ascending by offset
            // (their natural fetch order — `fetch_ranges_blocking` scans
            // low-to-high per partition regardless of `direction`; sorting
            // explicitly here is a cheap defensive guarantee, not a
            // load-bearing assumption).
            let mut by_partition: HashMap<i32, Vec<RawRecord>> = HashMap::new();
            for r in outcome.records {
                by_partition.entry(r.partition).or_default().push(r);
            }
            for v in by_partition.values_mut() {
                v.sort_by_key(|r| r.offset);
            }

            // Step 1 + 2: build each partition's adjacent-first stream.
            // `streams` and `is_complete` are indexed in parallel with
            // `windows` (minor: a plain `Vec` here, not a `HashMap<i32, _>`
            // keyed by partition, avoids a lookup-then-`.expect()` chain in
            // the merge loop below — indices are always in-bounds by
            // construction).
            let is_complete: Vec<bool> = windows.iter().map(|w| complete.contains(&w.partition)).collect();
            let mut streams: Vec<VecDeque<RawRecord>> = Vec::with_capacity(windows.len());
            for (w, &partition_complete) in windows.iter().zip(&is_complete) {
                let fetched = by_partition.remove(&w.partition).unwrap_or_default();
                // A complete partition's holes (if any) are confirmed
                // legitimate — trust its delivered records as-is. Only an
                // incomplete partition needs the short-read guard: without
                // the position-adjacent offset, a real hole is
                // indistinguishable from data that hasn't arrived yet.
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

            // Step 3: k-way merge with per-record decode+filter, taking
            // until this chunk's contribution plus every earlier chunk's
            // matches reach `limit`, or every stream is empty.
            let mut taken_min: Vec<Option<i64>> = vec![None; windows.len()];
            let mut taken_max: Vec<Option<i64>> = vec![None; windows.len()];
            let mut any_taken = false;
            let mut chunk_matches: Vec<MessageOut> = Vec::new();
            let mut records_popped_this_chunk: u64 = 0;
            loop {
                if total_matches + chunk_matches.len() as u64 >= limit_u64 {
                    break;
                }
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
                any_taken = true;

                let decoded = fetch::to_one_message_out(rec, handle.schema_registry.as_deref()).await;
                let is_match = filter.as_ref().is_none_or(|f| filter::matches(f, &decoded));
                if is_match {
                    chunk_matches.push(decoded);
                }

                // Progress guarantee (design doc): at least every
                // PROGRESS_INTERVAL scanned, a `progress` event goes out
                // mid-chunk, not just at chunk boundaries — a single chunk
                // can span thousands of records (`CHUNK_SPAN`), and a
                // filtered scan hunting a sparse needle must not go quiet
                // for that long. `mid_chunk_progress` reports a running
                // total (see its own doc comment) so consecutive emissions
                // within one chunk are never the same number.
                records_popped_this_chunk += 1;
                if let Some(scanned) = mid_chunk_progress(total_scanned, records_popped_this_chunk) {
                    // A failed send means the client is gone — stop right
                    // here rather than ignoring the error and scanning on;
                    // this is the mid-chunk half of the disconnect
                    // handling, alongside the top-of-loop flag check.
                    if tx.send(TimelineEvent::Progress {
                        scanned,
                        matches: total_matches + chunk_matches.len() as u64,
                        budget,
                    }).await.is_err() {
                        return;
                    }
                }
            }

            // Step 4: exact offset-based cursor math — window boundary for
            // a complete-and-drained partition, otherwise the extreme
            // offset actually taken, otherwise unchanged. Each branch's
            // reasoning is in `run_page`'s doc comment, step 4; the trap to
            // remember while editing here is that `is_complete[i] &&
            // streams[i].is_empty()` is the ONLY case allowed to skip past
            // offsets no record was taken from.
            let new_positions: Vec<(i32, i64)> = cur_positions
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

            // Step 6: progress guarantee — before committing this chunk's
            // results, so a stalled chunk never reaches the normal
            // page_end path.
            let any_incomplete_window = is_complete.iter().any(|&c| !c);
            if page_made_no_progress(!any_taken, any_incomplete_window, cancelled.load(Ordering::SeqCst)) {
                let _ = tx.send(ApiError::kafka(
                    &handle.name,
                    "page made no progress: broker returned no records for non-empty windows",
                ).into()).await;
                return;
            }

            // Charge the budget by the exact offset span each partition
            // advanced this chunk — holes included (see doc comment above).
            let chunk_scanned: u64 = cur_positions
                .iter()
                .zip(&new_positions)
                .map(|(&(_, old), &(_, new))| old.abs_diff(new))
                .sum();
            total_scanned += chunk_scanned;
            total_matches += chunk_matches.len() as u64;

            exhausted = new_positions.iter().all(|&(p, pos)| {
                watermarks.iter().find(|(wp, _, _)| *wp == p).is_some_and(|&(_, lo, hi)| at_edge(pos, lo, hi, direction))
            });
            cur_positions = new_positions;
            chunk_index += 1;

            // Step 5: display order, then emit — one chunk at a time.
            let chunk_matches = chunk_display_order(chunk_matches, direction);
            for m in chunk_matches {
                if tx.send(TimelineEvent::Match(Box::new(m))).await.is_err() {
                    return; // client disconnected: no point sending more
                }
            }
            if tx.send(TimelineEvent::Progress { scanned: total_scanned, matches: total_matches, budget }).await.is_err() {
                return; // client disconnected — stop, don't scan on regardless
            }

            if exhausted {
                break 'chunks;
            }
        }

        let cursor = if exhausted { None } else { Some(Cursor { direction, positions: cur_positions }.encode()) };
        let _ = tx.send(TimelineEvent::PageEnd { cursor, exhausted }).await;
    });

    (rx, cancel)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two mid-chunk progress emissions in the SAME chunk must report
    /// strictly increasing `scanned` values. The trap this pins: a counter
    /// reset to 0 after each emission makes both emits report
    /// `total_scanned + 2000` — an identical number, i.e. a progress bar
    /// that visibly freezes mid-chunk.
    #[test]
    fn mid_chunk_progress_fires_every_2000_and_never_repeats_within_a_chunk() {
        assert_eq!(mid_chunk_progress(0, 1_999), None);
        let first = mid_chunk_progress(0, 2_000);
        assert_eq!(first, Some(2_000));
        assert_eq!(mid_chunk_progress(0, 3_999), None);
        let second = mid_chunk_progress(0, 4_000);
        assert_eq!(second, Some(4_000));
        assert_ne!(first, second, "consecutive mid-chunk emissions in one chunk must not repeat the same scanned value");
    }

    #[test]
    fn mid_chunk_progress_scanned_includes_every_earlier_chunks_charge() {
        // `total_scanned` (prior chunks' real, offset-based charge) plus how
        // far this chunk has gotten so far — not just this chunk's count.
        assert_eq!(mid_chunk_progress(10_000, 2_000), Some(12_000));
    }

    /// Nothing taken, a genuinely incomplete (short-read) window, not
    /// cancelled ⇒ a real "no progress" condition that must surface as an
    /// error.
    #[test]
    fn page_made_no_progress_true_when_nothing_taken_and_a_window_is_incomplete() {
        assert!(page_made_no_progress(true, true, false));
    }

    #[test]
    fn page_made_no_progress_false_when_something_was_taken() {
        assert!(!page_made_no_progress(false, true, false));
    }

    /// The case that keeps healthy topics quiet: nothing taken, but every
    /// window was scanned to *completion* — e.g. a chunk whose only content
    /// was legitimate holes (transaction control records, compaction).
    /// That's confirmed, not suspicious, and must never be an error.
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
