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

/// Per-partition span of one scan iteration ("chunk") once a page needs to
/// keep looking past its first window — either because it's hunting for
/// filter matches, or (this task's amendment) crossing a hole-dominated
/// region on an unfiltered page. Deliberately much larger than a typical
/// `limit` (100-500): a chunk's job is to make real progress against
/// sparse matches/holes without round-tripping to the broker for every few
/// records.
const CHUNK_SPAN: u64 = 5_000;

/// How often (in records popped) a mid-chunk `progress` event goes out —
/// see `mid_chunk_progress`.
const PROGRESS_INTERVAL: u64 = 2_000;

/// Whether the `records_popped_this_chunk`-th record popped in the current
/// chunk (a plain running count, 1-indexed, reset only when a new chunk
/// starts) is due a mid-chunk `progress` emission, and if so, the `scanned`
/// value to report.
///
/// B1 fix: fires every `PROGRESS_INTERVAL` records, reporting
/// `total_scanned` (every earlier chunk's already-committed, offset-based
/// charge) plus how far *this* chunk has gotten so far. Critically,
/// `records_popped_this_chunk` is cumulative for the whole chunk and is
/// never reset between calls — the pre-fix version reset its counter to 0
/// after every emission, so the first emit in a chunk reported
/// `total_scanned + PROGRESS_INTERVAL` and the second, `PROGRESS_INTERVAL`
/// records later in that SAME chunk, reported `total_scanned +
/// PROGRESS_INTERVAL` again: an identical number, i.e. a progress bar that
/// visibly freezes then jumps at chunk boundaries. Reporting the running
/// total instead makes every mid-chunk emission strictly greater than the
/// last.
fn mid_chunk_progress(total_scanned: u64, records_popped_this_chunk: u64) -> Option<u64> {
    (records_popped_this_chunk > 0 && records_popped_this_chunk.is_multiple_of(PROGRESS_INTERVAL))
        .then_some(total_scanned + records_popped_this_chunk)
}

/// Runs one timeline page — filtered or not, `filter: None` behaving as
/// match-everything (the design's "filter-off = match-all, same code
/// path"). Fetches fresh-enough windows, scans and decodes them, and emits
/// `TimelineEvent`s over an mpsc channel until `limit` matches are found,
/// the topic edge is truly reached, or the per-request scan `budget` is
/// spent.
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
/// Task 3 turns the single-window pass into a **chunked scan loop**: each
/// iteration below is a "chunk" — one window of up to `CHUNK_SPAN` records
/// per partition (the very first chunk of an *unfiltered* page instead uses
/// exactly `limit`, matching this engine's original one-shot behavior and
/// its performance: an ordinary "give me the latest 100" page must not
/// suddenly fetch 5,000 records per partition just in case there's a hole
/// nearby). One chunk:
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
///    chunk (contributes nothing, position doesn't move) — because for an
///    incomplete partition we genuinely can't tell a hole from data that
///    just hasn't arrived yet.
/// 3. **k-way merge, decode-as-you-go**: repeatedly compare the current head
///    (next unconsumed, adjacent-most record) of every partition's stream
///    and take the one `merge_prefers` (best timestamp for the direction;
///    see its doc comment for the tie-break), decoding and filtering it
///    immediately — this record-by-record decode (as opposed to decoding
///    the whole taken batch at the end, task 2's shape) is what lets the
///    loop's stopping condition depend on the filter's verdict. Every
///    popped record (matching or not) still advances that partition's
///    cursor bookkeeping below; only matches are queued for emission. The
///    inner loop stops once accumulated matches (this chunk's plus every
///    earlier chunk's, this request) reach `limit`, or every stream is
///    empty.
/// 4. **Cursor (N6: by offset, not count)**: a *complete* partition whose
///    stream ends this chunk fully drained — every delivered record was
///    taken, or it started empty (a window that was nothing but holes) —
///    jumps straight to the **window boundary** (`w.start`/`w.end`):
///    completeness already confirms the *entire* range, real records and
///    holes alike, has been accounted for, so there's nothing left to wait
///    for, including any trailing hole past the last real record (a
///    committed transaction's control record, say). Otherwise, a partition
///    with records taken this chunk advances only to `min(taken offsets)`
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
///    (unknown state). The **scan budget** is charged by the exact offset
///    span each partition's position advanced this chunk (`old.abs_diff(new)`,
///    summed) — not by matches or even by delivered records — precisely so
///    that a chunk whose window is nothing but holes (zero delivered
///    records) still spends budget proportional to the ground it covered;
///    otherwise an unfiltered page could cross an unbounded hole region for
///    free, defeating the whole point of a *scan* budget.
/// 5. Each chunk's matches (already ≤ remaining `limit`, already loss-free,
///    already overlap-free) are put into **display** order by
///    `chunk_display_order` — a k-way merge per spec v1.2's Out-of-order
///    policy: within one partition, offset order ALWAYS; across partitions,
///    merge by timestamp (ties: smaller partition id; null ts = i64::MIN) —
///    and emitted before moving to the next chunk. This cannot invert a
///    partition's own offset order the way a pure-timestamp sort could
///    under non-monotonic producer timestamps. Chunk-to-chunk, the display
///    order is only as good as each chunk's own merge — that's exactly the
///    page-boundary "fuzz" the design doc already licenses, now also
///    licensed *within* one request's chunks.
/// 6. **Progress guarantee** (`page_made_no_progress`, narrowed by N4): a
///    chunk that took nothing, wasn't cancelled, and had at least one
///    genuinely *incomplete* window (a true short read) reports a terminal
///    `error` rather than an unadvanced, non-exhausted `page_end`. A chunk
///    that took nothing only because every window was complete-and-holes
///    is *not* an error — it simply advances past the hole and (step 7)
///    the outer loop tries another chunk.
///
/// The **outer loop** keeps running chunks until: accumulated matches reach
/// `limit`; the scan budget is spent (`page_end` reports `exhausted:
/// false`, never a silent stop — the client can request another page from
/// the returned cursor); or every partition has truly reached its
/// low/high watermark edge (`exhausted: true`, the only end-of-data
/// signal). This is what makes a hole-dominated region — filtered or not —
/// get crossed within one request instead of one near-empty page per
/// `limit` offsets.
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
        // C4/N5: clamp before anything else sees `positions` (see the doc
        // comment on `clamp_positions`).
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
            // C1: a client disconnect (`CancelOnDrop`) sets this flag but a
            // cancelled fetch always reports zero progress (see
            // `page_made_no_progress`'s cancellation carve-out) — positions,
            // budget, and `exhausted` all stay frozen, so without this check
            // the loop below would spin forever, minting a fresh
            // `BaseConsumer` every pass instead of stopping.
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
                // N4: a complete partition's holes (if any) are confirmed
                // legitimate — trust its delivered records as-is. Only an
                // incomplete partition still needs the N3 short-read guard:
                // without the position-adjacent offset, we can't tell a
                // real hole from data that simply hasn't arrived yet.
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
                // total (see its own doc comment, B1 fix) so consecutive
                // emissions within one chunk are never the same number.
                records_popped_this_chunk += 1;
                if let Some(scanned) = mid_chunk_progress(total_scanned, records_popped_this_chunk) {
                    // C1: a failed send means the client is gone — stop
                    // right here instead of ignoring the error and looping
                    // on regardless (the belt-and-braces half of the fix,
                    // alongside the top-of-loop cancellation check above).
                    if tx.send(TimelineEvent::Progress {
                        scanned,
                        matches: total_matches + chunk_matches.len() as u64,
                        budget,
                    }).await.is_err() {
                        return;
                    }
                }
            }

            // Step 4: N6 exact offset-based cursor math (see doc comment
            // above). A complete partition whose stream is now fully
            // drained (every delivered record taken, or it started empty —
            // a window that was nothing but holes) has had its *entire*
            // window accounted for, real records and holes alike, so it
            // jumps straight to the window boundary — not just to the edge
            // of what got taken, which could strand it just short of a
            // trailing hole forever. A partition that's complete but still
            // has real records left in its stream (the merge loop hit the
            // match target first) must NOT take that shortcut — there's
            // pending data it would skip past.
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
                return; // C1: client disconnected — stop, don't loop on regardless
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

    /// B1 regression: two mid-chunk progress emissions in the SAME chunk
    /// must report strictly increasing `scanned` values. The pre-fix
    /// arithmetic reset its counter to 0 after every emission, so the first
    /// emit reported `total_scanned + 2000` and the second (2000 records
    /// later, same chunk) reported `total_scanned + 2000` again — an
    /// identical number, i.e. a progress bar that visibly freezes.
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
