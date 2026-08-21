use std::collections::{HashMap, HashSet, VecDeque};
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

/// A chunk made genuinely no progress only when nothing was taken, the scan
/// wasn't cancelled, and some window was left incomplete by
/// `fetch_ranges_blocking`'s own deadline/cap — never when every window was
/// merely complete-and-holes (a confirmed-empty range is not an error) or
/// the scan was cancelled (a client disconnect is not a broker anomaly).
fn page_made_no_progress(taken_is_empty: bool, any_incomplete_window: bool, cancelled: bool) -> bool {
    taken_is_empty && any_incomplete_window && !cancelled
}

/// The user-visible text for a stalled chunk. This is OUR OWN read
/// deadline giving up (`fetch_ranges_blocking`'s wall-clock cap,
/// `FETCH_DEADLINE` — see `fetch.rs`) leaving a window incomplete, not the
/// broker going silent — attributing that to Kafka would blame it for a
/// limit this service itself imposes. Product voice: still no
/// "page"/"window"/"chunk" internals on the wire.
fn no_progress_message() -> &'static str {
    "the cluster didn't return new records within the fetch deadline"
}

fn no_progress_error(cluster: &str) -> ApiError {
    ApiError::fetch_deadline(cluster, no_progress_message())
}

/// Per-partition span of one scan iteration ("chunk"): used by every chunk
/// of a filtered page, and by every chunk after the first on an unfiltered
/// one (whose first chunk uses `limit` instead — see `chunk_span`).
/// Deliberately much larger than a typical `limit` (100-500), so a chunk
/// hunting a sparse filter match or crossing a hole-dominated region makes
/// real progress without round-tripping to the broker for every few
/// records.
const CHUNK_SPAN: u64 = 5_000;

/// How often (in records popped) a mid-chunk `progress` event goes out —
/// see `mid_chunk_progress`.
const PROGRESS_INTERVAL: u64 = 2_000;

/// The `scanned` value due on a mid-chunk `Progress` emission, every
/// `PROGRESS_INTERVAL` records popped in the current chunk: `total_scanned`
/// (every earlier chunk's already-committed charge) plus how far *this*
/// chunk has gotten so far; `None` in between. `records_popped_this_chunk`
/// must be a running count for the whole chunk (reset only when a new
/// chunk starts), or consecutive emissions within one chunk would report
/// the same number — a progress bar that freezes mid-chunk.
fn mid_chunk_progress(total_scanned: u64, records_popped_this_chunk: u64) -> Option<u64> {
    (records_popped_this_chunk > 0 && records_popped_this_chunk.is_multiple_of(PROGRESS_INTERVAL))
        .then_some(total_scanned + records_popped_this_chunk)
}

/// One page request's parameters, bundled so `run_page` takes a single named
/// struct instead of a long positional argument list. `handle` and `topic`
/// stay separate arguments to `run_page` (identify *which* cluster/topic;
/// everything here is *how* to page it).
pub struct PageRequest {
    pub positions: Vec<(i32, i64)>,
    pub watermarks: Vec<(i32, i64, i64)>,
    pub direction: Direction,
    pub limit: usize,
    pub filter: Option<Filter>,
    pub budget: u64,
}

/// The record span this chunk may request per partition: `limit` for an
/// unfiltered page's very first chunk (so an ordinary "give me the latest
/// 100" costs one small fetch instead of `CHUNK_SPAN` records per partition
/// on the off chance a hole is nearby), `CHUNK_SPAN` otherwise — capped to
/// this chunk's share of the remaining budget, split only among partitions
/// still active in `direction` (a partition already at its edge contributes
/// no window and must not dilute everyone else's share).
fn chunk_span(
    filter: &Option<Filter>,
    chunk_index: u64,
    limit_u64: u64,
    remaining_budget: u64,
    positions: &[(i32, i64)],
    watermarks: &[(i32, i64, i64)],
    direction: Direction,
) -> u64 {
    let active_partitions = positions
        .iter()
        .filter(|&&(p, pos)| watermarks.iter().find(|(wp, _, _)| *wp == p).is_some_and(|&(_, lo, hi)| !at_edge(pos, lo, hi, direction)))
        .count() as u64;
    let per_partition_share = (remaining_budget / active_partitions.max(1)).max(1);
    let base_span = if filter.is_none() && chunk_index == 0 { limit_u64 } else { CHUNK_SPAN };
    base_span.min(per_partition_share).max(1)
}

/// This chunk's windows: `span`-sized per still-active partition, then
/// capped as a *set* to `remaining_budget` outright — belt-and-braces
/// against `chunk_span`'s own `.max(1)` floor letting several partitions'
/// floored spans add up to more than what's actually left (see
/// `cap_windows_to_budget`'s doc comment). An empty result means no
/// partition has anything left in `direction`: the true topic edge,
/// independent of the budget.
fn plan_chunk_windows(
    positions: &[(i32, i64)],
    watermarks: &[(i32, i64, i64)],
    direction: Direction,
    span: u64,
    remaining_budget: u64,
) -> Vec<PartitionRange> {
    let windows = page_windows(positions, watermarks, direction, span);
    cap_windows_to_budget(windows, remaining_budget)
}

/// Runs one chunk's blocking Kafka scan over `windows` on the blocking
/// pool. `run_page`'s own `cancelled` flag (set by `CancelOnDrop` on client
/// disconnect) reaches the blocking scan loop, so a dropped SSE stream
/// stops the in-flight Kafka poll loop instead of only ever toggling a flag
/// nothing reads. A `spawn_blocking` join failure surfaces as an `ApiError`
/// like any other fetch failure.
async fn scan_chunk(
    handle: &ClusterHandle,
    topic: &str,
    windows: Vec<PartitionRange>,
    cancelled: &Arc<AtomicBool>,
) -> Result<(fetch::FetchOutcome, Vec<PartitionRange>), ApiError> {
    let cfg = handle.config.clone();
    let topic = topic.to_string();
    let task_windows = windows.clone();
    let task_cancelled = cancelled.clone();
    let outcome = tokio::task::spawn_blocking(move || -> Result<fetch::FetchOutcome, ApiError> {
        let cap = range::total(&task_windows) as usize;
        fetch::fetch_ranges_blocking(&cfg, &topic, &task_windows, cap, &task_cancelled)
    })
    .await
    .unwrap_or_else(|join_err| Err(ApiError::task_join(join_err)))?;
    Ok((outcome, windows))
}

/// Builds each partition's adjacent-first, trust-gated record stream from
/// one chunk's raw fetch. Adjacent-first: nearest-to-position first (`Back`
/// descending by offset — the position is the window's exclusive upper
/// bound — `Forward` ascending), so that the merge below, which always pops
/// from the front, can only ever consume a *contiguous prefix* of a
/// partition's window, walked strictly outward from the position.
/// Trust-gated: a **complete** partition's records are trusted as-is, holes
/// and all — the whole range was scanned, so a gap is a confirmed hole
/// (transaction markers, compacted tombstones), not suspicious. An
/// **incomplete** partition's hole is indistinguishable from data that
/// simply hasn't arrived, so its window is trusted only if its records
/// include the position-adjacent offset (`adjacent_offset`); otherwise the
/// entire window is discarded for this chunk. Streams and completeness are
/// indexed in parallel with `windows` (a plain `Vec`, not a partition-keyed
/// map, so the merge loop never needs a lookup-then-`.expect()` chain).
fn select_partition_streams(
    records: Vec<RawRecord>,
    windows: &[PartitionRange],
    complete: &HashSet<i32>,
    direction: Direction,
) -> (Vec<VecDeque<RawRecord>>, Vec<bool>) {
    // Group by partition, ascending by offset (their natural fetch order —
    // `fetch_ranges_blocking` scans low-to-high regardless of `direction`;
    // sorting explicitly here is a cheap defensive guarantee, not a
    // load-bearing assumption).
    let mut by_partition: HashMap<i32, Vec<RawRecord>> = HashMap::new();
    for r in records {
        by_partition.entry(r.partition).or_default().push(r);
    }
    for v in by_partition.values_mut() {
        v.sort_by_key(|r| r.offset);
    }

    let is_complete: Vec<bool> = windows.iter().map(|w| complete.contains(&w.partition)).collect();
    let mut streams: Vec<VecDeque<RawRecord>> = Vec::with_capacity(windows.len());
    for (w, &partition_complete) in windows.iter().zip(&is_complete) {
        let fetched = by_partition.remove(&w.partition).unwrap_or_default();
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
    (streams, is_complete)
}

/// One chunk's k-way merge outcome: decoded `matches` (already ≤ the
/// remaining `limit`, loss-free and overlap-free by construction), and each
/// partition's extreme offset actually taken (`None` if nothing was), which
/// `advance_positions` turns into the next cursor.
struct ChunkTaken {
    matches: Vec<MessageOut>,
    taken_min: Vec<Option<i64>>,
    taken_max: Vec<Option<i64>>,
    any_taken: bool,
}

/// `merge_chunk_records`'s parameters, bundled so the function takes one
/// named struct instead of a long positional list (the same rationale as
/// `PageRequest`): everything here is the page's running state at the
/// start of this chunk, not per-record state.
struct MergeChunkArgs<'a> {
    direction: Direction,
    filter: &'a Option<Filter>,
    handle: &'a ClusterHandle,
    limit_u64: u64,
    total_matches: u64,
    total_scanned: u64,
    budget: u64,
}

/// Repeatedly takes the head record `merge_prefers` picks across every
/// still-nonempty stream — best timestamp for the direction, ties broken by
/// partition then offset, see `merge_prefers`'s own doc comment — decoding
/// and filtering it immediately, so the stopping condition can depend on
/// the filter's verdict rather than waiting for a whole batch to decode.
/// Every popped record advances that partition's taken-offset bookkeeping
/// whether it matched or not; only matches are kept for emission. Sends a
/// mid-chunk `Progress` event every `PROGRESS_INTERVAL` records so a scan
/// hunting a sparse filter match doesn't go quiet for the length of a whole
/// chunk. Returns `None` the moment a send fails — the client is gone, and
/// the caller must stop scanning right there rather than continue against a
/// stream nobody is reading.
async fn merge_chunk_records(
    streams: &mut [VecDeque<RawRecord>],
    args: MergeChunkArgs<'_>,
    tx: &mpsc::Sender<TimelineEvent>,
) -> Option<ChunkTaken> {
    let MergeChunkArgs { direction, filter, handle, limit_u64, total_matches, total_scanned, budget } = args;
    let mut taken_min: Vec<Option<i64>> = vec![None; streams.len()];
    let mut taken_max: Vec<Option<i64>> = vec![None; streams.len()];
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

        records_popped_this_chunk += 1;
        if let Some(scanned) = mid_chunk_progress(total_scanned, records_popped_this_chunk)
            && tx.send(TimelineEvent::Progress {
                scanned,
                matches: total_matches + chunk_matches.len() as u64,
                budget,
            }).await.is_err()
        {
            return None;
        }
    }

    Some(ChunkTaken { matches: chunk_matches, taken_min, taken_max, any_taken })
}

/// Offset-exact cursor advance per partition — never a record count, which
/// would break on holes: "4 records taken" can span 5 offsets, landing the
/// cursor 1 short of where the scan really got and re-serving already-taken
/// records next page. A **complete** partition whose stream ended this
/// chunk fully drained jumps straight to the window boundary (`w.start` for
/// `Back`, `w.end` for `Forward`): completeness confirms the entire range
/// is accounted for, including any trailing hole past the last real
/// record. A complete partition that still has records left in its stream
/// (the merge hit the match target first) must NOT take that shortcut, or
/// it would skip pending data. Otherwise, a partition with records taken
/// this chunk advances to `min(taken)` for `Back` (the position is an
/// exclusive upper bound) or `max(taken) + 1` for `Forward` (an inclusive
/// lower bound). A partition with nothing taken and not complete-and-
/// drained keeps its old position — the safe default, whether it lost
/// every merge comparison or was incomplete (unknown state).
fn advance_positions(
    cur_positions: &[(i32, i64)],
    windows: &[PartitionRange],
    is_complete: &[bool],
    streams: &[VecDeque<RawRecord>],
    taken_min: &[Option<i64>],
    taken_max: &[Option<i64>],
    direction: Direction,
) -> Vec<(i32, i64)> {
    cur_positions
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
        .collect()
}

/// Budget charge for one chunk: the exact offset span each partition's
/// position advanced — holes included — never matches or delivered
/// records, so a chunk whose window is all holes still spends budget
/// proportional to the ground it covered (otherwise an unfiltered page
/// could cross an unbounded hole region for free).
fn charge_budget(old_positions: &[(i32, i64)], new_positions: &[(i32, i64)]) -> u64 {
    old_positions.iter().zip(new_positions).map(|(&(_, old), &(_, new))| old.abs_diff(new)).sum()
}

/// A page is exhausted only once every tracked partition has truly reached
/// its low/high watermark edge in `direction` — the sole end-of-data
/// signal; hitting `limit` or spending the budget ends the page with a
/// resume cursor instead.
fn all_partitions_at_edge(positions: &[(i32, i64)], watermarks: &[(i32, i64, i64)], direction: Direction) -> bool {
    positions.iter().all(|&(p, pos)| {
        watermarks.iter().find(|(wp, _, _)| *wp == p).is_some_and(|&(_, lo, hi)| at_edge(pos, lo, hi, direction))
    })
}

/// Sends one chunk's matches in display order (the out-of-order
/// policy: offset order within a partition always, timestamp order across
/// partitions — see `chunk_display_order`), then the chunk's own `Progress`
/// event. Returns `false` the moment a send fails, so the caller stops
/// scanning immediately rather than continuing against a stream nobody is
/// reading.
async fn emit_chunk(
    tx: &mpsc::Sender<TimelineEvent>,
    matches: Vec<MessageOut>,
    direction: Direction,
    total_scanned: u64,
    total_matches: u64,
    budget: u64,
) -> bool {
    for m in chunk_display_order(matches, direction) {
        if tx.send(TimelineEvent::Match(Box::new(m))).await.is_err() {
            return false;
        }
    }
    tx.send(TimelineEvent::Progress { scanned: total_scanned, matches: total_matches, budget }).await.is_ok()
}

/// Runs one timeline page — filtered or not, `filter: None` behaving as
/// match-everything (the design's "filter-off = match-all, same code
/// path"). Scans and decodes windows of `topic` starting from `positions`,
/// emitting `TimelineEvent`s over an mpsc channel until `limit` matches are
/// found, the topic edge is truly reached, or the per-request scan
/// `budget` is spent. `watermarks` and `positions` are resolved by the
/// caller (`api::messages`); nothing here re-reads them mid-page.
///
/// A page is a loop of chunks, each built from `chunk_span` (how much to
/// request), `plan_chunk_windows` (where), `scan_chunk` (the Kafka round
/// trip), `select_partition_streams` (which fetched records can be
/// trusted), `merge_chunk_records` (the k-way merge, decode, and filter),
/// `advance_positions` + `charge_budget` (committing the chunk), and
/// `emit_chunk` (sending it) — read in that order for the full mechanics of
/// one chunk. Only `all_partitions_at_edge` ends the page with
/// `exhausted: true` and no cursor, the sole end-of-data signal; the other
/// two loop exits (`limit` reached, budget spent) end with
/// `exhausted: false` and a resume cursor, and may legitimately carry ZERO
/// matches (budget spent crossing holes or non-matching records) — an
/// empty page is never a silent stop.
///
/// Failures never leave the page half-reported: a fetch error, a
/// `spawn_blocking` join failure, or `page_made_no_progress` all go out as
/// a terminal `TimelineEvent::Error` and end the stream *without* a
/// `page_end`, so a client can tell "this page finished" from "this page
/// broke". A client disconnect ends the task silently instead — there is
/// nobody left to tell.
pub fn run_page(
    handle: Arc<ClusterHandle>,
    topic: String,
    req: PageRequest,
) -> (mpsc::Receiver<TimelineEvent>, Arc<AtomicBool>) {
    let PageRequest { positions, watermarks, direction, limit, filter, budget } = req;
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
            let span = chunk_span(&filter, chunk_index, limit_u64, remaining_budget, &cur_positions, &watermarks, direction);
            let windows = plan_chunk_windows(&cur_positions, &watermarks, direction, span, remaining_budget);
            if windows.is_empty() {
                exhausted = true;
                break;
            }

            let (outcome, windows) = match scan_chunk(&handle, &topic, windows, &cancelled).await {
                Ok(pair) => pair,
                Err(e) => {
                    let _ = tx.send(e.into()).await;
                    return;
                }
            };

            let (mut streams, is_complete) = select_partition_streams(outcome.records, &windows, &outcome.complete, direction);

            let merge_args = MergeChunkArgs { direction, filter: &filter, handle: &handle, limit_u64, total_matches, total_scanned, budget };
            let Some(taken) = merge_chunk_records(&mut streams, merge_args, &tx).await else {
                return; // client gone mid-chunk
            };

            let new_positions = advance_positions(&cur_positions, &windows, &is_complete, &streams, &taken.taken_min, &taken.taken_max, direction);

            // Progress guarantee, checked before committing this chunk's
            // results, so a stalled chunk never reaches the normal page_end
            // path.
            let any_incomplete_window = is_complete.iter().any(|&c| !c);
            if page_made_no_progress(!taken.any_taken, any_incomplete_window, cancelled.load(Ordering::SeqCst)) {
                // Truthful about WHOSE deadline this is (see
                // `no_progress_message`'s own doc comment) — never blames
                // Kafka for our own `fetch_ranges_blocking` cap.
                let _ = tx.send(no_progress_error(&handle.name).into()).await;
                return;
            }

            total_scanned += charge_budget(&cur_positions, &new_positions);
            total_matches += taken.matches.len() as u64;
            exhausted = all_partitions_at_edge(&new_positions, &watermarks, direction);
            cur_positions = new_positions;
            chunk_index += 1;

            if !emit_chunk(&tx, taken.matches, direction, total_scanned, total_matches, budget).await {
                return;
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

    /// `page_made_no_progress` trips on OUR OWN `fetch_ranges_blocking`
    /// deadline/cap leaving a window incomplete — the broker itself may be
    /// perfectly healthy. The message must own that, never blame Kafka for
    /// going silent.
    #[test]
    fn no_progress_message_blames_our_own_deadline_not_kafka() {
        let msg = no_progress_message();
        assert_eq!(msg, "the cluster didn't return new records within the fetch deadline");
        assert!(
            !msg.to_lowercase().contains("kafka"),
            "must not attribute a stall under our own deadline to Kafka going quiet: {msg:?}"
        );
    }

    /// The message owns our deadline — the wire CODE must too. `kafka_error`
    /// and `kafka_timeout` both render under the frontend's "Kafka
    /// unreachable" headline (`describeError`), which would sit directly
    /// above a message saying the deadline was ours: contradictory. A stall
    /// gets its own code, which the frontend renders headline-free.
    #[test]
    fn no_progress_error_code_never_renders_as_kafka_unreachable() {
        let err = no_progress_error("prod");
        assert_eq!(err.code, "fetch_deadline");
        assert_eq!(err.message, no_progress_message());
        assert_eq!(err.cluster.as_deref(), Some("prod"));
        assert!(err.retriable, "a stalled fetch is worth retrying");
    }
}
