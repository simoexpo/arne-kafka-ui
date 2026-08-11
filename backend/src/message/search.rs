use super::fetch::RawRecord;
use super::filter::{self, Filter};
use super::range::{total, PartitionRange};
use super::schema_registry::SchemaRegistry;
use super::{fetch, MessageOut};
use crate::cluster::ClusterHandle;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;

const PARTITION_CONCURRENCY: usize = 8;
const PROGRESS_EVERY: u64 = 500;
/// If a partition scanner goes this long without successfully receiving a
/// message (persistent poll errors, unreachable broker, ...) it gives up
/// instead of spinning on 200ms polls forever.
const STALL_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum SearchEvent {
    Progress { scanned: u64, total: u64, matches: u64 },
    Match(Box<MessageOut>),
    Done { reason: String },
    Error { code: String, message: String },
}

impl SearchEvent {
    pub fn name(&self) -> &'static str {
        match self {
            SearchEvent::Progress { .. } => "progress",
            SearchEvent::Match(_) => "match",
            SearchEvent::Done { .. } => "done",
            SearchEvent::Error { .. } => "error",
        }
    }
}

/// Monotonic per-process counter so each scanner's throwaway group.id is
/// unique even when two searches land in the same millisecond (mirrors
/// fetch.rs's pattern — `assign()` requires a group.id even though no real
/// consumer group is involved).
static SEARCH_SEQ: AtomicU64 = AtomicU64::new(0);

pub fn run(
    handle: Arc<ClusterHandle>,
    topic: String,
    ranges: Vec<PartitionRange>,
    filter: Filter,
    max_matches: u64,
) -> (mpsc::Receiver<SearchEvent>, Arc<AtomicBool>) {
    let (tx, rx) = mpsc::channel::<SearchEvent>(256);
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel = cancelled.clone();

    tokio::spawn(async move {
        let grand_total = total(&ranges);
        let scanned = Arc::new(AtomicU64::new(0));
        let matched = Arc::new(AtomicU64::new(0));
        // Highest 500-boundary already reported: concurrent partition
        // workers CAS-race to claim each boundary exactly once, instead of
        // every worker separately sampling the shared `scanned` counter
        // (which could skip, duplicate, or emit boundaries out of order —
        // see `maybe_report_progress`).
        let last_reported = Arc::new(AtomicU64::new(0));
        // Set as soon as any partition scan fails, so the coordinator sends
        // exactly one `error` event and skips the final progress/done pair
        // — per the SSE contract, `error` is terminal.
        let error_sent = Arc::new(AtomicBool::new(false));
        let semaphore = Arc::new(Semaphore::new(PARTITION_CONCURRENCY));
        let mut workers = Vec::new();

        for r in ranges {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break, // semaphore closed: coordinator is shutting down
            };
            let tx = tx.clone();
            let cfg = handle.config.clone();
            let sr = handle.schema_registry.clone();
            let topic = topic.clone();
            let filter = filter.clone();
            let cancelled = cancelled.clone();
            let scanned = scanned.clone();
            let matched = matched.clone();
            let last_reported = last_reported.clone();
            let error_sent = error_sent.clone();
            let partition = r.partition;

            workers.push(tokio::spawn(async move {
                let (raw_tx, raw_rx) = mpsc::channel::<RawRecord>(64);
                let scan_cancel = cancelled.clone();
                let scan_scanned = scanned.clone();
                let scan = tokio::task::spawn_blocking(move || {
                    scan_partition_blocking(&cfg, &topic, &r, &scan_cancel, &scan_scanned, raw_tx)
                });
                let _permit = permit;
                drive_partition(
                    raw_rx, scan, partition, sr, filter, tx, cancelled, scanned, matched,
                    last_reported, error_sent, grand_total, max_matches,
                ).await;
            }));
        }

        for w in workers {
            let _ = w.await;
        }

        // `error` is terminal per the SSE contract: a scan failure has
        // already been reported by whichever worker hit it, so no trailing
        // progress/done follows it.
        if !error_sent.load(Ordering::SeqCst) {
            let matches = matched.load(Ordering::SeqCst).min(max_matches);
            let _ = tx
                .send(SearchEvent::Progress {
                    scanned: scanned.load(Ordering::SeqCst),
                    total: grand_total,
                    matches,
                })
                .await;
            let reason = if matched.load(Ordering::SeqCst) >= max_matches { "max_matches" } else { "complete" };
            let _ = tx.send(SearchEvent::Done { reason: reason.into() }).await;
        }
    });

    (rx, cancel)
}

/// Drains one partition's raw-record channel, decoding and filtering each
/// record and forwarding matches, then joins the scanner's blocking task.
///
/// Extracted from `run()`'s per-partition loop so the exact worker
/// lifecycle — in particular what happens to `raw_rx` and to `scan` once
/// the worker stops reading — can be exercised directly in tests without a
/// real Kafka broker (see `tests::worker_giving_up_early_wakes_a_parked_scanner`).
#[allow(clippy::too_many_arguments)]
async fn drive_partition(
    mut raw_rx: mpsc::Receiver<RawRecord>,
    scan: JoinHandle<Result<(), String>>,
    partition: i32,
    sr: Option<Arc<SchemaRegistry>>,
    filter: Filter,
    tx: mpsc::Sender<SearchEvent>,
    cancelled: Arc<AtomicBool>,
    scanned: Arc<AtomicU64>,
    matched: Arc<AtomicU64>,
    last_reported: Arc<AtomicU64>,
    error_sent: Arc<AtomicBool>,
    grand_total: u64,
    max_matches: u64,
) {
    while let Some(record) = raw_rx.recv().await {
        let msg = one_message_out(record, sr.as_deref()).await;
        if filter::matches(&filter, &msg) {
            let m = matched.fetch_add(1, Ordering::SeqCst) + 1;
            if m > max_matches {
                cancelled.store(true, Ordering::SeqCst);
                break;
            }
            if tx.send(SearchEvent::Match(Box::new(msg))).await.is_err() {
                cancelled.store(true, Ordering::SeqCst);
                break;
            }
            if m == max_matches {
                cancelled.store(true, Ordering::SeqCst);
            }
        }
        maybe_report_progress(&scanned, &last_reported, &matched, grand_total, &tx);
    }

    // F1 fix: if this worker broke out early (max-matches hit, or the SSE
    // channel closed) the blocking scan thread may be parked in
    // `blocking_send`/`send_or_stop`, waiting for buffer space that will
    // never free up now that nobody is reading. Dropping `raw_rx` closes
    // the channel and wakes it immediately — without this the scanner
    // thread, its semaphore permit, and its BaseConsumer leak forever (a
    // zombie scan). This must happen *before* awaiting `scan` below.
    drop(raw_rx);

    match scan.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => report_scan_error(&tx, &cancelled, &error_sent, e).await,
        Err(join_err) => {
            report_scan_error(
                &tx,
                &cancelled,
                &error_sent,
                format!("partition {partition} scan task failed: {join_err}"),
            ).await;
        }
    }
}

/// Emits a `SearchEvent::Progress` for each 500-record boundary the shared
/// `scanned` counter has crossed since the last report, advancing
/// `last_reported` via compare-exchange so exactly one worker emits per
/// boundary and reported `scanned` values are strictly increasing — never
/// skipped, duplicated, or delivered out of order across partitions.
fn maybe_report_progress(
    scanned: &AtomicU64,
    last_reported: &AtomicU64,
    matched: &AtomicU64,
    grand_total: u64,
    tx: &mpsc::Sender<SearchEvent>,
) {
    let s = scanned.load(Ordering::SeqCst);
    loop {
        let last = last_reported.load(Ordering::SeqCst);
        let next_boundary = last + PROGRESS_EVERY;
        if s < next_boundary {
            return;
        }
        if last_reported.compare_exchange(last, next_boundary, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            let _ = tx.try_send(SearchEvent::Progress {
                scanned: next_boundary,
                total: grand_total,
                matches: matched.load(Ordering::SeqCst),
            });
            // `s` may already be several boundaries ahead (e.g. this
            // worker was scheduled late); loop to catch up instead of
            // reporting only one boundary per call.
            continue;
        }
        // CAS lost the race: another worker just advanced `last_reported`.
        // Reload and re-check against the (unchanged) `s` we captured.
    }
}

/// Reports a partition scan failure as a terminal `error` event and stops
/// every other scanner. `error_sent` ensures only the first failure (of
/// possibly several concurrent ones, e.g. a broker outage hitting every
/// partition at once) reaches the client — later ones are superseded since
/// the stream is ending regardless.
async fn report_scan_error(
    tx: &mpsc::Sender<SearchEvent>,
    cancelled: &AtomicBool,
    error_sent: &AtomicBool,
    message: String,
) {
    cancelled.store(true, Ordering::SeqCst);
    if error_sent.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
        let _ = tx.send(SearchEvent::Error { code: "kafka_error".into(), message }).await;
    }
}

/// Hands `record` to the async decode/filter task. The fast path is a
/// plain `try_send` (no cost beyond the channel op itself, so a healthy
/// scan runs at full speed with no artificial throughput cap). If the
/// channel is momentarily full — the decoder briefly falling behind a
/// burst — this falls back to a real `blocking_send`, which parks the
/// thread but wakes it *immediately* either when capacity frees up or when
/// the channel closes. It never leaks a thread: after the F1 fix,
/// `drive_partition` drops its `raw_rx` on every loop exit (max-matches
/// hit, SSE client gone, or the recv loop ending normally), so `Closed`
/// covers every case where nobody is left to drain the channel. The
/// `cancelled` checks are cheap early-outs, not a substitute for that —
/// they just skip a doomed send attempt slightly sooner.
fn send_or_stop(out: &mpsc::Sender<RawRecord>, record: RawRecord, cancelled: &AtomicBool) -> bool {
    if cancelled.load(Ordering::SeqCst) {
        return false;
    }
    match out.try_send(record) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Closed(_)) => false,
        Err(mpsc::error::TrySendError::Full(rec)) => match out.blocking_send(rec) {
            Ok(()) => !cancelled.load(Ordering::SeqCst),
            Err(_) => false,
        },
    }
}

fn scan_partition_blocking(
    cfg: &crate::config::ClusterConfig,
    topic: &str,
    r: &PartitionRange,
    cancelled: &AtomicBool,
    scanned: &AtomicU64,
    out: mpsc::Sender<RawRecord>,
) -> Result<(), String> {
    let seq = SEARCH_SEQ.fetch_add(1, Ordering::Relaxed);
    let group_id = format!("betrachtung-search-{}-{}-{seq}", std::process::id(), crate::util::now_ms());
    let consumer: BaseConsumer = crate::cluster::build_client_config(cfg)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .create()
        .map_err(|e| e.to_string())?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, r.partition, Offset::Offset(r.start)).map_err(|e| e.to_string())?;
    consumer.assign(&tpl).map_err(|e| e.to_string())?;

    let mut next = r.start;
    let mut last_progress = Instant::now();
    let mut last_err: Option<String> = None;
    while next < r.end && !cancelled.load(Ordering::SeqCst) {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(msg)) => {
                if msg.offset() >= r.end {
                    break;
                }
                next = msg.offset() + 1;
                scanned.fetch_add(1, Ordering::SeqCst);
                let headers = msg
                    .headers()
                    .map(|hs| hs.iter().map(|h| (h.key.to_string(), h.value.unwrap_or_default().to_vec())).collect())
                    .unwrap_or_default();
                let record = RawRecord {
                    partition: msg.partition(),
                    offset: msg.offset(),
                    timestamp_ms: msg.timestamp().to_millis(),
                    key: msg.key().map(<[u8]>::to_vec),
                    value: msg.payload().map(<[u8]>::to_vec),
                    headers,
                };
                if !send_or_stop(&out, record, cancelled) {
                    break;
                }
                // Reset only *after* the handoff completes: `send_or_stop`
                // can legitimately block for a while on downstream
                // backpressure (a slow SSE client) or decode latency (a
                // flaky Schema Registry), and none of that is Kafka being
                // stalled — Kafka just handed us a message. Resetting
                // beforehand would let that wait count against the stall
                // budget and fabricate a Kafka error for a local slowdown.
                last_progress = Instant::now();
            }
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => break,
            Some(Err(e)) => {
                last_err = Some(e.to_string());
            }
            None => {}
        }
        if last_progress.elapsed() > STALL_TIMEOUT {
            let reason = last_err.unwrap_or_else(|| "no messages received".to_string());
            return Err(format!("partition {} stalled: {reason}", r.partition));
        }
    }
    Ok(())
}

async fn one_message_out(record: RawRecord, sr: Option<&SchemaRegistry>) -> MessageOut {
    fetch::to_message_out(vec![record], sr).await.pop().expect("one in, one out")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_record(offset: i64) -> RawRecord {
        RawRecord { partition: 0, offset, timestamp_ms: Some(0), key: None, value: Some(b"x".to_vec()), headers: vec![] }
    }

    /// F1 regression, reproduced deterministically without Kafka.
    ///
    /// `drive_partition`'s recv loop breaks out early once the match cap is
    /// hit (here: after exactly 2 records, via `max_matches = 1`), while
    /// its scanner (a fake blocking task standing in for
    /// `scan_partition_blocking`) is still trying to push more records
    /// into a small channel. With capacity 2 and 2 reads, at most 4 sends
    /// can ever succeed (2 to fill the initial buffer, 2 more unblocked by
    /// the 2 reads) — the fake scanner tries to push 10, so its 5th send
    /// is *guaranteed* to block forever once nobody drains the channel
    /// again, unless `drive_partition` drops its receiver before joining
    /// the scanner's `JoinHandle`.
    ///
    /// The fake scanner deliberately calls the *raw* `raw_tx.blocking_send`
    /// here, not `send_or_stop`: `send_or_stop` has its own `cancelled`
    /// early-out, which would rescue a parked sender independently of
    /// whether the receiver was ever dropped, making this test pass for
    /// the wrong reason (guarding a redundant safety net instead of the
    /// actual fix). A plain `blocking_send` has no such escape hatch — the
    /// only thing that can unblock it is the channel closing, which is
    /// exactly what `drop(raw_rx)` does. See the report's "Fix round 2 —
    /// N3" section for the kill-the-fix verification (temporarily removing
    /// `drop(raw_rx)` turns this test red).
    #[tokio::test]
    async fn worker_giving_up_early_wakes_a_parked_scanner() {
        let (raw_tx, raw_rx) = mpsc::channel::<RawRecord>(2);
        let scan: JoinHandle<Result<(), String>> = tokio::task::spawn_blocking(move || {
            for i in 0..10i64 {
                if raw_tx.blocking_send(dummy_record(i)).is_err() {
                    return Ok(());
                }
            }
            Ok(())
        });

        let (tx, mut rx) = mpsc::channel::<SearchEvent>(16);
        // Keep the SSE-side receiver alive and draining so `tx.send` for
        // the first match doesn't itself fail (that's a different code
        // path, not what this test is about).
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

        let cancelled = Arc::new(AtomicBool::new(false));
        let scanned = Arc::new(AtomicU64::new(0));
        let matched = Arc::new(AtomicU64::new(0));
        let last_reported = Arc::new(AtomicU64::new(0));
        let error_sent = Arc::new(AtomicBool::new(false));

        let drive = drive_partition(
            raw_rx,
            scan,
            0,
            None,
            Filter::ValueContains("x".into()),
            tx.clone(),
            cancelled,
            scanned,
            matched,
            last_reported,
            error_sent,
            10,
            1, // max_matches: the 2nd match (m=2) exceeds this and breaks
        );

        let result = tokio::time::timeout(Duration::from_secs(5), drive).await;
        drop(tx);
        let _ = drain.await;
        assert!(
            result.is_ok(),
            "drive_partition hung: a scanner parked mid-send on a full, un-drained channel was never woken"
        );
    }
}
