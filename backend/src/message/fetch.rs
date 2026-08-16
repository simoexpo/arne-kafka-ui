use super::range::PartitionRange;
use super::schema_registry::SchemaRegistry;
use super::{decode, HeaderOut, MessageOut};
use crate::cluster::{build_client_config, ClusterHandle, ADMIN_TIMEOUT};
use crate::config::ClusterConfig;
use crate::error::ApiError;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Monotonic per-process counter so each fetch's throwaway group.id is
/// unique even when two fetches land in the same millisecond.
static FETCH_SEQ: AtomicU64 = AtomicU64::new(0);

/// Per-topic count of real (non-trivial) `fetch_ranges_blocking` calls —
/// i.e. calls that actually built a `BaseConsumer`, not the early-return for
/// an already-empty range set. Test-only observability: a client-disconnect
/// integration test uses this to prove a cancelled scan stops minting fresh
/// consumers rather than spinning forever (the C1 zombie-scan bug). Scoped
/// per-topic (not a single global count) so it stays meaningful even when
/// other tests' fetches run concurrently against the shared test broker.
static FETCH_CALLS: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();

fn record_fetch_call(topic: &str) {
    let calls = FETCH_CALLS.get_or_init(|| Mutex::new(HashMap::new()));
    *calls.lock().unwrap().entry(topic.to_string()).or_insert(0) += 1;
}

pub fn fetch_call_count(topic: &str) -> u64 {
    FETCH_CALLS.get().and_then(|m| m.lock().unwrap().get(topic).copied()).unwrap_or(0)
}

#[derive(Debug)]
pub struct RawRecord {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl RawRecord {
    /// Builds a `RawRecord` from a borrowed rdkafka message — the one place
    /// that maps headers/timestamp/key/value, shared by every poll loop that
    /// reads real messages (this module's own scan loop below, and
    /// `tail::run_consumer_blocking`).
    pub fn from_borrowed(msg: &BorrowedMessage) -> Self {
        let headers = msg
            .headers()
            .map(|hs| hs.iter().map(|h| (h.key.to_string(), h.value.unwrap_or_default().to_vec())).collect())
            .unwrap_or_default();
        RawRecord {
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp_ms: msg.timestamp().to_millis(),
            key: msg.key().map(<[u8]>::to_vec),
            value: msg.payload().map(<[u8]>::to_vec),
            headers,
        }
    }
}

/// Fetches fresh per-partition low/high watermarks for `topic`. Shared by
/// every caller that needs them — `api::messages`'s browse/search anchor
/// resolution and `message::timeline::run_page` alike (fix round 1, M4) —
/// so there's exactly one implementation of "ask Kafka for a topic's
/// current bounds", not a copy per call site.
pub fn watermarks_blocking(handle: &ClusterHandle, topic: &str) -> Result<Vec<(i32, i64, i64)>, ApiError> {
    let md = handle.consumer()
        .fetch_metadata(Some(topic), ADMIN_TIMEOUT)
        .map_err(|e| crate::error::from_kafka(&handle.name, "fetch metadata", &e))?;
    let t = md.topics().iter()
        .find(|t| t.name() == topic && !t.partitions().is_empty())
        .ok_or_else(|| ApiError::topic_not_found(&handle.name, topic))?;
    let mut wm = Vec::new();
    for p in t.partitions() {
        let (lo, hi) = handle.consumer()
            .fetch_watermarks(topic, p.id(), ADMIN_TIMEOUT)
            .map_err(|e| crate::error::from_kafka(&handle.name, "fetch watermarks", &e))?;
        wm.push((p.id(), lo, hi));
    }
    Ok(wm)
}

/// Result of a bounded Kafka fetch: the records actually delivered, plus
/// which requested partitions were scanned to *completion* — i.e. the poll
/// loop reached that partition's target offset or `PartitionEOF`, as
/// opposed to stopping early because of this function's own deadline or
/// `cap`.
///
/// Fix round 3, N4: this distinction matters because Kafka topics
/// legitimately have offset holes that carry no message at all —
/// transaction control records (a committed transaction's own commit
/// marker consumes a real offset, typically the last one before the
/// partition's high watermark), aborted-transaction ranges, and compacted
/// tombstones. A partition marked complete here had its *entire* requested
/// range scanned, so any offsets in that range absent from `records` are
/// confirmed, legitimate holes — never mistaken for data that might still
/// be sitting behind a slow poll. A partition *not* marked complete
/// stopped for an unknown reason (deadline, `cap`, cancellation) and its
/// gaps (if any) cannot be trusted the same way.
#[derive(Debug)]
pub struct FetchOutcome {
    pub records: Vec<RawRecord>,
    pub complete: HashSet<i32>,
}

pub fn fetch_ranges_blocking(
    cfg: &ClusterConfig,
    topic: &str,
    ranges: &[PartitionRange],
    cap: usize,
    cancelled: &AtomicBool,
) -> Result<FetchOutcome, ApiError> {
    // A partition whose requested range is already empty (start >= end —
    // e.g. an offset beyond the high watermark) has nothing to fetch and is
    // trivially complete: there's no ambiguity about what's in an empty
    // range.
    let trivially_complete: HashSet<i32> = ranges.iter().filter(|r| r.start >= r.end).map(|r| r.partition).collect();
    // Ranges with start >= end have nothing to fetch — drop them before
    // assigning so the poll loop's `done` map only tracks partitions that
    // can actually terminate.
    let ranges: Vec<PartitionRange> = ranges.iter().filter(|r| r.start < r.end).cloned().collect();
    if ranges.is_empty() {
        return Ok(FetchOutcome { records: Vec::new(), complete: trivially_complete });
    }
    record_fetch_call(topic);
    // librdkafka's consumer machinery requires a group.id even for pure
    // assign()-based fetches with no group management involved; each fetch
    // uses a throwaway id and never commits, so no real group is affected.
    let seq = FETCH_SEQ.fetch_add(1, Ordering::Relaxed);
    let group_id = format!("betrachtung-fetch-{}-{}-{seq}", std::process::id(), crate::util::now_ms());
    let consumer: BaseConsumer = build_client_config(cfg)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .create()
        .map_err(|e| crate::error::from_kafka(&cfg.name, "create fetch consumer", &e))?;
    let mut tpl = TopicPartitionList::new();
    for r in &ranges {
        tpl.add_partition_offset(topic, r.partition, Offset::Offset(r.start))
            .map_err(|e| crate::error::from_kafka(&cfg.name, "assign offsets", &e))?;
    }
    consumer.assign(&tpl).map_err(|e| crate::error::from_kafka(&cfg.name, "assign", &e))?;

    let targets: HashMap<i32, i64> = ranges.iter().map(|r| (r.partition, r.end)).collect();
    let mut done: HashMap<i32, bool> = ranges.iter().map(|r| (r.partition, false)).collect();
    let mut out = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);

    while done.values().any(|d| !d)
        && Instant::now() < deadline
        && out.len() < cap
        // Honor the caller's cancellation flag, so a client-disconnect
        // (CancelOnDrop) actually stops an in-flight timeline scan instead
        // of only ever being read by whoever built the `Arc` and nothing
        // else.
        && !cancelled.load(Ordering::SeqCst)
    {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(msg)) => {
                let p = msg.partition();
                let end = targets[&p];
                if msg.offset() >= end {
                    done.insert(p, true);
                    continue;
                }
                let offset = msg.offset();
                out.push(RawRecord::from_borrowed(&msg));
                if offset + 1 >= end {
                    done.insert(p, true);
                }
            }
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(p))) => {
                done.insert(p, true);
            }
            Some(Err(_)) | None => {} // transient; deadline bounds us
        }
    }
    let mut complete: HashSet<i32> = done.into_iter().filter(|&(_, d)| d).map(|(p, _)| p).collect();
    complete.extend(trivially_complete);
    Ok(FetchOutcome { records: out, complete })
}

/// Fetches exactly one record at `(partition, offset)` — a bounded,
/// single-record read. Owner ruling 2026-08-15: this is how a forward
/// offset-anchor jump resolves the anchored message's own timestamp (so
/// every OTHER partition can align at that same moment — see
/// `Anchor::OffsetForwardAligned` in `timeline.rs`). Same consumer
/// discipline as every other fetch here (`fetch_ranges_blocking`, its own
/// throwaway consumer, the same internal deadline) — not a special,
/// unbounded path. Returns `None` if there's no message at that exact
/// position (already past the high watermark, or the offset lands on a
/// hole — a compacted/tombstoned record).
pub fn fetch_one_record_blocking(
    cfg: &ClusterConfig,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<Option<RawRecord>, ApiError> {
    let cancelled = AtomicBool::new(false);
    let range = PartitionRange { partition, start: offset, end: offset + 1 };
    let outcome = fetch_ranges_blocking(cfg, topic, std::slice::from_ref(&range), 1, &cancelled)?;
    Ok(outcome.records.into_iter().next())
}

/// Decodes exactly one record — the shared decode step both the batch API
/// below and a caller that must decode-then-test one record at a time (the
/// timeline engine's filtered merge, tail's per-message stream) build on.
pub async fn to_one_message_out(r: RawRecord, sr: Option<&SchemaRegistry>) -> MessageOut {
    MessageOut {
        partition: r.partition,
        offset: r.offset,
        timestamp_ms: r.timestamp_ms,
        key: decode::decode_payload(r.key.as_deref(), sr).await,
        value: decode::decode_payload(r.value.as_deref(), sr).await,
        headers: r.headers.into_iter().map(|(key, v)| HeaderOut {
            key,
            value: String::from_utf8_lossy(&v).into_owned(),
        }).collect(),
    }
}

pub async fn to_message_out(records: Vec<RawRecord>, sr: Option<&SchemaRegistry>) -> Vec<MessageOut> {
    let mut out = Vec::with_capacity(records.len());
    for r in records {
        out.push(to_one_message_out(r, sr).await);
    }
    out
}
