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
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};

const PARTITION_CONCURRENCY: usize = 8;
const PROGRESS_EVERY: u64 = 500;

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

            workers.push(tokio::spawn(async move {
                let _permit = permit;
                let (raw_tx, mut raw_rx) = mpsc::channel::<RawRecord>(64);
                let scan_cancel = cancelled.clone();
                let scan_scanned = scanned.clone();
                let scan = tokio::task::spawn_blocking(move || {
                    scan_partition_blocking(&cfg, &topic, &r, &scan_cancel, &scan_scanned, raw_tx)
                });

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
                    let s = scanned.load(Ordering::SeqCst);
                    if s.is_multiple_of(PROGRESS_EVERY) {
                        let _ = tx.try_send(SearchEvent::Progress {
                            scanned: s,
                            total: grand_total,
                            matches: matched.load(Ordering::SeqCst),
                        });
                    }
                }
                let _ = scan.await;
            }));
        }

        for w in workers {
            let _ = w.await;
        }

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
    });

    (rx, cancel)
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
                if out.blocking_send(record).is_err() {
                    break;
                }
            }
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => break,
            Some(Err(_)) | None => {}
        }
    }
    Ok(())
}

async fn one_message_out(record: RawRecord, sr: Option<&SchemaRegistry>) -> MessageOut {
    fetch::to_message_out(vec![record], sr).await.pop().expect("one in, one out")
}
