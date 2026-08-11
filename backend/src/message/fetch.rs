use super::range::PartitionRange;
use super::schema_registry::SchemaRegistry;
use super::{decode, HeaderOut, MessageOut};
use crate::cluster::build_client_config;
use crate::config::ClusterConfig;
use crate::error::ApiError;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Monotonic per-process counter so each fetch's throwaway group.id is
/// unique even when two fetches land in the same millisecond.
static FETCH_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct RawRecord {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

pub fn fetch_ranges_blocking(
    cfg: &ClusterConfig,
    topic: &str,
    ranges: &[PartitionRange],
    cap: usize,
) -> Result<Vec<RawRecord>, ApiError> {
    // Ranges with start >= end have nothing to fetch (e.g. offset beyond the
    // high watermark) — drop them before assigning so the poll loop's `done`
    // map only tracks partitions that can actually terminate.
    let ranges: Vec<PartitionRange> = ranges.iter().filter(|r| r.start < r.end).cloned().collect();
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
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

    while done.values().any(|d| !d) && Instant::now() < deadline && out.len() < cap {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(msg)) => {
                let p = msg.partition();
                let end = targets[&p];
                if msg.offset() >= end {
                    done.insert(p, true);
                    continue;
                }
                let headers = msg.headers().map(|hs| {
                    hs.iter()
                        .map(|h| (h.key.to_string(), h.value.unwrap_or_default().to_vec()))
                        .collect()
                }).unwrap_or_default();
                out.push(RawRecord {
                    partition: p,
                    offset: msg.offset(),
                    timestamp_ms: msg.timestamp().to_millis(),
                    key: msg.key().map(<[u8]>::to_vec),
                    value: msg.payload().map(<[u8]>::to_vec),
                    headers,
                });
                if msg.offset() + 1 >= end {
                    done.insert(p, true);
                }
            }
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(p))) => {
                done.insert(p, true);
            }
            Some(Err(_)) | None => {} // transient; deadline bounds us
        }
    }
    Ok(out)
}

pub async fn to_message_out(records: Vec<RawRecord>, sr: Option<&SchemaRegistry>) -> Vec<MessageOut> {
    let mut out = Vec::with_capacity(records.len());
    for r in records {
        out.push(MessageOut {
            partition: r.partition,
            offset: r.offset,
            timestamp_ms: r.timestamp_ms,
            key: decode::decode_payload(r.key.as_deref(), sr).await,
            value: decode::decode_payload(r.value.as_deref(), sr).await,
            headers: r.headers.into_iter().map(|(key, v)| HeaderOut {
                key,
                value: String::from_utf8_lossy(&v).into_owned(),
            }).collect(),
        });
    }
    out
}
