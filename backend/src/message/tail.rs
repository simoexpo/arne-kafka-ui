use super::{fetch, MessageOut};
use crate::cluster::ClusterHandle;
use crate::error::ApiError;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Monotonic per-process counter so each tail's throwaway group.id is unique
/// even when two tails land in the same millisecond (mirrors fetch.rs's and
/// search.rs's pattern — `assign()` requires a group.id even though no real
/// consumer group is involved).
static TAIL_SEQ: AtomicU64 = AtomicU64::new(0);

pub async fn run(
    handle: Arc<ClusterHandle>,
    topic: String,
) -> Result<(mpsc::Receiver<MessageOut>, Arc<AtomicBool>), ApiError> {
    // 404 before streaming starts if the topic doesn't exist
    let wm = {
        let handle = handle.clone();
        let topic = topic.clone();
        tokio::task::spawn_blocking(move || crate::api::messages::watermarks_blocking(&handle, &topic))
            .await
            .map_err(|e| ApiError::internal(format!("task join: {e}")))??
    };

    let (tx, rx) = mpsc::channel::<MessageOut>(256);
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel = cancelled.clone();
    let sr = handle.schema_registry.clone();
    let cfg = handle.config.clone();

    tokio::spawn(async move {
        let (raw_tx, mut raw_rx) = mpsc::channel::<fetch::RawRecord>(256);
        let poll_cancel = cancelled.clone();
        let poll_topic = topic.clone();
        tokio::task::spawn_blocking(move || {
            // librdkafka's consumer machinery requires a group.id even for
            // pure assign()-based consumption with no group management
            // involved; this uses a throwaway id and never commits, so no
            // real group is affected.
            let seq = TAIL_SEQ.fetch_add(1, Ordering::Relaxed);
            let group_id = format!("betrachtung-tail-{}-{}-{seq}", std::process::id(), crate::util::now_ms());
            let Ok(consumer) = crate::cluster::build_client_config(&cfg)
                .set("group.id", group_id)
                .set("enable.auto.commit", "false")
                .create::<BaseConsumer>()
            else {
                return;
            };
            let mut tpl = TopicPartitionList::new();
            for &(p, _, _) in &wm {
                let _ = tpl.add_partition_offset(&poll_topic, p, Offset::End);
            }
            if consumer.assign(&tpl).is_err() {
                return;
            }
            while !poll_cancel.load(Ordering::SeqCst) {
                if let Some(Ok(msg)) = consumer.poll(Duration::from_millis(200)) {
                    let headers = msg
                        .headers()
                        .map(|hs| hs.iter().map(|h| (h.key.to_string(), h.value.unwrap_or_default().to_vec())).collect())
                        .unwrap_or_default();
                    let record = fetch::RawRecord {
                        partition: msg.partition(),
                        offset: msg.offset(),
                        timestamp_ms: msg.timestamp().to_millis(),
                        key: msg.key().map(<[u8]>::to_vec),
                        value: msg.payload().map(<[u8]>::to_vec),
                        headers,
                    };
                    if raw_tx.blocking_send(record).is_err() {
                        return;
                    }
                }
            }
        });

        while let Some(record) = raw_rx.recv().await {
            let msg = fetch::to_message_out(vec![record], sr.as_deref()).await.pop().expect("one in one out");
            if tx.send(msg).await.is_err() {
                cancelled.store(true, Ordering::SeqCst);
                break;
            }
        }
    });

    Ok((rx, cancel))
}
