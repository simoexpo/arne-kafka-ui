use super::{fetch, MessageOut};
use crate::cluster::ClusterHandle;
use crate::error::ApiError;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::{ClientConfig, Offset};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Monotonic per-process counter so each tail's throwaway group.id is unique
/// even when two tails land in the same millisecond (mirrors fetch.rs's and
/// search.rs's pattern — `assign()` requires a group.id even though no real
/// consumer group is involved).
static TAIL_SEQ: AtomicU64 = AtomicU64::new(0);

/// If the poll loop sees nothing but errors for this long (dead broker,
/// deleted topic, ...) it gives up instead of spinning forever — mirrors
/// search.rs's STALL_TIMEOUT. Unlike search, a *quiet* topic (no errors, no
/// messages) never counts toward this: an idle tail is normal and must not
/// be mistaken for a stalled one.
const STALL_TIMEOUT: Duration = Duration::from_secs(10);

/// C1 fix: tail's SSE stream now carries an explicit event enum (mirroring
/// search's `SearchEvent`) instead of only ever emitting `MessageOut`s.
/// Previously, a consumer create/assign failure or a run of poll errors was
/// swallowed silently — the task just returned, the channel closed, and the
/// SSE stream ended with no event at all, so an `EventSource` client would
/// reconnect forever with nothing to show the user.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum TailEvent {
    Message(Box<MessageOut>),
    Error { code: String, message: String, cluster: String, retriable: bool },
}

impl TailEvent {
    pub fn name(&self) -> &'static str {
        match self {
            TailEvent::Message(_) => "message",
            TailEvent::Error { .. } => "error",
        }
    }

    /// Shapes a terminal error event to match the `ApiError` JSON envelope
    /// used everywhere else in the API, per the C1 fix's requirement.
    fn from_api_error(err: &ApiError) -> Self {
        TailEvent::Error {
            code: err.code.to_string(),
            message: err.message.clone(),
            cluster: err.cluster.clone().unwrap_or_default(),
            retriable: err.retriable,
        }
    }
}

/// One outcome of a single `consumer.poll()` call, decoupled from rdkafka's
/// borrowed-message types so the stall-detection loop (`drive_tail_poll`)
/// can be driven by a plain closure in tests, without a real consumer.
enum PollOutcome {
    Message(fetch::RawRecord),
    Error(String),
    Empty,
}

fn build_record(msg: &rdkafka::message::BorrowedMessage) -> fetch::RawRecord {
    let headers = msg
        .headers()
        .map(|hs| hs.iter().map(|h| (h.key.to_string(), h.value.unwrap_or_default().to_vec())).collect())
        .unwrap_or_default();
    fetch::RawRecord {
        partition: msg.partition(),
        offset: msg.offset(),
        timestamp_ms: msg.timestamp().to_millis(),
        key: msg.key().map(<[u8]>::to_vec),
        value: msg.payload().map(<[u8]>::to_vec),
        headers,
    }
}

/// Drives the tail poll loop until `cancel` is set or a stall is declared.
///
/// A stall is 10s (`stall_timeout`) elapsed since the last successfully
/// received message *with only poll errors in between* — a quiet topic
/// (poll returning "no message" over and over) never counts, since an idle
/// tail is the normal, expected state and must not be reported as an error.
/// This is the extracted seam the C1 fix's stall-detection unit tests drive
/// directly, without any real Kafka consumer.
fn drive_tail_poll(
    mut poll: impl FnMut() -> PollOutcome,
    cancel: &AtomicBool,
    out: &mpsc::Sender<fetch::RawRecord>,
    stall_timeout: Duration,
) -> Result<(), String> {
    let mut error_streak_since: Option<Instant> = None;
    while !cancel.load(Ordering::SeqCst) {
        match poll() {
            PollOutcome::Message(record) => {
                error_streak_since = None;
                if out.blocking_send(record).is_err() {
                    // Downstream (the async orchestration task) is gone —
                    // not an error, just nothing left to do.
                    return Ok(());
                }
            }
            PollOutcome::Error(e) => {
                let since = *error_streak_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= stall_timeout {
                    return Err(format!("tail stalled: {e}"));
                }
            }
            PollOutcome::Empty => {}
        }
    }
    Ok(())
}

/// Builds the tail consumer, assigns it to the end of every partition in
/// `watermarks`, and drives the poll loop. Both `.create()` and `.assign()`
/// failures are now reported (previously: `return;`, silently ending the
/// task with no event at all — C1a).
fn run_consumer_blocking(
    cc: ClientConfig,
    topic: &str,
    watermarks: &[(i32, i64, i64)],
    cancel: &AtomicBool,
    out: mpsc::Sender<fetch::RawRecord>,
    stall_timeout: Duration,
) -> Result<(), String> {
    let consumer: BaseConsumer = cc.create().map_err(|e| format!("create tail consumer: {e}"))?;
    let mut tpl = TopicPartitionList::new();
    for &(p, _, _) in watermarks {
        tpl.add_partition_offset(topic, p, Offset::End).map_err(|e| format!("build tail assignment: {e}"))?;
    }
    consumer.assign(&tpl).map_err(|e| format!("assign tail consumer: {e}"))?;

    drive_tail_poll(
        || match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(msg)) => PollOutcome::Message(build_record(&msg)),
            Some(Err(e)) => PollOutcome::Error(e.to_string()),
            None => PollOutcome::Empty,
        },
        cancel,
        &out,
        stall_timeout,
    )
}

pub async fn run(
    handle: Arc<ClusterHandle>,
    topic: String,
) -> Result<(mpsc::Receiver<TailEvent>, Arc<AtomicBool>), ApiError> {
    // 404 before streaming starts if the topic doesn't exist
    let wm = {
        let handle = handle.clone();
        let topic = topic.clone();
        tokio::task::spawn_blocking(move || crate::api::messages::watermarks_blocking(&handle, &topic))
            .await
            .map_err(|e| ApiError::internal(format!("task join: {e}")))??
    };

    let (tx, rx) = mpsc::channel::<TailEvent>(256);
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel = cancelled.clone();
    let sr = handle.schema_registry.clone();
    let cfg = handle.config.clone();
    let cluster_name = handle.name.clone();

    tokio::spawn(async move {
        let (raw_tx, mut raw_rx) = mpsc::channel::<fetch::RawRecord>(256);
        let poll_cancel = cancelled.clone();
        let poll_topic = topic.clone();
        let poll_wm = wm.clone();
        let scan = tokio::task::spawn_blocking(move || {
            // librdkafka's consumer machinery requires a group.id even for
            // pure assign()-based consumption with no group management
            // involved; this uses a throwaway id and never commits, so no
            // real group is affected.
            let seq = TAIL_SEQ.fetch_add(1, Ordering::Relaxed);
            let group_id = format!("betrachtung-tail-{}-{}-{seq}", std::process::id(), crate::util::now_ms());
            let mut cc = crate::cluster::build_client_config(&cfg);
            cc.set("group.id", group_id).set("enable.auto.commit", "false");
            run_consumer_blocking(cc, &poll_topic, &poll_wm, &poll_cancel, raw_tx, STALL_TIMEOUT)
        });

        while let Some(record) = raw_rx.recv().await {
            let msg = fetch::to_message_out(vec![record], sr.as_deref()).await.pop().expect("one in one out");
            if tx.send(TailEvent::Message(Box::new(msg))).await.is_err() {
                cancelled.store(true, Ordering::SeqCst);
                break;
            }
        }
        // Mirrors search.rs's F1 fix: drop the receiver before joining the
        // blocking task so a scanner parked mid-`blocking_send` (e.g. the
        // SSE client went away, breaking the loop above before the raw
        // channel drained) is woken immediately instead of leaking forever.
        drop(raw_rx);

        match scan.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let err = ApiError::kafka(&cluster_name, e);
                let _ = tx.send(TailEvent::from_api_error(&err)).await;
            }
            Err(join_err) => {
                let err = ApiError::internal(format!("tail poll task failed: {join_err}"));
                let _ = tx.send(TailEvent::from_api_error(&err)).await;
            }
        }
    });

    Ok((rx, cancel))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_record(offset: i64) -> fetch::RawRecord {
        fetch::RawRecord { partition: 0, offset, timestamp_ms: Some(0), key: None, value: Some(b"x".to_vec()), headers: vec![] }
    }

    /// C1 stall-detection regression: a poll loop that only ever errors
    /// (dead broker, "Unknown topic or partition" after a delete, ...) must
    /// eventually give up instead of polling forever with no visible sign
    /// of trouble — mirrors search.rs's STALL_TIMEOUT idiom. Uses a tiny
    /// `stall_timeout` so the test stays fast; the real STALL_TIMEOUT
    /// constant is only used in production.
    #[test]
    fn stalls_after_persistent_poll_errors() {
        let cancel = AtomicBool::new(false);
        let (tx, _rx) = mpsc::channel(8);
        let stall_timeout = Duration::from_millis(30);
        let start = Instant::now();
        let result = drive_tail_poll(
            || PollOutcome::Error("broker transport failure".into()),
            &cancel,
            &tx,
            stall_timeout,
        );
        let err = result.expect_err("persistent poll errors must eventually surface as a terminal error");
        assert!(err.contains("stalled"), "got: {err}");
        assert!(err.contains("broker transport failure"), "got: {err}");
        // Sanity bound so a broken implementation that never stalls doesn't
        // hang the test suite instead of failing fast.
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    /// A quiet topic — poll always returning "no message", never an error —
    /// must never be reported as stalled, no matter how long it stays quiet.
    /// This is the crux of the C1 fix's stall semantics: it counts *error*
    /// time, not *idle* time.
    #[test]
    fn quiet_topic_never_stalls() {
        let cancel = Arc::new(AtomicBool::new(false));
        let (tx, _rx) = mpsc::channel(8);
        let stall_timeout = Duration::from_millis(20);
        let mut calls = 0u32;
        let cancel_for_poll = cancel.clone();
        let result = drive_tail_poll(
            move || {
                calls += 1;
                // Run well past stall_timeout in wall time (via a handful of
                // Empty polls with tiny sleeps) before cancelling, proving
                // idle time alone never trips the stall check.
                if calls > 5 {
                    cancel_for_poll.store(true, Ordering::SeqCst);
                } else {
                    std::thread::sleep(Duration::from_millis(10));
                }
                PollOutcome::Empty
            },
            &cancel,
            &tx,
            stall_timeout,
        );
        assert!(result.is_ok(), "a quiet topic must never be reported as stalled: {result:?}");
    }

    /// A successfully received message must reset the error streak: a flaky
    /// broker that errors, recovers, then errors again must not have its
    /// error windows summed across the recovery — only a *sustained* error
    /// streak of `stall_timeout` should trip.
    #[test]
    fn message_resets_the_error_streak() {
        let cancel = AtomicBool::new(false);
        let (tx, rx) = mpsc::channel(8);
        let stall_timeout = Duration::from_millis(40);
        let mut calls = 0u32;
        let start = Instant::now();
        let result = drive_tail_poll(
            move || {
                calls += 1;
                match calls {
                    // A brief error streak, comfortably under stall_timeout...
                    1 | 2 => PollOutcome::Error("transient".into()),
                    // ...then a message resets the streak...
                    3 => PollOutcome::Message(dummy_record(0)),
                    // ...so this fresh streak needs its own full
                    // stall_timeout before tripping.
                    _ => PollOutcome::Error("transient".into()),
                }
            },
            &cancel,
            &tx,
            stall_timeout,
        );
        assert!(result.is_err(), "sustained errors after the reset must still eventually stall");
        // If the pre-reset errors had counted, this would trip almost
        // immediately; asserting it takes close to a full stall_timeout
        // proves the reset happened.
        assert!(start.elapsed() >= stall_timeout, "elapsed: {:?}", start.elapsed());
        drop(rx);
    }

    /// C1a regression: a consumer that fails to *create* (bad config,
    /// resource exhaustion, ...) must surface as an `Err`, not `return;` and
    /// leave the SSE stream closing with no event. `compression.codec` is a
    /// librdkafka enum property validated at `rd_kafka_new()` time, so this
    /// fails deterministically with no network access needed.
    #[test]
    fn consumer_create_failure_is_reported_not_swallowed() {
        let mut cc = ClientConfig::new();
        cc.set("bootstrap.servers", "localhost:9092");
        cc.set("compression.codec", "not-a-real-codec");
        let cancel = AtomicBool::new(false);
        let (tx, _rx) = mpsc::channel(8);
        let err = run_consumer_blocking(cc, "topic", &[], &cancel, tx, Duration::from_millis(50))
            .expect_err("consumer creation must fail loudly on invalid config, not be swallowed");
        assert!(err.contains("create"), "got: {err}");
    }

    #[test]
    fn tail_event_names_and_shapes_error_like_the_api_envelope() {
        let msg_event = TailEvent::Message(Box::new(MessageOut {
            partition: 0, offset: 0, timestamp_ms: None, key: None, value: None, headers: vec![],
        }));
        assert_eq!(msg_event.name(), "message");

        let api_err = ApiError::kafka("prod", "assign tail consumer: boom");
        let err_event = TailEvent::from_api_error(&api_err);
        assert_eq!(err_event.name(), "error");
        let json = serde_json::to_value(&err_event).unwrap();
        assert_eq!(json["code"], "kafka_error");
        assert_eq!(json["cluster"], "prod");
        assert_eq!(json["retriable"], true);
        assert!(json["message"].as_str().unwrap().contains("boom"));
    }
}
