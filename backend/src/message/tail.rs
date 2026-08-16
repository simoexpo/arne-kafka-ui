use super::{fetch, MessageOut};
use crate::cluster::{throwaway_group_id, ClusterHandle};
use crate::error::ApiError;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::{ClientConfig, Offset};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// If the poll loop sees nothing but errors for this long (dead broker,
/// deleted topic, ...) it gives up instead of spinning forever. A *quiet*
/// topic (no errors, no messages) never counts toward this: an idle tail is
/// normal and must not be mistaken for a stalled one.
const STALL_TIMEOUT: Duration = Duration::from_secs(10);

/// A tail's SSE stream carries an explicit event enum rather than only ever
/// emitting `MessageOut`s, so a consumer create/assign failure or a stalled
/// poll loop (see `drive_tail_poll`) reaches the client as a real event
/// instead of the channel just closing with nothing to show.
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
            // Deliberately NOT "error" — see `TimelineEvent::name`'s own
            // comment (`message/timeline/event.rs`): that literal name
            // collides with `EventSource`'s reserved connection-error event
            // in a real browser, so both this and the timeline event use
            // "app_error" instead.
            TailEvent::Error { .. } => "app_error",
        }
    }
}

/// Shapes a terminal error event to match the `ApiError` JSON envelope used
/// everywhere else in the API — mirrors `impl From<ApiError> for
/// TimelineEvent` (`message/timeline/event.rs`).
impl From<&ApiError> for TailEvent {
    fn from(err: &ApiError) -> Self {
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

/// Drives the tail poll loop until `cancel` is set or a stall is declared.
///
/// A stall is 10s (`stall_timeout`) elapsed since the last successfully
/// received message *with only poll errors in between* — a quiet topic
/// (poll returning "no message" over and over) never counts, since an idle
/// tail is the normal, expected state and must not be reported as an error.
/// Extracted as a plain closure-driven loop so the stall-detection tests
/// below can drive it directly, without any real Kafka consumer.
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
            // A quiet poll is not a poll error — it resets the streak
            // exactly like a successfully received message does (this
            // function's own doc comment: "with only poll errors in
            // between"), so two isolated, transient error blips separated by
            // an arbitrarily long idle period never sum toward one stall.
            PollOutcome::Empty => {
                error_streak_since = None;
            }
        }
    }
    Ok(())
}

/// The poll task's `JoinError` reported as a tail event. Never interpolates
/// `join_err` directly (its `Display` includes any panic payload verbatim —
/// see `ApiError::task_join`'s own doc comment for the same reasoning): the
/// full diagnostic goes to the log, the client gets a fixed sentence.
fn tail_join_error(join_err: tokio::task::JoinError) -> ApiError {
    tracing::error!(error = %join_err, "tail poll task failed to join");
    ApiError::internal("the live stream stopped unexpectedly".to_string())
}

/// Builds the tail consumer, assigns it to the end of every partition in
/// `watermarks`, and drives the poll loop. Both `.create()` and `.assign()`
/// failures are reported as an `Err`, never swallowed into a silent early
/// return.
fn run_consumer_blocking(
    cc: ClientConfig,
    topic: &str,
    watermarks: &[(i32, i64, i64)],
    cancel: &AtomicBool,
    out: mpsc::Sender<fetch::RawRecord>,
    stall_timeout: Duration,
) -> Result<(), String> {
    let consumer: BaseConsumer = cc.create().map_err(|e| format!("start reading live messages: {e}"))?;
    let mut tpl = TopicPartitionList::new();
    for &(p, _, _) in watermarks {
        tpl.add_partition_offset(topic, p, Offset::End).map_err(|e| format!("prepare partitions to read: {e}"))?;
    }
    consumer.assign(&tpl).map_err(|e| format!("begin reading live messages: {e}"))?;

    drive_tail_poll(
        || match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(msg)) => PollOutcome::Message(fetch::RawRecord::from_borrowed(&msg)),
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
        tokio::task::spawn_blocking(move || fetch::watermarks_blocking(&handle, &topic))
            .await
            .map_err(ApiError::task_join)??
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
            let mut cc = crate::cluster::build_client_config(&cfg);
            cc.set("group.id", throwaway_group_id("tail")).set("enable.auto.commit", "false");
            run_consumer_blocking(cc, &poll_topic, &poll_wm, &poll_cancel, raw_tx, STALL_TIMEOUT)
        });

        while let Some(record) = raw_rx.recv().await {
            let msg = fetch::to_one_message_out(record, sr.as_deref()).await;
            if tx.send(TailEvent::Message(Box::new(msg))).await.is_err() {
                cancelled.store(true, Ordering::SeqCst);
                break;
            }
        }
        // Drop the receiver before joining the blocking task so a scanner
        // parked mid-`blocking_send` (e.g. the SSE client went away,
        // breaking the loop above before the raw channel drained) is woken
        // immediately instead of leaking forever.
        drop(raw_rx);

        match scan.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let err = ApiError::kafka(&cluster_name, e);
                let _ = tx.send(TailEvent::from(&err)).await;
            }
            Err(join_err) => {
                let err = tail_join_error(join_err);
                let _ = tx.send(TailEvent::from(&err)).await;
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

    /// A poll loop that only ever errors (dead broker, "Unknown topic or
    /// partition" after a delete, ...) must eventually give up instead of
    /// polling forever with no visible sign of trouble. Uses a tiny
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
    /// This is the crux of the stall-detection semantics: it counts *error*
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

    /// B6 regression: a quiet poll (`PollOutcome::Empty`) must reset the
    /// error streak exactly like a successfully received message does — an
    /// error, then quiet, then error sequence must not sum the two error
    /// windows across the quiet gap. Before the fix, `Empty` did nothing, so
    /// two isolated, transient error blips minutes apart on an otherwise
    /// healthy (quiet) topic could still trip the stall check — directly
    /// contradicting `drive_tail_poll`'s own doc comment ("a stall is ...
    /// elapsed since the last successfully received message *with only poll
    /// errors in between*" — an `Empty` poll is not a poll error).
    #[test]
    fn quiet_poll_resets_the_error_streak() {
        let cancel = AtomicBool::new(false);
        let (tx, rx) = mpsc::channel(8);
        let stall_timeout = Duration::from_millis(40);
        let mut calls = 0u32;
        let start = Instant::now();
        let result = drive_tail_poll(
            move || {
                calls += 1;
                match calls {
                    // A transient error starts the streak...
                    1 => PollOutcome::Error("transient".into()),
                    // ...then a REAL quiet gap longer than stall_timeout
                    // elapses before the next poll returns Empty (not an
                    // error) — a broker that recovered and went idle, the
                    // ordinary state for a tail. If this Empty poll does
                    // NOT reset the streak, the very next error below would
                    // immediately trip the stall check, since the streak
                    // already exceeds stall_timeout by the time we get there.
                    2 => {
                        std::thread::sleep(stall_timeout * 2);
                        PollOutcome::Empty
                    }
                    _ => PollOutcome::Error("transient".into()),
                }
            },
            &cancel,
            &tx,
            stall_timeout,
        );
        assert!(result.is_err(), "sustained errors after the quiet gap must still eventually stall");
        // Must take (at least) the quiet sleep PLUS a fresh stall_timeout —
        // proving the quiet poll reset the streak instead of letting it
        // carry through from before the quiet gap.
        assert!(
            start.elapsed() >= stall_timeout * 3,
            "elapsed: {:?} (expected roughly the quiet sleep {:?} plus a fresh stall_timeout {:?})",
            start.elapsed(), stall_timeout * 2, stall_timeout
        );
        drop(rx);
    }

    /// A consumer that fails to *create* (bad config,
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
        assert!(err.contains("start reading live messages"), "got: {err}");
        assert!(!err.to_lowercase().contains("consumer"), "must not name the internal Kafka client type: {err}");
    }

    #[test]
    fn tail_event_names_and_shapes_error_like_the_api_envelope() {
        let msg_event = TailEvent::Message(Box::new(MessageOut {
            partition: 0, offset: 0, timestamp_ms: None, key: None, value: None, headers: vec![],
        }));
        assert_eq!(msg_event.name(), "message");

        let api_err = ApiError::kafka("prod", "begin reading live messages: boom");
        let err_event = TailEvent::from(&api_err);
        assert_eq!(err_event.name(), "app_error");
        let json = serde_json::to_value(&err_event).unwrap();
        assert_eq!(json["code"], "kafka_error");
        assert_eq!(json["cluster"], "prod");
        assert_eq!(json["retriable"], true);
        assert!(json["message"].as_str().unwrap().contains("boom"));
    }

    /// The poll task's `JoinError` (including any panic payload) must never
    /// reach the wire — only a fixed, generic sentence; the diagnostic goes
    /// to the log instead.
    #[tokio::test]
    async fn tail_join_error_never_leaks_the_panic_payload() {
        let handle = tokio::spawn(async { panic!("some very specific internal detail nobody should see") });
        let join_err = handle.await.unwrap_err();
        let err = tail_join_error(join_err);
        assert_eq!(err.message, "the live stream stopped unexpectedly");
        assert!(!err.message.contains("specific internal detail"));
        assert!(!err.message.to_lowercase().contains("panicked"));
    }
}
