use serde::Serialize;

use crate::error::ApiError;
use crate::message::MessageOut;

/// One event of a `run_page` SSE stream. Serialized untagged: the SSE
/// `event:` field carries the discriminant (`.name()`), so the JSON `data:`
/// payload is just the variant's own fields.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum TimelineEvent {
    Match(Box<MessageOut>),
    Progress { scanned: u64, matches: u64, budget: u64 },
    PageEnd { cursor: Option<String>, exhausted: bool },
    Error { code: String, message: String, cluster: Option<String>, retriable: bool },
}

impl TimelineEvent {
    pub fn name(&self) -> &'static str {
        match self {
            TimelineEvent::Match(_) => "match",
            TimelineEvent::Progress { .. } => "progress",
            TimelineEvent::PageEnd { .. } => "page_end",
            // Deliberately NOT "error" (review finding, 2026-08-15): SSE's
            // `EventSource` treats a server-sent frame whose `event:` field
            // is literally "error" as colliding with its OWN reserved
            // connection-level error type — both `addEventListener('error',
            // ...)` AND the `onerror` IDL handler fire for it, in a REAL
            // browser (not the test double here, which is exactly why this
            // shipped unnoticed: this app's own error events had this
            // collision from day one). The result: the frontend's
            // transport-error handler wins the race and shows generic
            // "connection lost" wording, discarding the real, structured
            // error this event carries — the very same failure mode M1+M2
            // was fixing by moving an anchor error in-stream in the first
            // place, just one layer deeper. "app_error" carries the exact
            // same envelope, named to never collide with a reserved
            // EventSource type.
            TimelineEvent::Error { .. } => "app_error",
        }
    }
}

impl From<ApiError> for TimelineEvent {
    fn from(e: ApiError) -> Self {
        TimelineEvent::Error { code: e.code.to_string(), message: e.message, cluster: e.cluster, retriable: e.retriable }
    }
}
